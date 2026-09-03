#!/usr/bin/env python3
"""Analyze emoji frequency in tiktok_video_enriched (and/or SQLite staging).

Run on comm-cme-p01:
    python scripts/analyze_emoji_frequency.py
    python scripts/analyze_emoji_frequency.py --report data/emoji_frequency_report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.logging_setup import setup_logging
from tiktok.enrichment.emoji_extract import annotate_emoji, emoji_context_words
from tiktok.enrichment.store import ensure_enrichment_schema

logger = logging.getLogger(__name__)


def _from_sqlite(conn) -> Dict[str, Any]:
    videos = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    # Prefer enriched staging: any video with emoji rows
    emoji_videos = conn.execute(
        "SELECT COUNT(DISTINCT video_id) FROM video_emojis"
    ).fetchone()[0]
    # Also count videos that have enrichment rows in BQ sense via transcripts/ocr
    enriched_candidates = conn.execute(
        """SELECT COUNT(DISTINCT video_id) FROM (
             SELECT video_id FROM video_transcripts
             UNION
             SELECT video_id FROM video_ocr
             UNION
             SELECT video_id FROM video_emojis
           )"""
    ).fetchone()[0]

    rows = conn.execute(
        """SELECT e.emoji, e.emoji_name, e.count, e.text_source, e.video_id,
                  v.caption, t.transcript,
                  (SELECT GROUP_CONCAT(ocr_text, ' ') FROM video_ocr o WHERE o.video_id=e.video_id) AS ocr
           FROM video_emojis e
           LEFT JOIN videos v ON v.video_id = e.video_id
           LEFT JOIN video_transcripts t ON t.video_id = e.video_id AND t.status='ok'
        """
    ).fetchall()

    freq: Counter = Counter()
    name_map: Dict[str, str] = {}
    source_counts: Counter = Counter()
    cooccur: Dict[str, Counter] = defaultdict(Counter)

    for r in rows:
        d = dict(r)
        g = d.get("emoji") or ""
        if not g:
            continue
        cnt = int(d.get("count") or 1)
        freq[g] += cnt
        name_map[g] = d.get("emoji_name") or annotate_emoji(g)[0]
        source_counts[d.get("text_source") or "unknown"] += cnt
        blob = " ".join(
            x for x in (d.get("caption"), d.get("transcript"), d.get("ocr")) if x
        )
        for w in emoji_context_words(blob, g):
            cooccur[g][w] += 1

    denom = max(enriched_candidates, 1)
    pct = round(100.0 * emoji_videos / denom, 2)

    top50 = []
    for g, c in freq.most_common(50):
        words = [w for w, _ in cooccur[g].most_common(8)]
        top50.append(
            {
                "emoji": g,
                "description": name_map.get(g) or annotate_emoji(g)[0],
                "count": c,
                "associated_words": words,
            }
        )

    return {
        "source": "sqlite_staging",
        "videos_total_in_db": videos,
        "videos_with_enrichment_staging": enriched_candidates,
        "videos_with_at_least_one_emoji": emoji_videos,
        "pct_enriched_videos_with_emoji": pct,
        "emoji_source_counts": dict(source_counts),
        "unique_emoji_types": len(freq),
        "top_50_emojis": top50,
    }


def _from_bigquery() -> Dict[str, Any]:
    from tiktok.enrichment.bigquery_loader import (
        bigquery_configured,
        enriched_table_id,
        ensure_dataset_and_tables,
        _client,
    )

    if not bigquery_configured():
        return {"source": "bigquery", "error": "not_configured"}
    ensure_dataset_and_tables()
    client = _client()
    table = enriched_table_id()
    q = f"""
    SELECT
      COUNT(*) AS n,
      COUNTIF(IFNULL(emoji_count, 0) > 0 OR LENGTH(IFNULL(emoji_characters, '')) > 0
              OR LENGTH(IFNULL(emojis, '')) > 0) AS with_emoji
    FROM `{table}`
    """
    summary = list(client.query(q).result())[0]
    n = int(summary.n)
    with_emoji = int(summary.with_emoji)

    # Expand emoji_characters / emoji_sources JSON when present
    q2 = f"""
    SELECT emoji_characters, emoji_descriptions, emoji_count, emoji_sources,
           cleaned_ocr_text, ocr_text, transcript, description
    FROM `{table}`
    WHERE IFNULL(emoji_count, 0) > 0
       OR LENGTH(IFNULL(emoji_characters, '')) > 0
       OR LENGTH(IFNULL(emojis, '')) > 0
    """
    freq: Counter = Counter()
    name_map: Dict[str, str] = {}
    cooccur: Dict[str, Counter] = defaultdict(Counter)
    for row in client.query(q2).result():
        d = dict(row)
        # Prefer structured emoji_sources JSON
        parsed = None
        try:
            parsed = json.loads(d.get("emoji_sources") or "[]")
        except Exception:
            parsed = None
        if parsed:
            for item in parsed:
                g = item.get("emoji") or ""
                if not g:
                    continue
                freq[g] += int(item.get("count") or 1)
                name_map[g] = item.get("description") or annotate_emoji(g)[0]
        else:
            chars = [x.strip() for x in (d.get("emoji_characters") or d.get("emojis") or "").split("|") if x.strip()]
            descs = [x.strip() for x in (d.get("emoji_descriptions") or "").split("|") if x.strip()]
            for i, g in enumerate(chars):
                freq[g] += 1
                if i < len(descs):
                    name_map[g] = descs[i]
        blob = " ".join(
            x
            for x in (
                d.get("description"),
                d.get("transcript"),
                d.get("cleaned_ocr_text") or d.get("ocr_text"),
            )
            if x
        )
        for g in list(freq.keys())[-20:]:
            pass
        chars_now = []
        if parsed:
            chars_now = [i.get("emoji") for i in parsed if i.get("emoji")]
        else:
            chars_now = [
                x.strip()
                for x in (d.get("emoji_characters") or d.get("emojis") or "").split("|")
                if x.strip()
            ]
        for g in chars_now:
            for w in emoji_context_words(blob, g):
                cooccur[g][w] += 1

    top50 = []
    for g, c in freq.most_common(50):
        top50.append(
            {
                "emoji": g,
                "description": name_map.get(g) or annotate_emoji(g)[0],
                "count": c,
                "associated_words": [w for w, _ in cooccur[g].most_common(8)],
            }
        )
    return {
        "source": "bigquery",
        "table": table,
        "videos_in_table": n,
        "videos_with_at_least_one_emoji": with_emoji,
        "pct_videos_with_emoji": round(100.0 * with_emoji / n, 2) if n else 0.0,
        "unique_emoji_types": len(freq),
        "top_50_emojis": top50,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Emoji frequency analysis")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--report", default="data/emoji_frequency_report.json")
    parser.add_argument("--skip-bigquery", action="store_true")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "host": "comm-cme-p01.moody.utexas.edu",
        "sqlite": _from_sqlite(conn),
    }
    if not args.skip_bigquery:
        try:
            report["bigquery"] = _from_bigquery()
        except Exception as e:
            report["bigquery"] = {"error": str(e)}
            logger.exception("BigQuery emoji analysis failed")

    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", args.report)
    bq = report.get("bigquery") or {}
    logger.info(
        "BQ emoji coverage: %s / %s (%.1f%%); top=%s",
        bq.get("videos_with_at_least_one_emoji"),
        bq.get("videos_in_table"),
        bq.get("pct_videos_with_emoji") or 0,
        [(x["emoji"], x["description"], x["count"]) for x in (bq.get("top_50_emojis") or [])[:10]],
    )
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
