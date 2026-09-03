#!/usr/bin/env python3
"""Export a researcher-friendly dataset from tiktok_video_enriched.

Exports a research subset of tiktok_video_enriched (see docs/SCHEMA.md, section 6).
Intentionally excludes ops/debug fields (latency, retries, internal pipeline
columns). It also omits `comments_json`, which is empty until comment collection
is enabled; add it to RESEARCH_COLUMNS below once comments are collected.

The canonical schema (including which columns are research vs operational) is
tiktok/enrichment/bigquery_loader.py (RESEARCH_COLUMNS / OPERATIONAL_COLUMNS).

Usage (on comm-cme-p01 preferred):
    python scripts/export_research_dataset.py
    python scripts/export_research_dataset.py --out-prefix data/exports/tiktok_research_enriched

Outputs:
    <prefix>.csv
    <prefix>.parquet  (requires pyarrow or fastparquet)
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.enrichment.bigquery_loader import bigquery_configured, enriched_table_id
from tiktok.logging_setup import setup_logging

# Research-facing columns only
RESEARCH_COLUMNS = [
    # Identifiers / links
    "video_id",
    "video_url",
    # Creator
    "creator_username",
    "creator_display_name",
    "creator_bio",
    "creator_verified",
    "creator_followers",
    "creator_following",
    "creator_total_likes",
    "creator_video_count",
    # Metadata / engagement
    "posted_at",
    "caption",
    "hashtags",
    "like_count",
    "comment_count",
    "share_count",
    "favorite_count",
    "video_duration_seconds",
    # Text layers
    "voice_to_text",
    "sticker_text",
    "whisper_transcript",
    "ocr_text",
    "emoji_characters",
    "emoji_descriptions",
    "emoji_category",
    # Light research status (not ops latency)
    "enrichment_status",
    "enrichment_quality_score",
    "enrichment_date",
    "pipeline_version",
]


def _fetch_rows() -> List[Dict[str, Any]]:
    from google.cloud import bigquery

    client = bigquery.Client()
    cols = ", ".join(RESEARCH_COLUMNS)
    # Prefer cleaned OCR already exposed as ocr_text; dedupe by video_id
    q = f"""
    SELECT * EXCEPT(rn) FROM (
      SELECT
        {cols},
        ROW_NUMBER() OVER (
          PARTITION BY video_id
          ORDER BY enrichment_date DESC, enrichment_quality_score DESC
        ) AS rn
      FROM `{enriched_table_id()}`
    )
    WHERE rn = 1
    ORDER BY posted_at DESC, video_id
    """
    # Some columns may be missing on older tables — fall back to SELECT *
    try:
        return [dict(r) for r in client.query(q).result()]
    except Exception:
        q2 = f"SELECT * FROM `{enriched_table_id()}`"
        raw = [dict(r) for r in client.query(q2).result()]
        out = []
        seen = set()
        for r in raw:
            vid = r.get("video_id")
            if vid in seen:
                continue
            seen.add(vid)
            out.append({c: r.get(c) for c in RESEARCH_COLUMNS})
        return out


def _write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RESEARCH_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") if r.get(c) is not None else "" for c in RESEARCH_COLUMNS})


def _write_parquet(path: str, rows: List[Dict[str, Any]]) -> str:
    try:
        import pandas as pd
    except ImportError as e:
        raise RuntimeError("pandas required for parquet export") from e
    df = pd.DataFrame(rows, columns=RESEARCH_COLUMNS)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try:
        df.to_parquet(path, index=False)
        return path
    except Exception as e:
        # Retry with pyarrow engine hint
        try:
            df.to_parquet(path, index=False, engine="pyarrow")
            return path
        except Exception:
            raise RuntimeError(
                f"Parquet write failed ({e}). Install pyarrow: pip install pyarrow"
            ) from e


def main() -> int:
    parser = argparse.ArgumentParser(description="Export research CSV/Parquet from BQ")
    parser.add_argument(
        "--out-prefix",
        default="data/exports/tiktok_research_enriched",
        help="Output path prefix (without extension)",
    )
    parser.add_argument("--csv-only", action="store_true")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    if not bigquery_configured():
        print("BigQuery not configured", file=sys.stderr)
        return 1

    rows = _fetch_rows()
    csv_path = f"{args.out_prefix}.csv"
    _write_csv(csv_path, rows)
    print(f"Wrote {csv_path} ({len(rows)} rows)")

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_table": enriched_table_id(),
        "rows": len(rows),
        "columns": RESEARCH_COLUMNS,
        "csv": csv_path,
        "parquet": None,
    }

    if not args.csv_only:
        pq_path = f"{args.out_prefix}.parquet"
        try:
            _write_parquet(pq_path, rows)
            meta["parquet"] = pq_path
            print(f"Wrote {pq_path}")
        except Exception as e:
            print(f"Parquet skipped: {e}", file=sys.stderr)
            meta["parquet_error"] = str(e)

    meta_path = f"{args.out_prefix}.manifest.json"
    import json

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print(f"Wrote {meta_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
