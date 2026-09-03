#!/usr/bin/env python3
"""Validate emoji → CLDR name/category extraction, plus a live caption sample.

Run on comm-cme-p01:
    python scripts/validate_emoji_extract.py
    python scripts/validate_emoji_extract.py --live-limit 20 --sync-bigquery
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.logging_setup import setup_logging
from tiktok.enrichment.emoji_extract import annotate_emoji, extract_emoji_rows_from_text
from tiktok.enrichment.store import ensure_enrichment_schema, replace_emoji_rows

logger = logging.getLogger(__name__)

UNIT_CASES = [
    {
        "input": "🔥 Breaking news 🔥",
        "expect_emoji": "🔥",
        "expect_name_contains": "fire",
        "expect_category_in": ["emphasis", "emotion"],
    },
    {
        "input": "😂 lol ❤️",
        "expect_any": [
            {"emoji": "😂", "name_contains": "tears"},
            {"emoji": "❤️", "name_contains": "heart"},
        ],
    },
]


def run_unit_tests() -> Dict[str, Any]:
    results = []
    for case in UNIT_CASES:
        rows = extract_emoji_rows_from_text(case["input"], "unit_test")
        entry: Dict[str, Any] = {"input": case["input"], "rows": rows, "ok": False}
        if "expect_emoji" in case:
            match = next((r for r in rows if r["emoji"] == case["expect_emoji"]), None)
            name_ok = bool(
                match
                and case["expect_name_contains"] in (match.get("emoji_name") or "").lower()
            )
            cat_ok = bool(
                match and (match.get("emoji_category") in case["expect_category_in"])
            )
            entry["ok"] = bool(match and name_ok and cat_ok)
            entry["matched"] = match
        else:
            ok_any = True
            for exp in case.get("expect_any") or []:
                m = next((r for r in rows if r["emoji"] == exp["emoji"]), None)
                if not m or exp["name_contains"] not in (m.get("emoji_name") or "").lower():
                    ok_any = False
            entry["ok"] = ok_any
        # Direct annotate check for 🔥
        if "🔥" in case["input"]:
            entry["annotate_fire"] = list(annotate_emoji("🔥"))
        results.append(entry)
    return {
        "passed": all(r["ok"] for r in results),
        "cases": results,
    }


def find_caption_emoji_videos(conn, limit: int) -> List[Dict[str, Any]]:
    """Find videos whose captions contain emoji characters."""
    # Broad unicode pictograph filter in Python after SQL pull of recent captions
    rows = conn.execute(
        """SELECT video_id, username, caption, video_url, hashtags
           FROM videos
           WHERE caption IS NOT NULL AND caption != ''
           ORDER BY create_time DESC
           LIMIT 5000"""
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        extracted = extract_emoji_rows_from_text(d.get("caption") or "", "caption")
        if not extracted:
            continue
        out.append({**d, "emoji_rows": extracted})
        if len(out) >= limit:
            break
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate emoji CLDR extraction")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--live-limit", type=int, default=15)
    parser.add_argument("--sync-bigquery", action="store_true")
    parser.add_argument(
        "--report",
        default="data/emoji_validation_report.json",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    unit = run_unit_tests()
    logger.info("Unit emoji tests passed=%s", unit["passed"])

    live = find_caption_emoji_videos(conn, args.live_limit)
    logger.info("Found %s videos with caption emojis", len(live))

    synced = []
    if args.sync_bigquery and live:
        from tiktok.enrichment.bigquery_loader import (
            bigquery_configured,
            sync_video_from_sqlite,
        )

        if not bigquery_configured():
            logger.error("BigQuery not configured")
        else:
            # Ensure emoji staging rows exist, then upsert enriched row
            # (transcript/OCR may already exist from prior runs)
            for item in live:
                vid = item["video_id"]
                replace_emoji_rows(conn, vid, item["emoji_rows"])
                conn.commit()
                try:
                    counts = sync_video_from_sqlite(conn, vid)
                    synced.append({"video_id": vid, "counts": counts, "emojis": item["emoji_rows"]})
                    logger.info(
                        "Synced emoji video %s @%s → %s",
                        vid,
                        item.get("username"),
                        [e["emoji"] for e in item["emoji_rows"][:8]],
                    )
                except Exception as e:
                    logger.error("Sync failed %s: %s", vid, e)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "unit_tests": unit,
        "live_caption_emoji_videos": [
            {
                "video_id": x["video_id"],
                "creator_handle": x.get("username"),
                "caption_preview": (x.get("caption") or "")[:160],
                "emojis": [
                    {
                        "emoji": e["emoji"],
                        "emoji_name": e["emoji_name"],
                        "emoji_category": e["emoji_category"],
                        "count": e["count"],
                    }
                    for e in x["emoji_rows"]
                ],
            }
            for x in live
        ],
        "synced": synced,
    }
    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", args.report)
    conn.close()
    return 0 if unit["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
