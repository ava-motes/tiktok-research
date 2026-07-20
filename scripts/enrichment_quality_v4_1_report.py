#!/usr/bin/env python3
"""Production readiness report comparing enrichment-v4.0 vs v4.1 metrics.

Usage (on comm-cme-p01):
    python scripts/enrichment_quality_v4_1_report.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.enrichment.bigquery_loader import (
    PIPELINE_VERSION,
    bigquery_configured,
    enrichment_quality_score,
    enriched_table_id,
)
from tiktok.logging_setup import setup_logging

BEFORE = {
    "whisper_pct": 97.7,
    "ocr_pct": 94.4,
    "emoji_pct": 11.6,
    "quality_score": 88.8,
    "total_videos": 517,
    "ok_pct": 94.2,
    "partial_pct": 5.8,
}

TARGETS = {
    "whisper_pct": 98.0,
    "ocr_pct": 97.0,
    "partial_pct_max": 2.0,
    "quality_score": 92.0,
}


def _metrics_from_bq() -> Dict[str, Any]:
    from google.cloud import bigquery

    client = bigquery.Client()
    table = enriched_table_id()
    q = f"""
    SELECT
      COUNT(*) AS total,
      COUNTIF(IFNULL(whisper_transcript,'') != '') AS whisper,
      COUNTIF(
        IFNULL(cleaned_ocr_text, ocr_text) IS NOT NULL
        AND IFNULL(cleaned_ocr_text, ocr_text) != ''
      ) AS ocr,
      COUNTIF(IFNULL(emoji_characters,'') != '') AS emoji,
      COUNTIF(LOWER(IFNULL(enrichment_status,'')) = 'ok') AS ok_n,
      COUNTIF(LOWER(IFNULL(enrichment_status,'')) = 'partial') AS partial_n,
      ROUND(AVG(
        IFNULL(
          enrichment_quality_score,
          CASE
            WHEN IFNULL(video_id,'')='' OR IFNULL(creator_username,'')='' THEN 0
            WHEN IFNULL(cleaned_ocr_text, ocr_text) != ''
                 AND IFNULL(whisper_transcript,'') != ''
                 AND IFNULL(emoji_characters,'') != '' THEN 100
            WHEN IFNULL(cleaned_ocr_text, ocr_text) != ''
                 AND IFNULL(whisper_transcript,'') != '' THEN 90
            WHEN IFNULL(cleaned_ocr_text, ocr_text) != '' THEN 80
            WHEN IFNULL(whisper_transcript,'') != '' THEN 60
            ELSE 40
          END
        )
      ), 1) AS avg_quality,
      ROUND(AVG(IFNULL(ocr_quality_score, NULL)), 1) AS avg_ocr_quality,
      ROUND(AVG(IFNULL(ocr_unique_text_ratio, NULL)), 3) AS avg_ocr_unique_ratio,
      COUNTIF(IFNULL(pipeline_version,'') = 'enrichment-v4.1') AS v41_rows
    FROM `{table}`
    """
    row = dict(list(client.query(q).result())[0])
    n = int(row["total"] or 0) or 1
    return {
        "total_videos": int(row["total"] or 0),
        "whisper": int(row["whisper"] or 0),
        "ocr": int(row["ocr"] or 0),
        "emoji": int(row["emoji"] or 0),
        "ok": int(row["ok_n"] or 0),
        "partial": int(row["partial_n"] or 0),
        "whisper_pct": round(100 * int(row["whisper"] or 0) / n, 1),
        "ocr_pct": round(100 * int(row["ocr"] or 0) / n, 1),
        "emoji_pct": round(100 * int(row["emoji"] or 0) / n, 1),
        "ok_pct": round(100 * int(row["ok_n"] or 0) / n, 1),
        "partial_pct": round(100 * int(row["partial_n"] or 0) / n, 1),
        "quality_score": float(row["avg_quality"] or 0),
        "avg_ocr_quality": row.get("avg_ocr_quality"),
        "avg_ocr_unique_ratio": row.get("avg_ocr_unique_ratio"),
        "v41_rows": int(row["v41_rows"] or 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/enrichment_quality_v4_1_report.json")
    parser.add_argument(
        "--six-report",
        default="data/final_six_video_validation_v4_1.json",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    if not bigquery_configured():
        print("BigQuery not configured", file=sys.stderr)
        return 1

    after = _metrics_from_bq()
    targets_met = {
        "whisper_gte_98": after["whisper_pct"] >= TARGETS["whisper_pct"],
        "ocr_gte_97": after["ocr_pct"] >= TARGETS["ocr_pct"],
        "partial_lte_2": after["partial_pct"] <= TARGETS["partial_pct_max"],
        "quality_gte_92": after["quality_score"] >= TARGETS["quality_score"],
    }
    ready = all(targets_met.values())

    six = None
    if os.path.isfile(args.six_report):
        with open(args.six_report, encoding="utf-8") as f:
            six = json.load(f)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_version": PIPELINE_VERSION,
        "table": enriched_table_id(),
        "before_v4_0": BEFORE,
        "after": after,
        "targets": TARGETS,
        "targets_met": targets_met,
        "recommendation": (
            "ready for daily collection"
            if ready
            else "additional fixes required"
        ),
        "delta": {
            "whisper_pct": round(after["whisper_pct"] - BEFORE["whisper_pct"], 1),
            "ocr_pct": round(after["ocr_pct"] - BEFORE["ocr_pct"], 1),
            "emoji_pct": round(after["emoji_pct"] - BEFORE["emoji_pct"], 1),
            "quality_score": round(after["quality_score"] - BEFORE["quality_score"], 1),
            "partial_pct": round(after["partial_pct"] - BEFORE["partial_pct"], 1),
        },
        "six_video_validation": {
            "report": args.six_report if six else None,
            "cases": len((six or {}).get("cases") or []) if six else 0,
        },
        "known_limitations": [
            "Emoji rate remains content-dependent (many news videos have no emoji).",
            "OCR still depends on Vision frame sampling; some green-screen/tweet text may need more frames.",
            "format_not_supported Whisper failures require ffmpeg WAV path; rare download failures remain.",
            "Web hydration gaps (e.g. hydration_item_struct_missing) are separate from Vision OCR.",
        ],
    }

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(
        {
            "recommendation": report["recommendation"],
            "targets_met": targets_met,
            "after": after,
            "delta": report["delta"],
        },
        indent=2,
        default=str,
    ))
    print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
