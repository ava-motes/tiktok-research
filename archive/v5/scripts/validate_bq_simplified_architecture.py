#!/usr/bin/env python3
"""Validate simplified BigQuery architecture (v3).

Reports totals, duplicates, null PKs, sample Whisper/OCR/emoji, and pipeline logs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.enrichment.bigquery_loader import (
    ENRICHED_TABLE,
    PIPELINE_LOGS_TABLE,
    ensure_dataset_and_tables,
    enriched_table_id,
    gcp_project,
    inspect_schema,
    pipeline_logs_table_id,
)
from tiktok.logging_setup import setup_logging


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/bq_architecture_validation.json")
    parser.add_argument("--sample", type=int, default=5)
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    from google.cloud import bigquery

    ensure_dataset_and_tables()
    client = bigquery.Client(project=gcp_project())
    table = enriched_table_id()
    logs = pipeline_logs_table_id()

    def q(sql):
        return list(client.query(sql).result())

    total = int(q(f"SELECT COUNT(*) n FROM `{table}`")[0].n)
    dups = int(
        q(
            f"SELECT COUNT(*) n FROM (SELECT video_id FROM `{table}` GROUP BY video_id HAVING COUNT(*)>1)"
        )[0].n
    )
    null_pk = int(q(f"SELECT COUNT(*) n FROM `{table}` WHERE video_id IS NULL OR video_id=''")[0].n)
    with_whisper = int(
        q(f"SELECT COUNT(*) n FROM `{table}` WHERE LENGTH(IFNULL(whisper_transcript,''))>0")[0].n
    )
    with_ocr = int(q(f"SELECT COUNT(*) n FROM `{table}` WHERE IFNULL(frames_with_text,0)>0")[0].n)
    with_emoji = int(q(f"SELECT COUNT(*) n FROM `{table}` WHERE IFNULL(emoji_count,0)>0")[0].n)

    try:
        log_n = int(q(f"SELECT COUNT(*) n FROM `{logs}`")[0].n)
        log_stages = [dict(r) for r in q(
            f"SELECT stage, status, COUNT(*) n FROM `{logs}` GROUP BY stage, status ORDER BY n DESC LIMIT 20"
        )]
    except Exception as e:
        log_n = -1
        log_stages = [{"error": str(e)}]

    samples = {
        "enriched": [dict(r) for r in q(
            f"""SELECT video_id, creator_username, caption, enrichment_status,
                       enrichment_quality_score, like_count, emoji_count
                FROM `{table}` ORDER BY enrichment_date DESC LIMIT {args.sample}"""
        )],
        "whisper": [dict(r) for r in q(
            f"""SELECT video_id, SUBSTR(whisper_transcript,1,160) AS whisper_transcript,
                       transcript_language, transcript_duration_seconds
                FROM `{table}`
                WHERE LENGTH(IFNULL(whisper_transcript,''))>0
                ORDER BY enrichment_date DESC LIMIT {args.sample}"""
        )],
        "ocr": [dict(r) for r in q(
            f"""SELECT video_id, SUBSTR(cleaned_ocr_text,1,160) AS cleaned_ocr_text,
                       frames_processed, frames_with_text, ocr_confidence, ocr_sources
                FROM `{table}`
                WHERE IFNULL(frames_with_text,0)>0
                ORDER BY enrichment_date DESC LIMIT {args.sample}"""
        )],
        "emoji": [dict(r) for r in q(
            f"""SELECT video_id, emoji_characters, emoji_descriptions, emoji_codepoints,
                       emoji_categories, emoji_count
                FROM `{table}`
                WHERE IFNULL(emoji_count,0)>0
                ORDER BY enrichment_date DESC LIMIT {args.sample}"""
        )],
        "pipeline_logs": [dict(r) for r in q(
            f"""SELECT log_id, video_id, stage, status, duration_seconds, error_message, created_at
                FROM `{logs}` ORDER BY created_at DESC LIMIT {args.sample}"""
        )] if log_n >= 0 else [],
    }

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "architecture": {
            "analytics_table": table,
            "ops_table": logs,
            "allowed_tables": [ENRICHED_TABLE, PIPELINE_LOGS_TABLE],
        },
        "schema": inspect_schema(),
        "metrics": {
            "total_rows": total,
            "duplicate_video_ids": dups,
            "null_primary_keys": null_pk,
            "with_whisper_transcript": with_whisper,
            "with_ocr_text": with_ocr,
            "with_emoji": with_emoji,
            "pipeline_logs_rows": log_n,
        },
        "pass": dups == 0 and null_pk == 0 and total > 0,
        "samples": samples,
        "pipeline_log_stages": log_stages,
        "drop_legacy_sql": "sql/drop_legacy_bq_tables.sql",
    }

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["pass"] else 2


if __name__ == "__main__":
    sys.exit(main())
