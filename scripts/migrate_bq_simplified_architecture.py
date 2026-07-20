#!/usr/bin/env python3
"""Migrate BigQuery to the simplified two-table architecture (v3).

Steps:
  1. Ensure tiktok_video_enriched + tiktok_pipeline_logs schemas exist
  2. Backfill new column names from legacy columns on tiktok_video_enriched
  3. Optionally merge sparse data from deprecated BQ staging tables
  4. Deduplicate to one row per video_id (keep latest processing_timestamp)
  5. Verify counts / duplicates / null PKs
  6. Write migration report + print DROP SQL path

Does NOT modify TikTok collection. Does NOT drop legacy tables automatically.

Usage (comm-cme-p01):
  python scripts/migrate_bq_simplified_architecture.py
  python scripts/migrate_bq_simplified_architecture.py --apply-dedupe
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.enrichment.bigquery_loader import (
    ENRICHED_TABLE,
    LEGACY_BQ_TABLES,
    PIPELINE_LOGS_TABLE,
    PIPELINE_VERSION,
    bq_dataset,
    enriched_table_id,
    ensure_dataset_and_tables,
    gcp_project,
    inspect_schema,
    pipeline_logs_table_id,
)
from tiktok.logging_setup import setup_logging


def _q(client, sql: str):
    return list(client.query(sql).result())


def _table_exists(client, name: str) -> bool:
    tid = f"{gcp_project()}.{bq_dataset()}.{name}"
    try:
        client.get_table(tid)
        return True
    except Exception:
        return False


def backfill_column_aliases(client) -> List[str]:
    """Copy legacy enriched-column values into v3 names where new cols are empty."""
    table = enriched_table_id()
    # (new_col, old_col) — numeric vs string handled separately
    string_mappings = [
        ("creator_username", "creator_handle"),
        ("caption", "description"),
        ("create_time", "posted_at"),
        ("whisper_transcript", "transcript"),
        ("cleaned_ocr_text", "ocr_text"),
    ]
    numeric_mappings = [
        ("like_count", "likes"),
        ("comment_count", "comments"),
        ("share_count", "shares"),
        ("view_count", "views"),
        ("video_duration_seconds", "duration_seconds"),
        ("frames_processed", "number_of_frames_processed"),
        ("frames_processed", "ocr_frames_processed"),
        ("ocr_confidence", "ocr_confidence_avg"),
        ("favorite_count", "favorites_count"),  # may not exist
    ]
    schema = {f.name: f.field_type for f in client.get_table(table).schema}
    applied = []
    for new_col, old_col in string_mappings:
        if new_col not in schema or old_col not in schema:
            continue
        sql = f"""
        UPDATE `{table}`
        SET {new_col} = {old_col}
        WHERE ({new_col} IS NULL OR {new_col} = '')
          AND {old_col} IS NOT NULL AND {old_col} != ''
        """
        try:
            job = client.query(sql)
            job.result()
            applied.append(f"{old_col}->{new_col} rows_affected={job.num_dml_affected_rows}")
        except Exception as e:
            applied.append(f"{old_col}->{new_col} FAILED: {e}")
    for new_col, old_col in numeric_mappings:
        if new_col not in schema or old_col not in schema:
            continue
        sql = f"""
        UPDATE `{table}`
        SET {new_col} = {old_col}
        WHERE {new_col} IS NULL AND {old_col} IS NOT NULL
        """
        try:
            job = client.query(sql)
            job.result()
            applied.append(f"{old_col}->{new_col} rows_affected={job.num_dml_affected_rows}")
        except Exception as e:
            applied.append(f"{old_col}->{new_col} FAILED: {e}")
    return applied


def dedupe_enriched(client) -> Dict[str, Any]:
    """Keep one row per video_id (latest processing_timestamp / enrichment_date)."""
    table = enriched_table_id()
    before = int(_q(client, f"SELECT COUNT(*) AS n FROM `{table}`")[0].n)
    client.query(
        f"""
        CREATE OR REPLACE TABLE `{table}` AS
        SELECT * EXCEPT(rn) FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY video_id
              ORDER BY
                COALESCE(processing_timestamp, TIMESTAMP('1970-01-01')) DESC,
                enrichment_date DESC
            ) AS rn
          FROM `{table}`
          WHERE video_id IS NOT NULL AND video_id != ''
        )
        WHERE rn = 1
        """
    ).result()
    ensure_dataset_and_tables()  # re-add any v3 columns dropped by SELECT *
    after = int(_q(client, f"SELECT COUNT(*) AS n FROM `{table}`")[0].n)
    return {
        "rows_before": before,
        "rows_after": after,
        "removed": before - after,
    }


def verify(client) -> Dict[str, Any]:
    table = enriched_table_id()
    logs = pipeline_logs_table_id()
    total = int(_q(client, f"SELECT COUNT(*) n FROM `{table}`")[0].n)
    dups = int(
        _q(
            client,
            f"""
            SELECT COUNT(*) n FROM (
              SELECT video_id FROM `{table}` GROUP BY video_id HAVING COUNT(*) > 1
            )
            """,
        )[0].n
    )
    null_pk = int(
        _q(
            client,
            f"SELECT COUNT(*) n FROM `{table}` WHERE video_id IS NULL OR video_id = ''",
        )[0].n
    )
    distinct = int(
        _q(client, f"SELECT COUNT(DISTINCT video_id) n FROM `{table}`")[0].n
    )
    log_count = 0
    if _table_exists(client, PIPELINE_LOGS_TABLE):
        log_count = int(_q(client, f"SELECT COUNT(*) n FROM `{logs}`")[0].n)

    legacy_status = {}
    for name in LEGACY_BQ_TABLES:
        if _table_exists(client, name):
            tid = f"{gcp_project()}.{bq_dataset()}.{name}"
            n = int(_q(client, f"SELECT COUNT(*) n FROM `{tid}`")[0].n)
            legacy_status[name] = {"exists": True, "rows": n}
        else:
            legacy_status[name] = {"exists": False, "rows": 0}

    samples = [
        dict(r)
        for r in _q(
            client,
            f"""
            SELECT
              video_id, creator_username, enrichment_status, enrichment_quality_score,
              SUBSTR(IFNULL(whisper_transcript, ''), 1, 80) AS whisper_preview,
              SUBSTR(IFNULL(cleaned_ocr_text, ''), 1, 80) AS ocr_preview,
              emoji_characters, emoji_count, pipeline_version
            FROM `{table}`
            ORDER BY enrichment_date DESC
            LIMIT 5
            """,
        )
    ]

    return {
        "total_rows": total,
        "distinct_video_ids": distinct,
        "duplicate_video_ids": dups,
        "null_primary_keys": null_pk,
        "one_row_per_video": dups == 0 and null_pk == 0 and total == distinct,
        "pipeline_logs_rows": log_count,
        "legacy_tables": legacy_status,
        "sample_rows": samples,
        "pipeline_version_target": PIPELINE_VERSION,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply-dedupe",
        action="store_true",
        help="Rewrite tiktok_video_enriched to one row per video_id",
    )
    parser.add_argument("--out", default="data/bq_migration_report.json")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    from google.cloud import bigquery

    ensure_dataset_and_tables()
    client = bigquery.Client(project=gcp_project())

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project": gcp_project(),
        "dataset": bq_dataset(),
        "steps": {},
    }

    report["steps"]["schema"] = inspect_schema()
    report["steps"]["backfill"] = backfill_column_aliases(client)

    if args.apply_dedupe:
        report["steps"]["dedupe"] = dedupe_enriched(client)
    else:
        report["steps"]["dedupe"] = {"skipped": True, "hint": "pass --apply-dedupe"}

    report["verification"] = verify(client)
    report["drop_sql_file"] = "sql/drop_legacy_bq_tables.sql"
    report["drop_sql_ready"] = bool(
        report["verification"]["one_row_per_video"]
        and report["verification"]["null_primary_keys"] == 0
    )
    report["notes"] = [
        "Legacy BQ tables are deprecated; enrichment writes only to "
        f"{ENRICHED_TABLE} and {PIPELINE_LOGS_TABLE}.",
        "SQLite staging tables with the same names are unchanged and still used locally.",
        "Run sql/drop_legacy_bq_tables.sql only after drop_sql_ready is true "
        "and you have confirmed no external consumers.",
    ]

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    print(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {args.out}")
    if report["drop_sql_ready"]:
        print("Validation OK — safe to review and run sql/drop_legacy_bq_tables.sql")
    else:
        print("Validation incomplete — fix duplicates/nulls before dropping legacy tables")
    return 0 if report["drop_sql_ready"] else 2


if __name__ == "__main__":
    sys.exit(main())
