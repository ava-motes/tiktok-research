#!/usr/bin/env python3
"""Rebuild tiktok_video_enriched to the final content schema and reload from SQLite.

Usage (comm-cme-p01 ONLY):
  python scripts/rebuild_tiktok_video_enriched.py
  python scripts/rebuild_tiktok_video_enriched.py --limit 50
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
from tiktok.enrichment.bigquery_loader import (
    BQ_SCHEMAS,
    ENRICHED_TABLE,
    PIPELINE_VERSION,
    build_enriched_row,
    enriched_table_id,
    ensure_dataset_and_tables,
    gcp_project,
    load_enriched_rows,
)
from tiktok.enrichment.store import ensure_enrichment_schema
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def _candidate_video_ids(conn, limit: int = None) -> List[str]:
    """Videos that have any enrichment staging and/or are in videos."""
    sql = """
    SELECT DISTINCT v.video_id
    FROM videos v
    WHERE v.video_id IN (SELECT video_id FROM video_transcripts)
       OR v.video_id IN (SELECT DISTINCT video_id FROM video_ocr)
       OR v.video_id IN (SELECT DISTINCT video_id FROM video_emojis)
    ORDER BY v.create_time DESC
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [r[0] for r in conn.execute(sql).fetchall()]


def rebuild_empty_table(client) -> str:
    """Backup current table and create empty final-schema table."""
    from google.cloud import bigquery

    table = enriched_table_id()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup = f"{gcp_project()}.tiktok_research.tiktok_video_enriched_backup_{stamp}"
    exists = True
    try:
        client.get_table(table)
    except Exception:
        exists = False
    if exists:
        logger.info("Backing up %s → %s", table, backup)
        client.query(f"CREATE TABLE `{backup}` AS SELECT * FROM `{table}`").result()
        client.query(f"DROP TABLE `{table}`").result()

    schema = [
        bigquery.SchemaField(f["name"], f["type"]) for f in BQ_SCHEMAS[ENRICHED_TABLE]
    ]
    client.create_table(bigquery.Table(table, schema=schema))
    logger.info("Created empty %s with final schema (%s fields)", table, len(schema))
    return backup if exists else ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--out", default="data/rebuild_tiktok_video_enriched.json")
    parser.add_argument(
        "--skip-backup-reload",
        action="store_true",
        help="Only ensure schema columns (no DROP/reload)",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    from google.cloud import bigquery

    client = bigquery.Client(project=gcp_project())
    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_version": PIPELINE_VERSION,
        "table": enriched_table_id(),
        "schema_fields": [f["name"] for f in BQ_SCHEMAS[ENRICHED_TABLE]],
    }

    if args.skip_backup_reload:
        ensure_dataset_and_tables()
        report["mode"] = "ensure_columns_only"
    else:
        backup = rebuild_empty_table(client)
        ensure_dataset_and_tables()
        report["backup_table"] = backup
        vids = _candidate_video_ids(conn, args.limit)
        report["candidates"] = len(vids)

        rows: List[Dict[str, Any]] = []
        failures = []
        for i, vid in enumerate(vids, 1):
            try:
                row = build_enriched_row(conn, vid)
                if row:
                    rows.append(row)
                else:
                    failures.append({"video_id": vid, "error": "build_returned_none"})
            except Exception as e:
                failures.append({"video_id": vid, "error": str(e)[:200]})
            if i % 100 == 0 or i == len(vids):
                logger.info(
                    "Built %s/%s rows (ok=%s fail=%s)",
                    i,
                    len(vids),
                    len(rows),
                    len(failures),
                )

        logger.info("Batch loading %s rows into %s", len(rows), enriched_table_id())
        loaded = load_enriched_rows(rows) if rows else 0
        report["reloaded_ok"] = loaded
        report["reloaded_fail"] = len(failures)
        report["failures"] = failures[:50]

    # Verify
    table = enriched_table_id()
    total = list(client.query(f"SELECT COUNT(*) n FROM `{table}`").result())[0].n
    dups = list(
        client.query(
            f"SELECT COUNT(*) n FROM (SELECT video_id FROM `{table}` GROUP BY video_id HAVING COUNT(*)>1)"
        ).result()
    )[0].n
    nulls = list(
        client.query(
            f"SELECT COUNT(*) n FROM `{table}` WHERE video_id IS NULL OR video_id=''"
        ).result()
    )[0].n
    fields = [f.name for f in client.get_table(table).schema]
    samples = [
        dict(r)
        for r in client.query(
            f"""SELECT video_id, creator_username, posted_at,
                       SUBSTR(IFNULL(caption,''),1,60) caption,
                       SUBSTR(IFNULL(whisper_transcript,''),1,80) whisper_transcript,
                       SUBSTR(IFNULL(ocr_text,''),1,80) ocr_text,
                       emoji_characters, enrichment_status, pipeline_version
                FROM `{table}` ORDER BY enrichment_date DESC LIMIT 5"""
        ).result()
    ]
    report["verification"] = {
        "total_rows": int(total),
        "duplicate_video_ids": int(dups),
        "null_primary_keys": int(nulls),
        "schema_fields": fields,
        "expected_fields": [f["name"] for f in BQ_SCHEMAS[ENRICHED_TABLE]],
        "samples": samples,
    }
    report["ok"] = int(dups) == 0 and int(nulls) == 0

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    sys.exit(main())
