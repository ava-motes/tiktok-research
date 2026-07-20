#!/usr/bin/env python3
"""One-shot BigQuery consistency repair for production gate.

Fixes (on comm-cme-p01):
  1. Deduplicate any video_id with >1 row
  2. Re-sync / re-enrich known problem rows (paulette, newsnationnow)
  3. Re-run production validation

Usage:
    python scripts/fix_bq_consistency.py
    python scripts/fix_bq_consistency.py --skip-retry
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.enrichment.bigquery_loader import (
    bigquery_configured,
    dedupe_all_video_ids,
    sync_video_from_sqlite,
)
from tiktok.enrichment.store import ensure_enrichment_schema
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)

# Known hygiene targets from production validation / partial audit
PAULETTE_ID = "7625992856901111070"
NEWSNATION_ID = "7660286527884315917"
COHEN_DUP_ID = "7659906481889840414"


def _run(script: str, args: List[str]) -> int:
    cmd = [sys.executable, script] + args
    logger.info("Running: %s", " ".join(cmd))
    return subprocess.call(cmd, env=os.environ.copy())


def main() -> int:
    parser = argparse.ArgumentParser(description="Fix BQ consistency issues")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--skip-retry",
        action="store_true",
        help="Only dedupe + re-sync from SQLite (no Whisper/OCR force)",
    )
    parser.add_argument(
        "--validate-out",
        default="data/production_validation_report.json",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    if not bigquery_configured():
        logger.error("BigQuery not configured")
        return 1

    removed = dedupe_all_video_ids()
    logger.info("Global dedupe removed %s duplicate row(s)", removed)

    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    targets = [PAULETTE_ID, NEWSNATION_ID, COHEN_DUP_ID]
    if not args.skip_retry:
        # Force Whisper for paulette (empty transcript with ok status)
        rc = _run(
            "scripts/transcription_worker.py",
            ["--config", args.config, "--video-id", PAULETTE_ID, "--force"],
        )
        logger.info("Paulette transcription exit=%s", rc)
        # Refresh OCR/emoji lightly not required for newsnation; re-sync rebuilds status
        for vid in (PAULETTE_ID, NEWSNATION_ID):
            rc_e = _run(
                "scripts/emoji_worker.py",
                ["--config", args.config, "--video-id", vid, "--force"],
            )
            logger.info("Emoji refresh %s exit=%s", vid, rc_e)

    results = {}
    for vid in targets:
        try:
            counts = sync_video_from_sqlite(conn, vid)
            results[vid] = counts
            logger.info("Synced %s -> %s", vid, counts)
        except Exception as e:
            results[vid] = {"error": str(e)}
            logger.error("Sync failed for %s: %s", vid, e)

    # Second global dedupe pass after syncs
    removed2 = dedupe_all_video_ids()
    logger.info("Post-sync dedupe removed %s duplicate row(s)", removed2)
    conn.close()

    val_rc = _run(
        "scripts/run_production_validation.py",
        ["--out", args.validate_out],
    )
    summary = {
        "dedupe_removed": removed + removed2,
        "sync_results": results,
        "validation_exit_code": val_rc,
    }
    print(json.dumps(summary, indent=2, default=str))
    return 0 if val_rc == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
