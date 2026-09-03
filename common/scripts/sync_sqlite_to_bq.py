"""Upload existing SQLite collection rows to BigQuery. No TikTok API calls.

    python scripts/sync_sqlite_to_bq.py --pipeline keyword --date 2026-08-15

Server only. Does not start collection or enrichment workers.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from pathlib import Path
import importlib.util

def _setup_repo():
    for p in Path(__file__).resolve().parents:
        boot = p / "common" / "bootstrap.py"
        if boot.is_file():
            spec = importlib.util.spec_from_file_location("_tiktok_bootstrap", boot)
            mod = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(mod)
            return mod.setup()
    raise RuntimeError("common/bootstrap.py not found")

ROOT = _setup_repo()

from tiktok.collection.server_guard import require_collection_server


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batch-sync one pipeline collection_date from SQLite to BigQuery"
    )
    parser.add_argument("--config", default="common/config.yaml")
    parser.add_argument(
        "--pipeline",
        required=True,
        choices=("keyword",),
        help="Only keyword batch sync is implemented (86k-row safe)",
    )
    parser.add_argument("--date", required=True, help="Collection date YYYY-MM-DD")
    args = parser.parse_args()

    require_collection_server()

    from tiktok.config import load_config
    from tiktok.db import get_connection
    from enrichment.bigquery_loader import sync_keyword_collection_date
    from tiktok.logging_setup import setup_logging

    setup_logging()
    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    result = sync_keyword_collection_date(conn, args.date)
    print(json.dumps(result, indent=2), flush=True)
    return 0 if result.get("keyword", 0) > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
