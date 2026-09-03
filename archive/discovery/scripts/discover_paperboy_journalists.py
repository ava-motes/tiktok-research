"""Paperboy News Account Discovery — review CSV only (not a production pipeline).

Uses NEWS_API_CLIENT_KEY / NEWS_API_CLIENT_SECRET and research/user/info.
Does not collect videos, enrich, write SQLite/BigQuery, or edit handle lists.

Usage (comm-cme-p01 only):
    python scripts/discover_paperboy_journalists.py --sample
    python scripts/discover_paperboy_journalists.py
    python scripts/discover_paperboy_journalists.py --csv PATH
"""

from __future__ import annotations

import argparse
import os
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.collection.server_guard import require_collection_server


def _load_base_url(config_path: str) -> str:
    with open(config_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    url = ((raw.get("tiktok") or {}).get("base_url") or "").strip()
    if not url:
        raise RuntimeError(f"Missing tiktok.base_url in {config_path}")
    return url


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Paperboy journalist handle discovery (review CSV only). "
            "Not Pipeline 1, 2, or 3."
        )
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--csv",
        default=None,
        help="Paperboy journalist CSV (default: config/discovery/paperboy_journalist_list.csv)",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="First 10 cleaned journalists only",
    )
    parser.add_argument("--reset-checkpoints", action="store_true")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry candidates checkpointed as failed (HTTP 500 etc.)",
    )
    args = parser.parse_args()

    require_collection_server()

    from dotenv import load_dotenv

    load_dotenv()

    from tiktok.pipelines import require_news_credentials

    try:
        client_key, client_secret = require_news_credentials()
    except RuntimeError as e:
        print(str(e), flush=True)
        return 2

    from tiktok import auth
    from tiktok.api.client import TikTokClient
    from tiktok.discovery.paperboy_journalists import (
        DEFAULT_CSV,
        DISCOVERY_RAW_DIR,
        P1_HANDLE_FILE,
        P2_HANDLE_FILE,
        load_known_handles,
        run_discovery,
    )
    from tiktok.logging_setup import setup_logging

    setup_logging()
    csv_path = args.csv or DEFAULT_CSV
    if not os.path.isfile(csv_path):
        print(f"Paperboy CSV not found: {csv_path}", flush=True)
        return 2

    base_url = _load_base_url(args.config)
    auth.init(base_url, client_key, client_secret)
    client = TikTokClient(base_url, DISCOVERY_RAW_DIR, db_conn=None)
    known = load_known_handles(P1_HANDLE_FILE, P2_HANDLE_FILE)
    stats = run_discovery(
        csv_path=csv_path,
        client=client,
        known=known,
        sample=args.sample,
        reset_checkpoints=args.reset_checkpoints,
        retry_failed=args.retry_failed,
    )
    print(
        "paperboy_discovery journalists={journalists} candidates={candidates_generated} "
        "already_known={candidates_already_known} api_calls={api_calls_attempted} "
        "found={successful_user_info_responses} not_found={accounts_not_found} "
        "csv={output_csv}".format(**stats),
        flush=True,
    )
    if stats.get("stopped_reason"):
        return 1
    return 0 if int(stats.get("api_errors") or 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
