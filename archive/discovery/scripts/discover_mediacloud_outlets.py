"""MediaCloud US outlet TikTok-handle discovery — review CSV only.

Uses Pipeline 1 credentials (TIKTOK_CLIENT_* / CONTENT_CREATOR_*) and
research/user/info. Does not collect videos, enrich, write SQLite/BigQuery,
or edit P1/P2 handle lists. Pipeline 2 NEWS_API is left for Paperboy.

Usage (comm-cme-p01 only):
    python scripts/discover_mediacloud_outlets.py --sample
    python scripts/discover_mediacloud_outlets.py
    python scripts/discover_mediacloud_outlets.py --csv PATH
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
            "MediaCloud outlet handle discovery (review CSV only). "
            "Not Pipeline 1, 2, or 3. Does not edit handle lists."
        )
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--csv",
        default=None,
        help="MediaCloud outlets CSV (default: config/discovery/mediacloud_us_news_outlets.csv)",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="First 10 outlets only",
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

    from tiktok.config import load_config
    from tiktok.pipelines import PIPELINE_CONTENT_CREATORS, get_pipeline

    cfg = load_config(args.config)
    pipeline = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
    try:
        client_key, client_secret = pipeline.resolve_credentials(cfg)
    except RuntimeError as e:
        print(str(e), flush=True)
        return 2

    from tiktok import auth
    from tiktok.api.client import TikTokClient
    from tiktok.discovery.mediacloud_outlets import (
        DEFAULT_CSV,
        DISCOVERY_OUT_DIR,
        P1_HANDLE_FILE,
        P2_HANDLE_FILE,
        load_known_handles,
        run_discovery,
    )
    from tiktok.logging_setup import setup_logging

    setup_logging()
    csv_path = args.csv or DEFAULT_CSV
    if not os.path.isfile(csv_path):
        print(f"MediaCloud outlets CSV not found: {csv_path}", flush=True)
        return 2

    os.makedirs(DISCOVERY_OUT_DIR, exist_ok=True)
    base_url = _load_base_url(args.config)
    auth.init(base_url, client_key, client_secret)
    client = TikTokClient(base_url, os.path.join("data", "discovery", "raw_outlets"), db_conn=None)
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
        "mediacloud_outlet_discovery outlets={outlets} candidates={candidates_generated} "
        "already_known={candidates_already_known} api_calls={api_calls_attempted} "
        "found={successful_user_info_responses} not_found={accounts_not_found} "
        "high_confidence={high_confidence_candidates} csv={output_csv}".format(**stats),
        flush=True,
    )
    if stats.get("stopped_reason"):
        print("stopped_reason={}".format(stats["stopped_reason"]), flush=True)
        return 1
    return 0 if int(stats.get("api_errors") or 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
