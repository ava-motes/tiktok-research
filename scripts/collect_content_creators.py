"""Pipeline 1 — content creators daily collection (server only).

Usage:
    python scripts/collect_content_creators.py --date 2026-08-25 --sample
    python scripts/collect_content_creators.py --date 2026-08-25
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.collection.server_guard import require_collection_server


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pipeline 1: content_creators collection + CSV archive"
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--date",
        required=True,
        help="Research date YYYY-MM-DD (America/Chicago window, stored as UTC)",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use sample_handle_group (batch_test) instead of complete",
    )
    parser.add_argument("--reset-checkpoints", action="store_true")
    parser.add_argument("--skip-collect", action="store_true")
    args = parser.parse_args()

    require_collection_server()

    from tiktok.collection.daily_handle_pipeline import run_handle_pipeline
    from tiktok.config import load_config
    from tiktok.logging_setup import setup_logging
    from tiktok.pipelines import PIPELINE_CONTENT_CREATORS, get_pipeline

    setup_logging()
    cfg = load_config(args.config)
    pipeline = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
    report = run_handle_pipeline(
        cfg=cfg,
        pipeline=pipeline,
        sample=args.sample,
        research_date=args.date,
        reset_checkpoints=args.reset_checkpoints,
        skip_collect=args.skip_collect,
        file_prefix="content_creators",
    )
    return 0 if int(report.get("api_failures") or 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
