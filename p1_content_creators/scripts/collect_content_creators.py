"""Pipeline 1 — content creators daily collection (server only).

Usage:
    python scripts/collect_content_creators.py --date 2026-08-25 --sample
    python scripts/collect_content_creators.py --date 2026-08-25
"""

from __future__ import annotations

import argparse
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
        description="Pipeline 1: content_creators collection + CSV archive"
    )
    parser.add_argument("--config", default="common/config.yaml")
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
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry handles checkpointed as failed (HTTP 500 etc.)",
    )
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
        retry_failed=args.retry_failed,
    )
    return 0 if int(report.get("api_failures") or 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
