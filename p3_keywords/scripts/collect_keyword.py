"""Pipeline 3 — keyword search daily collection (server only).

Usage:
    python scripts/collect_keyword.py --date YYYY-MM-DD --sample
    python scripts/collect_keyword.py --date YYYY-MM-DD

Do not run the full 263-term list until the five-keyword sample is reviewed.
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
        description="Pipeline 3: keyword collection + CSV archive"
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
        help="Use sample_keywords (news, trump, tsa, ice, netanyahu)",
    )
    parser.add_argument("--keywords-file", default="")
    parser.add_argument("--limit-keywords", type=int, default=None)
    parser.add_argument(
        "--max-videos-per-keyword",
        type=int,
        default=None,
        help="Cap videos kept per keyword (1 API page still costs 1 quota unit). "
        "Required with --keywords-file unless set to 0 (no cap).",
    )
    parser.add_argument(
        "--utc-day",
        action="store_true",
        help="Query one UTC calendar day (start_date == end_date); no Chicago hour filter",
    )
    parser.add_argument(
        "--file-prefix",
        default="keyword",
        help="Export filename prefix (default: keyword)",
    )
    parser.add_argument("--reset-checkpoints", action="store_true")
    parser.add_argument("--skip-collect", action="store_true")
    parser.add_argument("--retry-failed", action="store_true")
    args = parser.parse_args()

    require_collection_server()

    from tiktok.pipelines import (
        PIPELINE_KEYWORD,
        get_pipeline,
        require_keyword_search_credentials,
    )

    try:
        require_keyword_search_credentials()
    except RuntimeError as e:
        print(str(e), flush=True)
        return 2

    from tiktok.collection.daily_keyword_pipeline import run_keyword_pipeline
    from tiktok.config import load_config
    from tiktok.logging_setup import setup_logging

    setup_logging()
    cfg = load_config(args.config)
    pipeline = get_pipeline(cfg, PIPELINE_KEYWORD)
    try:
        pipeline.resolve_credentials(cfg)
    except RuntimeError as e:
        print(str(e), flush=True)
        return 2
    if args.keywords_file and args.max_videos_per_keyword is None:
        print(
            "STOP: --keywords-file requires --max-videos-per-keyword "
            "(pass a small cap for tests; 0 means no cap).",
            flush=True,
        )
        return 2
    if args.keywords_file:
        preview = pipeline.resolve_keywords(
            cfg,
            sample=False,
            keywords_file=args.keywords_file,
            limit_keywords=args.limit_keywords,
        )
        print(
            "P3 keyword list:",
            len(preview),
            "terms;",
            "max_videos_per_keyword=",
            args.max_videos_per_keyword,
            "; utc_day=",
            bool(args.utc_day),
            "; terms=",
            preview,
            flush=True,
        )
        if len(preview) > 10 and int(args.max_videos_per_keyword or 0) == 0:
            print(
                "STOP: refusing uncapped --keywords-file with "
                f"{len(preview)} terms. Pass --max-videos-per-keyword N.",
                flush=True,
            )
            return 2
    report = run_keyword_pipeline(
        cfg=cfg,
        pipeline=pipeline,
        sample=args.sample,
        research_date=args.date,
        reset_checkpoints=args.reset_checkpoints,
        skip_collect=args.skip_collect,
        file_prefix=args.file_prefix or "keyword",
        retry_failed=args.retry_failed,
        keywords_file=args.keywords_file or None,
        limit_keywords=args.limit_keywords,
        utc_day=bool(args.utc_day),
        max_videos_per_keyword=args.max_videos_per_keyword,
    )
    if report.get("stop_reason"):
        return 1
    return 0 if int(report.get("api_failures") or 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
