"""Pipeline 1 daily command — collect, enrich, upsert tiktok_content_creators.

Server only (comm-cme-p01). Does not write tiktok_video_enriched.

    python scripts/run_content_creators.py --date 2026-08-25 --sample
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.collection.server_guard import require_collection_server


def _run(script: str, extra: list[str]) -> int:
    cmd = [sys.executable, script] + extra
    print("Running:", " ".join(cmd), flush=True)
    return subprocess.call(cmd, env=os.environ.copy())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pipeline 1 daily: collect + enrich + tiktok_content_creators"
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--date", required=True, help="Research date YYYY-MM-DD")
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use batch_test (underthedesknews, aaronparnas1)",
    )
    parser.add_argument("--reset-checkpoints", action="store_true")
    parser.add_argument("--skip-collect", action="store_true")
    parser.add_argument(
        "--skip-enrich",
        action="store_true",
        help="Collection/export only (no Whisper/OCR/emoji/BQ)",
    )
    parser.add_argument(
        "--skip-bigquery",
        action="store_true",
        help="Run enrichment workers but do not upsert BigQuery",
    )
    args = parser.parse_args()

    require_collection_server()

    from tiktok.collection.daily_handle_pipeline import run_handle_pipeline
    from tiktok.config import load_config
    from tiktok.logging_setup import setup_logging
    from tiktok.pipelines import PIPELINE_CONTENT_CREATORS, get_pipeline

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)
    pipeline = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)

    t0 = time.perf_counter()
    started = datetime.now(timezone.utc).isoformat()
    collect = run_handle_pipeline(
        cfg=cfg,
        pipeline=pipeline,
        sample=args.sample,
        research_date=args.date,
        reset_checkpoints=args.reset_checkpoints,
        skip_collect=args.skip_collect,
        file_prefix="content_creators",
    )
    ids_path = collect.get("ids_path") or ""
    video_ids = list(collect.get("collected_video_ids") or [])

    enrich_rc = None
    if args.skip_enrich:
        enrich_rc = 0
    elif not video_ids:
        print("No videos in window; skipping enrichment/BQ", flush=True)
        enrich_rc = 0
    else:
        extra = [
            "--config",
            args.config,
            "--pipeline",
            PIPELINE_CONTENT_CREATORS,
            "--video-ids-file",
            ids_path,
            "--incremental",
        ]
        if not args.skip_bigquery:
            extra.append("--sync-bigquery")
        enrich_rc = _run("scripts/enrich_pipeline.py", extra)

    val_rc = _run(
        "scripts/validate_content_creators.py",
        [
            "--config",
            args.config,
            "--date",
            args.date,
            "--ids-file",
            ids_path,
            "--collect-report",
            collect.get("report_path") or "",
            "--started-at",
            started,
            "--runtime-seconds",
            f"{time.perf_counter() - t0:.3f}",
            "--enrich-exit",
            str(enrich_rc if enrich_rc is not None else ""),
            "--sample" if args.sample else "--full-list",
        ],
    )

    if int(collect.get("api_failures") or 0) != 0:
        return 1
    if enrich_rc not in (0, None):
        return enrich_rc
    return 0 if val_rc == 0 else val_rc


if __name__ == "__main__":
    raise SystemExit(main())
