"""Pipeline 3 daily command — collect, enrich, upsert keyword.

Server only (comm-cme-p01). Does not write tiktok_video_enriched,
content_creators, or news.
Requires KEYWORD_SEARCH_API_CLIENT_KEY / SECRET (no P1/P2 fallback).

    python p3_keywords/scripts/run_keyword.py --date YYYY-MM-DD --sample --utc-day --skip-whisper
    python p3_keywords/scripts/run_keyword.py --date YYYY-MM-DD --utc-day --skip-whisper

Calls common/scripts/enrich_pipeline.py --pipeline keyword only.

Do not run the full 263-keyword collection until the five-keyword sample
has been reviewed. Use an older Chicago research date, not a just-finished day.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

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


def _run(script: str, extra: list[str]) -> int:
    cmd = [sys.executable, script] + extra
    print("Running:", " ".join(cmd), flush=True)
    return subprocess.call(cmd, env=os.environ.copy())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pipeline 3 daily: collect + enrich + keyword"
    )
    parser.add_argument("--config", default="common/config.yaml")
    parser.add_argument("--date", required=True, help="Research date YYYY-MM-DD")
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use sample_keywords (news, trump, tsa, ice, netanyahu)",
    )
    parser.add_argument(
        "--keywords-file",
        default="",
        help="Optional keyword file override (one term per line)",
    )
    parser.add_argument(
        "--limit-keywords",
        type=int,
        default=None,
        help="Optional cap on the resolved keyword list length",
    )
    parser.add_argument(
        "--max-videos-per-keyword",
        type=int,
        default=None,
        help="Cap videos kept per keyword. Required with --keywords-file unless 0.",
    )
    parser.add_argument(
        "--utc-day",
        action="store_true",
        help="Query one UTC calendar day (start_date == end_date); no Chicago hour filter",
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
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry keywords checkpointed as failed (HTTP 500 etc.)",
    )
    parser.add_argument(
        "--skip-whisper",
        action="store_true",
        help="Enrich with OCR and emoji only (skip Whisper transcription)",
    )
    parser.add_argument(
        "--skip-gcs",
        action="store_true",
        help="Do not archive the completed CSV to GCS",
    )
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

    if not args.skip_bigquery:
        try:
            from enrichment.bigquery_loader import ensure_keyword_table

            ensure_keyword_table()
            print("BigQuery keyword schema ensured", flush=True)
        except Exception as e:
            print(f"STOP: BigQuery schema error: {e}", flush=True)
            return 2

    if args.keywords_file and args.max_videos_per_keyword is None:
        print(
            "STOP: --keywords-file requires --max-videos-per-keyword "
            "(pass a small cap for tests; 0 means no cap).",
            flush=True,
        )
        return 2

    t0 = time.perf_counter()
    started = datetime.now(timezone.utc).isoformat()
    collect = run_keyword_pipeline(
        cfg=cfg,
        pipeline=pipeline,
        sample=args.sample,
        research_date=args.date,
        reset_checkpoints=args.reset_checkpoints,
        skip_collect=args.skip_collect,
        file_prefix="keyword",
        retry_failed=args.retry_failed,
        keywords_file=args.keywords_file or None,
        limit_keywords=args.limit_keywords,
        utc_day=bool(args.utc_day),
        max_videos_per_keyword=args.max_videos_per_keyword,
    )

    batch_ids = list(collect.get("collected_video_ids") or [])
    enrich_rc = 0
    if args.skip_enrich:
        enrich_rc = 0
    elif not batch_ids:
        print("No new videos; skipping enrichment/BQ", flush=True)
        enrich_rc = 0
    else:
        extra = [
            "--config",
            args.config,
            "--pipeline",
            PIPELINE_KEYWORD,
            "--video-ids-file",
            collect.get("ids_path") or "",
            "--incremental",
        ]
        if not args.skip_bigquery:
            extra.append("--sync-bigquery")
            extra.extend(["--collection-date", args.date])
        if args.skip_whisper:
            extra.extend(["--steps", "ocr,emoji"])
        enrich_rc = _run("common/scripts/enrich_pipeline.py", extra)
        if enrich_rc != 0:
            collect["stop_reason"] = (
                collect.get("stop_reason") or f"enrich_pipeline_exit={enrich_rc}"
            )
            print(f"STOP: enrichment/BQ failed with exit {enrich_rc}", flush=True)

    combined_ids_path = collect.get("ids_path") or ""
    val_rc = _run(
        "p3_keywords/scripts/validate_keyword.py",
        [
            "--config",
            args.config,
            "--date",
            args.date,
            "--ids-file",
            combined_ids_path,
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

    summary_dir = pipeline.resolved_summary_dir(cfg)
    summary_path = os.path.join(
        summary_dir,
        f"keyword_full_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    os.makedirs(summary_dir, exist_ok=True)
    full_summary = {
        "pipeline_id": PIPELINE_KEYWORD,
        "research_date": args.date,
        "sample_mode": bool(args.sample),
        "keyword_count": collect.get("keyword_count"),
        "inserted_new": collect.get("inserted_new"),
        "upserted_existing": collect.get("upserted_existing"),
        "excluded_pipeline_1": collect.get("excluded_pipeline_1"),
        "excluded_pipeline_2": collect.get("excluded_pipeline_2"),
        "excluded_overlap": collect.get("excluded_overlap"),
        "api_failures": collect.get("api_failures"),
        "unique_collected_video_ids": len(set(batch_ids)),
        "stop_reason": collect.get("stop_reason") or "",
        "started_at": started,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": round(time.perf_counter() - t0, 3),
        "enrich_exit": enrich_rc,
        "validate_exit": val_rc,
        "ids_path": combined_ids_path,
        "report_path": collect.get("report_path"),
        "max_videos_per_keyword": collect.get("max_videos_per_keyword"),
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(full_summary, f, indent=2, ensure_ascii=False)
    print(json.dumps(full_summary, indent=2), flush=True)
    print(f"Wrote {summary_path}", flush=True)

    if collect.get("stop_reason"):
        return 1
    if int(collect.get("api_failures") or 0) != 0:
        return 1
    if enrich_rc not in (0, None):
        return enrich_rc
    if val_rc != 0:
        return val_rc

    from tiktok.gcs_archive import upload_run_csv_after_success

    csv_paths = [collect.get("csv_path") or ""]
    return upload_run_csv_after_success(
        run_fn=_run,
        pipeline_id=PIPELINE_KEYWORD,
        research_date=args.date,
        cfg=cfg,
        pipeline=pipeline,
        csv_paths=csv_paths,
        skip=args.skip_gcs,
    )


if __name__ == "__main__":
    raise SystemExit(main())
