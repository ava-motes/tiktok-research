"""Pipeline 2 daily command — collect, enrich, upsert news.

Server only (comm-cme-p01). Does not write tiktok_video_enriched or
content_creators. Requires NEWS_API_CLIENT_KEY / SECRET (no P1/P3 fallback).

    python p2_news/scripts/run_news.py --date YYYY-MM-DD --utc-day --skip-whisper
    python p2_news/scripts/run_news.py --date YYYY-MM-DD --sample --utc-day --skip-whisper

Calls common/scripts/enrich_pipeline.py --pipeline news only.
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
        description="Pipeline 2 daily: collect + enrich + news"
    )
    parser.add_argument("--config", default="common/config.yaml")
    parser.add_argument("--date", required=True, help="Research date YYYY-MM-DD")
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use news_sample (first two unique handles from news_accounts.txt)",
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
        "--batch-size",
        type=int,
        default=None,
        help="Collect/enrich this many pending handles per batch (default all for --sample)",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry handles checkpointed as failed (HTTP 500 etc.), not successful completions",
    )
    parser.add_argument(
        "--skip-whisper",
        action="store_true",
        help="Enrich with OCR and emoji only (skip Whisper transcription)",
    )
    parser.add_argument(
        "--utc-day",
        action="store_true",
        help="Query one UTC calendar day: TikTok start_date == end_date "
        "(keep all returned videos; no Chicago hour filter)",
    )
    parser.add_argument(
        "--skip-gcs",
        action="store_true",
        help="Do not archive the completed CSV to GCS",
    )
    args = parser.parse_args()

    require_collection_server()

    from tiktok.pipelines import PIPELINE_NEWS, get_pipeline, require_news_credentials

    try:
        require_news_credentials()
    except RuntimeError as e:
        print(str(e), flush=True)
        return 2

    from tiktok.collection.daily_handle_pipeline import run_handle_pipeline
    from tiktok.config import load_config
    from tiktok.logging_setup import setup_logging

    setup_logging()
    cfg = load_config(args.config)
    pipeline = get_pipeline(cfg, PIPELINE_NEWS)

    try:
        pipeline.resolve_credentials(cfg)
    except RuntimeError as e:
        print(str(e), flush=True)
        return 2

    batch_size = args.batch_size

    if not args.skip_bigquery:
        try:
            from enrichment.bigquery_loader import ensure_news_table

            ensure_news_table()
            print("BigQuery news schema ensured", flush=True)
        except Exception as e:
            print(f"STOP: BigQuery schema error: {e}", flush=True)
            return 2

    t0 = time.perf_counter()
    started = datetime.now(timezone.utc).isoformat()
    totals = {
        "handles_attempted": 0,
        "handles_query_ok": 0,
        "api_rows_seen": 0,
        "inserted_new": 0,
        "upserted_existing": 0,
        "duplicate_skips": 0,
        "api_failures": 0,
        "user_info_ok": 0,
        "user_info_failed": 0,
        "videos_enriched": 0,
        "enrich_exit_codes": [],
        "csv_paths": [],
        "ids_paths": [],
        "report_paths": [],
        "collected_video_ids": [],
        "batches": 0,
        "stop_reason": "",
        "last_report_path": "",
    }
    last_collect: dict = {}
    enrich_rc = 0

    while True:
        totals["batches"] += 1
        print(
            f"=== Pipeline 2 batch {totals['batches']} "
            f"(batch_size={batch_size or 'all'}) ===",
            flush=True,
        )
        collect = run_handle_pipeline(
            cfg=cfg,
            pipeline=pipeline,
            sample=args.sample,
            research_date=args.date,
            reset_checkpoints=args.reset_checkpoints and totals["batches"] == 1,
            skip_collect=args.skip_collect,
            file_prefix="news",
            batch_size=batch_size,
            retry_failed=args.retry_failed and totals["batches"] == 1,
            utc_day=args.utc_day,
        )
        last_collect = collect
        totals["last_report_path"] = collect.get("report_path") or ""
        if collect.get("csv_path"):
            totals["csv_paths"].append(collect["csv_path"])
        if collect.get("ids_path"):
            totals["ids_paths"].append(collect["ids_path"])
        if collect.get("report_path"):
            totals["report_paths"].append(collect["report_path"])

        for key in (
            "handles_attempted",
            "handles_query_ok",
            "api_rows_seen",
            "inserted_new",
            "upserted_existing",
            "duplicate_skips",
            "api_failures",
            "user_info_ok",
            "user_info_failed",
        ):
            totals[key] += int(collect.get(key) or 0)

        batch_ids = list(collect.get("collected_video_ids") or [])
        totals["collected_video_ids"].extend(batch_ids)

        stop_reason = (collect.get("stop_reason") or "").strip()
        if stop_reason:
            totals["stop_reason"] = stop_reason
            print(f"STOP: {stop_reason}", flush=True)

        if args.skip_enrich:
            enrich_rc = 0
        elif not batch_ids:
            print("No new videos in this batch; skipping enrichment/BQ", flush=True)
            enrich_rc = 0
        else:
            extra = [
                "--config",
                args.config,
                "--pipeline",
                PIPELINE_NEWS,
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
            totals["enrich_exit_codes"].append(enrich_rc)
            totals["videos_enriched"] += len(batch_ids)
            if enrich_rc != 0:
                totals["stop_reason"] = totals["stop_reason"] or f"enrich_pipeline_exit={enrich_rc}"
                print(f"STOP: enrichment/BQ failed with exit {enrich_rc}", flush=True)

        if args.skip_collect:
            break
        if totals["stop_reason"]:
            break
        if not collect.get("more_pending"):
            break

    export_dir = pipeline.resolved_export_dir(cfg)
    summary_dir = pipeline.resolved_summary_dir(cfg)
    combined_ids_path = os.path.join(
        export_dir,
        f"news_video_ids_combined_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt",
    )
    os.makedirs(export_dir, exist_ok=True)
    seen_ids = []
    seen_set = set()
    for vid in totals["collected_video_ids"]:
        if vid and vid not in seen_set:
            seen_set.add(vid)
            seen_ids.append(vid)
    with open(combined_ids_path, "w", encoding="utf-8") as f:
        for vid in seen_ids:
            f.write(f"{vid}\n")
    totals["ids_paths"].append(combined_ids_path)

    val_rc = _run(
        "p2_news/scripts/validate_news.py",
        [
            "--config",
            args.config,
            "--date",
            args.date,
            "--ids-file",
            combined_ids_path,
            "--collect-report",
            totals["last_report_path"] or last_collect.get("report_path") or "",
            "--started-at",
            started,
            "--runtime-seconds",
            f"{time.perf_counter() - t0:.3f}",
            "--enrich-exit",
            str(enrich_rc if enrich_rc is not None else ""),
            "--sample" if args.sample else "--full-list",
        ]
        + (["--utc-day"] if args.utc_day else []),
    )

    summary_path = os.path.join(
        summary_dir,
        f"news_full_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    os.makedirs(summary_dir, exist_ok=True)
    full_summary = {
        **{k: v for k, v in totals.items() if k != "collected_video_ids"},
        "unique_collected_video_ids": len(set(totals["collected_video_ids"])),
        "started_at": started,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": round(time.perf_counter() - t0, 3),
        "sample_mode": bool(args.sample),
        "batch_size": batch_size,
        "research_date": args.date,
        "utc_day": bool(args.utc_day),
        "validate_exit": val_rc,
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(full_summary, f, indent=2, ensure_ascii=False)
    print(json.dumps(full_summary, indent=2), flush=True)
    print(f"Wrote {summary_path}", flush=True)

    if totals["stop_reason"]:
        return 1
    if int(totals.get("api_failures") or 0) != 0:
        return 1
    if enrich_rc not in (0, None):
        return enrich_rc
    if val_rc != 0:
        return val_rc

    from tiktok.gcs_archive import upload_run_csv_after_success

    return upload_run_csv_after_success(
        run_fn=_run,
        pipeline_id=PIPELINE_NEWS,
        research_date=args.date,
        cfg=cfg,
        pipeline=pipeline,
        csv_paths=totals.get("csv_paths") or [],
        skip=args.skip_gcs,
    )


if __name__ == "__main__":
    raise SystemExit(main())
