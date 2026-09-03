"""Pipeline 1 daily command — collect, enrich, upsert content_creators.

Server only (comm-cme-p01). Does not write tiktok_video_enriched.

    python p1_content_creators/scripts/run_content_creators.py \\
        --date YYYY-MM-DD --utc-day --skip-whisper --continue-on-failures --skip-user-info
    python p1_content_creators/scripts/run_content_creators.py --date YYYY-MM-DD --sample --utc-day --skip-whisper

Calls common/scripts/enrich_pipeline.py --pipeline content_creators only.
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
        description="Pipeline 1 daily: collect + enrich + content_creators"
    )
    parser.add_argument("--config", default="common/config.yaml")
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
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Collect/enrich this many pending handles per batch (default 50 unless --sample)",
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
        "--continue-on-failures",
        action="store_true",
        help="If a handle hits HTTP 500 after retries, skip it and keep "
        "collecting the rest (do not abort on consecutive/fail-rate guards)",
    )
    parser.add_argument(
        "--utc-day",
        action="store_true",
        help="Query one UTC calendar day: TikTok start_date == end_date "
        "(keep all returned videos; no Chicago hour filter)",
    )
    parser.add_argument(
        "--skip-user-info",
        action="store_true",
        help="Do not call research/user/info after video/query (saves ~1 "
        "request per handle; use when daily quota is tight)",
    )
    parser.add_argument(
        "--skip-gcs",
        action="store_true",
        help="Do not archive the completed CSV to GCS",
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

    if args.sample:
        batch_size = None
    else:
        batch_size = args.batch_size if args.batch_size is not None else 50

    if not args.skip_bigquery:
        try:
            from enrichment.bigquery_loader import ensure_content_creators_table

            ensure_content_creators_table()
            print("BigQuery content_creators schema ensured", flush=True)
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
    hard_stops = (
        "daily_quota_limit_exceeded",
        "authentication_failure",
        "rate_limited HTTP 429",
    )

    def run_collect_loop(*, retry_failed_once: bool, reset_once: bool) -> None:
        nonlocal last_collect, enrich_rc
        first = True
        while True:
            totals["batches"] += 1
            print(
                f"=== Pipeline 1 batch {totals['batches']} "
                f"(batch_size={batch_size or 'all'}) ===",
                flush=True,
            )
            collect = run_handle_pipeline(
                cfg=cfg,
                pipeline=pipeline,
                sample=args.sample,
                research_date=args.date,
                reset_checkpoints=reset_once and first,
                skip_collect=args.skip_collect,
                file_prefix="content_creators",
                batch_size=batch_size,
                retry_failed=retry_failed_once and first,
                continue_on_failures=args.continue_on_failures,
                utc_day=args.utc_day,
                skip_user_info=args.skip_user_info,
            )
            first = False
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
                    PIPELINE_CONTENT_CREATORS,
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
                    totals["stop_reason"] = (
                        totals["stop_reason"] or f"enrich_pipeline_exit={enrich_rc}"
                    )
                    print(
                        f"STOP: enrichment/BQ failed with exit {enrich_rc}",
                        flush=True,
                    )

            if args.skip_collect:
                break
            if totals["stop_reason"]:
                break
            if not collect.get("more_pending"):
                break

    run_collect_loop(retry_failed_once=args.retry_failed, reset_once=args.reset_checkpoints)

    # Skip errors on the first pass; retry those failed handles once at the end.
    stop_now = (totals.get("stop_reason") or "").strip()
    if (
        args.continue_on_failures
        and not args.skip_collect
        and not args.retry_failed
        and int(totals.get("api_failures") or 0) > 0
        and stop_now not in hard_stops
        and not stop_now.startswith("enrich_pipeline_exit")
    ):
        print(
            "=== Retry failed handles once (end of run; successful checkpoints kept) ===",
            flush=True,
        )
        run_collect_loop(retry_failed_once=True, reset_once=False)

    export_dir = pipeline.resolved_export_dir(cfg)
    summary_dir = pipeline.resolved_summary_dir(cfg)
    combined_ids_path = os.path.join(
        export_dir,
        f"content_creators_video_ids_combined_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt",
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
        "p1_content_creators/scripts/validate_content_creators.py",
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
        ],
    )

    summary_path = os.path.join(
        summary_dir,
        f"content_creators_full_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
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
        pipeline_id=PIPELINE_CONTENT_CREATORS,
        research_date=args.date,
        cfg=cfg,
        pipeline=pipeline,
        csv_paths=totals.get("csv_paths") or [],
        skip=args.skip_gcs,
    )


if __name__ == "__main__":
    raise SystemExit(main())
