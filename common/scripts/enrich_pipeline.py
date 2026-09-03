#!/usr/bin/env python3
"""Orchestrate Whisper / OCR / emoji workers and pipeline BigQuery sync.

Shared infrastructure: one copy of the workers. Each pipeline runner must pass
**only its own** ``--pipeline`` (``content_creators``, ``news``, or ``keyword``)
so BigQuery writes stay on that table. This script never writes
``tiktok_video_enriched`` (that workflow is in ``archive/v5/``).

Usage (comm-cme-p01 only)::

    python common/scripts/enrich_pipeline.py \\
        --pipeline content_creators --video-ids-file PATH --incremental --sync-bigquery

    python common/scripts/enrich_pipeline.py --ensure-bq-schema
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.logging_setup import setup_logging
from enrichment.store import ensure_enrichment_schema, fetch_videos_for_enrichment

logger = logging.getLogger(__name__)


def _ensure_ffmpeg_path() -> None:
    """Prefer user-local ~/bin ffmpeg (comm-cme-p01) for workers."""
    home_bin = os.path.join(os.path.expanduser("~"), "bin")
    if os.path.isdir(home_bin):
        path = os.environ.get("PATH", "")
        if home_bin not in path.split(os.pathsep):
            os.environ["PATH"] = home_bin + os.pathsep + path


def _run(script: str, extra: List[str]) -> int:
    _ensure_ffmpeg_path()
    cmd = [sys.executable, script] + extra
    logger.info("Running: %s", " ".join(cmd))
    env = os.environ.copy()
    return subprocess.call(cmd, env=env)


def _write_sample_metrics(
    conn,
    video_ids: List[str],
    path: str,
    wall_seconds: float,
    bq_ok: int,
    bq_fail: int,
    *,
    validation: Optional[Dict[str, Any]] = None,
    export_paths: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    n = len(video_ids)
    ocr_ok = whisper_ok = whisper_text = emoji_ok = 0
    ocr_lat: List[float] = []
    wh_lat: List[float] = []
    for vid in video_ids:
        if conn.execute("SELECT COUNT(*) FROM video_ocr WHERE video_id=?", (vid,)).fetchone()[0]:
            ocr_ok += 1
        t = conn.execute(
            "SELECT status, transcript FROM video_transcripts WHERE video_id=?", (vid,)
        ).fetchone()
        if t and (t[1] or "").strip():
            whisper_ok += 1
            whisper_text += 1
        elif t and t[0] == "ok":
            whisper_ok += 0  # status alone is not success
        if conn.execute("SELECT COUNT(*) FROM video_emojis WHERE video_id=?", (vid,)).fetchone()[0]:
            emoji_ok += 1
        for worker, bucket in (("ocr", ocr_lat), ("transcription", wh_lat)):
            row = conn.execute(
                """SELECT elapsed_seconds FROM enrichment_log
                   WHERE video_id=? AND worker=? ORDER BY id DESC LIMIT 1""",
                (vid, worker),
            ).fetchone()
            if row and row[0] is not None:
                bucket.append(float(row[0]))

    # Rough cost estimate (Vision per frame + Whisper per minute of audio duration)
    from enrichment.bigquery_loader import (
        PIPELINE_VERSION,
        VISION_USD_PER_IMAGE,
        WHISPER_USD_PER_MINUTE,
    )

    whisper_min = 0.0
    vision_frames = 0
    for vid in video_ids:
        dur = conn.execute(
            "SELECT COALESCE(audio_duration_seconds, 0) FROM video_transcripts WHERE video_id=?",
            (vid,),
        ).fetchone()
        if dur and dur[0]:
            whisper_min += float(dur[0]) / 60.0
        fr = conn.execute(
            "SELECT COUNT(*) FROM video_ocr WHERE video_id=?", (vid,)
        ).fetchone()
        if fr:
            vision_frames += int(fr[0] or 0)
    est_cost = whisper_min * WHISPER_USD_PER_MINUTE + vision_frames * VISION_USD_PER_IMAGE

    metrics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_version": PIPELINE_VERSION,
        "processed": n,
        "wall_clock_seconds": round(wall_seconds, 2),
        "avg_wall_seconds_per_video": round(wall_seconds / n, 2) if n else None,
        "ocr": {
            "success_count": ocr_ok,
            "success_rate": round(ocr_ok / n, 3) if n else None,
            "avg_latency_seconds": round(sum(ocr_lat) / len(ocr_lat), 2) if ocr_lat else None,
        },
        "whisper": {
            "success_count": whisper_text,
            "success_rate": round(whisper_text / n, 3) if n else None,
            "with_transcript_text": whisper_text,
            "avg_latency_seconds": round(sum(wh_lat) / len(wh_lat), 2) if wh_lat else None,
        },
        "emoji": {
            "videos_with_emojis": emoji_ok,
            "rate": round(emoji_ok / n, 3) if n else None,
        },
        "bigquery": {
            "synced_ok": bq_ok,
            "synced_fail": bq_fail,
        },
        "estimated_cost_usd": round(est_cost, 4),
        "validation": validation,
        "export": export_paths,
        "video_ids": video_ids,
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    logger.info(
        "Processed=%s OCR=%.0f%% Whisper=%.0f%% BQ=%s/%s avg_wall=%.1fs/video cost~$%.4f",
        n,
        100 * (metrics["ocr"]["success_rate"] or 0),
        100 * (metrics["whisper"]["success_rate"] or 0),
        bq_ok,
        bq_ok + bq_fail,
        metrics["avg_wall_seconds_per_video"] or 0,
        est_cost,
    )
    return metrics


def _append_validation_log(report: Dict[str, Any]) -> None:
    """Record production validation outcome in tiktok_pipeline_logs."""
    try:
        from enrichment.bigquery_loader import (
            PIPELINE_VERSION,
            append_pipeline_logs,
            bigquery_configured,
        )

        if not bigquery_configured():
            return
        now = datetime.now(timezone.utc).isoformat()
        ok = bool(report.get("passed"))
        failed = report.get("failed_checks") or []
        append_pipeline_logs(
            [
                {
                    "log_id": str(uuid.uuid4()),
                    "video_id": "_pipeline_",
                    "stage": "production_validation",
                    "status": "ok" if ok else "error",
                    "retry_count": 0,
                    "pipeline_version": PIPELINE_VERSION,
                    "start_time": now,
                    "end_time": now,
                    "duration_seconds": 0.0,
                    "error_type": "" if ok else "validation_failed",
                    "error_message": (
                        "" if ok else f"failed_checks={','.join(failed)}"
                    )[:500],
                    "worker_hostname": os.uname().nodename,
                    "created_at": now,
                }
            ]
        )
    except Exception as e:
        logger.warning("Could not append validation pipeline log: %s", e)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run enrichment pipeline")
    parser.add_argument("--config", default="common/config.yaml")
    parser.add_argument("--group", default=None)
    parser.add_argument("--video-id", default=None)
    parser.add_argument(
        "--video-ids-file",
        default=None,
        help="Text file with one video_id per line (Pipeline 1)",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--steps",
        default="transcript,ocr,emoji",
        help="Comma list: transcript,ocr,emoji",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-enrich even if transcript/OCR/emoji already exist. "
        "Omit for incremental production runs (only incomplete/failed videos).",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Alias for default safe mode: skip videos already enriched "
        "(do not pass --force). Preferred for daily jobs.",
    )
    parser.add_argument("--max-frames", type=int, default=12)
    parser.add_argument(
        "--allow-web-fallback",
        action="store_true",
        help="Pass through to OCR worker when Vision SA key is not installed yet",
    )
    parser.add_argument(
        "--sync-bigquery",
        action="store_true",
        help="After enrichment, upsert rows into the isolated --pipeline table "
        "(content_creators / news / keyword). Never writes tiktok_video_enriched.",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Required for BigQuery writes: content_creators, news, or keyword. "
        "The old tiktok_video_enriched path lives in archive/v5/.",
    )
    parser.add_argument(
        "--collection-date",
        default="",
        help="Research/collection date YYYY-MM-DD for Box CSV name after BQ sync",
    )
    parser.add_argument(
        "--skip-box",
        action="store_true",
        help="Do not upload the collection-date CSV to UT Box after BigQuery sync",
    )
    parser.add_argument(
        "--export-research",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help="Incremental enrich + BigQuery sync for --pipeline (no v5 export)",
    )
    parser.add_argument(
        "--ensure-bq-schema",
        action="store_true",
        help="Create/migrate pipeline BigQuery tables and exit",
    )
    parser.add_argument(
        "--inspect-bq-schema",
        action="store_true",
        help="Print BigQuery schema field list and exit",
    )
    parser.add_argument(
        "--metrics-report",
        default="data/enrichment_sample_metrics.json",
        help="Write sample success/latency metrics JSON",
    )
    parser.add_argument(
        "--validation-report",
        default="data/production_validation_report.json",
    )
    parser.add_argument(
        "--export-prefix",
        default="data/exports/tiktok_research_enriched",
    )
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)

    if args.export_research:
        raise RuntimeError(
            "--export-research is the archived v5 tiktok_video_enriched exporter. "
            "Use each pipeline's results/ and validate_*.py. "
            "See archive/v5/scripts/export_research_dataset.py."
        )

    if args.production:
        args.incremental = True
        args.sync_bigquery = True
        args.force = False
        logger.info(
            "Production mode: incremental enrich → BQ upsert for --pipeline"
        )

    if args.ensure_bq_schema or args.inspect_bq_schema:
        from enrichment.bigquery_loader import (
            ensure_dataset_and_tables,
            inspect_schema,
        )

        ensure_dataset_and_tables()
        if args.inspect_bq_schema:
            info = inspect_schema()
            print(json.dumps(info, indent=2))
            missing = []
            for tinfo in (info.get("tables") or {}).values():
                missing.extend(tinfo.get("missing") or [])
            if missing:
                logger.error("Missing fields: %s", missing)
                return 1
            logger.info("Schema OK (%s tables)", len(info.get("tables") or {}))
        else:
            logger.info("BigQuery schema ensured")
        return 0

    steps = {s.strip().lower() for s in args.steps.split(",") if s.strip()}
    # Validation batches with --limit + --sync-bigquery historically forced a shared set.
    # Production daily runs should pass --incremental (or omit --force) so only gaps are filled.
    if args.incremental and args.force:
        logger.warning("--incremental overrides --force")
        args.force = False
    use_force = (not args.incremental) and (
        args.force or (args.sync_bigquery and args.limit is not None and not args.incremental)
    )
    if use_force and not args.force:
        logger.info("Enabling --force so workers share one coherent candidate set")
    if not use_force:
        logger.info("Incremental mode: skipping videos already successfully enriched")

    common: List[str] = ["--config", args.config]
    if args.group:
        common += ["--group", args.group]
    if args.video_id:
        common += ["--video-id", args.video_id]
    if args.video_ids_file:
        common += ["--video-ids-file", args.video_ids_file]
    if args.limit is not None:
        common += ["--limit", str(args.limit)]
    if use_force:
        common += ["--force"]

    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)
    handles = cfg.get_handles(args.group) if args.group else None
    video_ids_arg = [args.video_id] if args.video_id else None
    if args.video_ids_file:
        from tiktok.collection.video_ids import resolve_video_ids

        from argparse import Namespace

        video_ids_arg = resolve_video_ids(
            Namespace(video_id=args.video_id, video_ids_file=args.video_ids_file)
        )
    # Same selection workers use when --force (no need_* filters)
    preselect = fetch_videos_for_enrichment(
        conn, handles=handles, video_ids=video_ids_arg, limit=args.limit
    )
    video_ids = [r["video_id"] for r in preselect]
    logger.info("Candidate set size: %s", len(video_ids))

    t0 = time.perf_counter()
    codes: Dict[str, int] = {}
    if "transcript" in steps:
        codes["transcript"] = _run("common/scripts/transcription_worker.py", common)
    if "ocr" in steps:
        ocr_extra = ["--max-frames", str(args.max_frames)]
        if args.allow_web_fallback:
            ocr_extra.append("--allow-web-fallback")
        codes["ocr"] = _run("common/scripts/ocr_worker.py", common + ocr_extra)
    if "emoji" in steps:
        codes["emoji"] = _run("common/scripts/emoji_worker.py", common)

    bq_ok = 0
    bq_fail = 0
    if args.sync_bigquery:
        from enrichment.bigquery_loader import (
            CONTENT_CREATORS_TABLE,
            KEYWORD_TABLE,
            NEWS_TABLE,
            bigquery_configured,
            sync_content_creator_video,
            sync_keyword_search_video,
            sync_news_account_video,
        )
        from tiktok.pipelines import PIPELINE_ID_ALIASES

        pipeline_id = (args.pipeline or "").strip()
        pipeline_id = PIPELINE_ID_ALIASES.get(pipeline_id, pipeline_id)
        if pipeline_id not in ("content_creators", "news", "keyword"):
            raise RuntimeError(
                "BigQuery sync requires --pipeline content_creators|news|keyword. "
                "Active pipelines never write tiktok_video_enriched "
                "(that workflow is in archive/v5/)."
            )
        if not bigquery_configured():
            logger.error("BigQuery not configured; skip sync")
            bq_fail = len(video_ids) or 1
        else:
            for vid in video_ids:
                try:
                    if pipeline_id == "content_creators":
                        counts = sync_content_creator_video(conn, vid)
                        written = counts.get(CONTENT_CREATORS_TABLE, 0)
                    elif pipeline_id == "news":
                        counts = sync_news_account_video(conn, vid)
                        written = counts.get(NEWS_TABLE, 0)
                    elif pipeline_id == "keyword":
                        counts = sync_keyword_search_video(conn, vid)
                        written = counts.get(KEYWORD_TABLE, 0)
                    else:
                        raise RuntimeError(f"Unknown pipeline_id={pipeline_id!r}")
                    logger.info("BQ sync %s: %s", vid, counts)
                    if written > 0:
                        bq_ok += 1
                    else:
                        bq_fail += 1
                except Exception as e:
                    bq_fail += 1
                    logger.error("BQ sync failed for %s: %s", vid, e)

            if (
                pipeline_id in ("content_creators", "news", "keyword")
                and not args.skip_box
                and bq_ok > 0
            ):
                from tiktok.box_delivery import (
                    infer_collection_date,
                    maybe_deliver_after_bq,
                )

                collection_date = (args.collection_date or "").strip()
                if not collection_date:
                    collection_date = infer_collection_date(conn, video_ids)
                box_result = maybe_deliver_after_bq(
                    pipeline_id=pipeline_id,
                    collection_date=collection_date,
                    box_cfg=getattr(cfg, "box", None) or {},
                )
                logger.info("Box CSV delivery: %s", box_result)
                print(
                    f"Box CSV delivery: {json.dumps(box_result, default=str)}",
                    flush=True,
                )

    validation_report = None
    validation_failed = False
    export_paths = None
    if getattr(args, "validate", False):
        raise RuntimeError(
            "--validate is the archived v5 tiktok_video_enriched checker. "
            "Use p1/p2/p3 validate_*.py instead. "
            "See archive/v5/scripts/run_production_validation.py."
        )

    wall = time.perf_counter() - t0
    _write_sample_metrics(
        conn,
        video_ids,
        args.metrics_report,
        wall,
        bq_ok,
        bq_fail,
        validation=validation_report,
        export_paths=export_paths,
    )
    conn.close()

    worker_failed = any(c != 0 for c in codes.values())
    logger.info("Pipeline finished: workers=%s validation_failed=%s", codes, validation_failed)

    # Non-zero when workers fail or critical validation fails
    if validation_failed:
        return 2
    if worker_failed or (args.sync_bigquery and bq_fail and not bq_ok and video_ids):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
