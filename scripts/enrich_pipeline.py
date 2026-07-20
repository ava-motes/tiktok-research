#!/usr/bin/env python3
"""Orchestrate enrichment workers and optional BigQuery sync.

Usage (on comm-cme-p01 only):
    python scripts/enrich_pipeline.py --group batch_test --limit 5
    python scripts/enrich_pipeline.py --group sample --limit 100 --sync-bigquery
    python scripts/enrich_pipeline.py --ensure-bq-schema
    python scripts/enrich_pipeline.py --inspect-bq-schema

Does not modify TikTok API collection. Failures in one worker do not stop others.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.logging_setup import setup_logging
from tiktok.enrichment.store import ensure_enrichment_schema, fetch_videos_for_enrichment

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
    conn, video_ids: List[str], path: str, wall_seconds: float, bq_ok: int, bq_fail: int
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
        if t and t[0] == "ok":
            whisper_ok += 1
            if t[1]:
                whisper_text += 1
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

    metrics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "processed": n,
        "wall_clock_seconds": round(wall_seconds, 2),
        "avg_wall_seconds_per_video": round(wall_seconds / n, 2) if n else None,
        "ocr": {
            "success_count": ocr_ok,
            "success_rate": round(ocr_ok / n, 3) if n else None,
            "avg_latency_seconds": round(sum(ocr_lat) / len(ocr_lat), 2) if ocr_lat else None,
        },
        "whisper": {
            "success_count": whisper_ok,
            "success_rate": round(whisper_ok / n, 3) if n else None,
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
        "video_ids": video_ids,
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    logger.info(
        "Processed=%s OCR=%.0f%% Whisper=%.0f%% BQ=%s/%s avg_wall=%.1fs/video",
        n,
        100 * (metrics["ocr"]["success_rate"] or 0),
        100 * (metrics["whisper"]["success_rate"] or 0),
        bq_ok,
        bq_ok + bq_fail,
        metrics["avg_wall_seconds_per_video"] or 0,
    )
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(description="Run enrichment pipeline")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--group", default=None)
    parser.add_argument("--video-id", default=None)
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
        help="After enrichment, upsert rows into tiktok_video_enriched",
    )
    parser.add_argument(
        "--ensure-bq-schema",
        action="store_true",
        help="Only create/migrate BigQuery tiktok_video_enriched and exit",
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
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)

    if args.ensure_bq_schema or args.inspect_bq_schema:
        from tiktok.enrichment.bigquery_loader import (
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
    if args.limit is not None:
        common += ["--limit", str(args.limit)]
    if use_force:
        common += ["--force"]

    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)
    handles = cfg.get_handles(args.group) if args.group else None
    video_ids_arg = [args.video_id] if args.video_id else None
    # Same selection workers use when --force (no need_* filters)
    preselect = fetch_videos_for_enrichment(
        conn, handles=handles, video_ids=video_ids_arg, limit=args.limit
    )
    video_ids = [r["video_id"] for r in preselect]
    logger.info("Candidate set size: %s", len(video_ids))

    t0 = time.perf_counter()
    codes = {}
    if "transcript" in steps:
        codes["transcript"] = _run("scripts/transcription_worker.py", common)
    if "ocr" in steps:
        ocr_extra = ["--max-frames", str(args.max_frames)]
        if args.allow_web_fallback:
            ocr_extra.append("--allow-web-fallback")
        codes["ocr"] = _run("scripts/ocr_worker.py", common + ocr_extra)
    if "emoji" in steps:
        codes["emoji"] = _run("scripts/emoji_worker.py", common)

    bq_ok = 0
    bq_fail = 0
    if args.sync_bigquery:
        from tiktok.enrichment.bigquery_loader import (
            bigquery_configured,
            sync_video_from_sqlite,
        )

        if not bigquery_configured():
            logger.error("BigQuery not configured; skip sync")
        else:
            for vid in video_ids:
                try:
                    counts = sync_video_from_sqlite(conn, vid)
                    logger.info("BQ sync %s: %s", vid, counts)
                    if counts.get("tiktok_video_enriched", 0) > 0:
                        bq_ok += 1
                    else:
                        bq_fail += 1
                except Exception as e:
                    bq_fail += 1
                    logger.error("BQ sync failed for %s: %s", vid, e)

    wall = time.perf_counter() - t0
    _write_sample_metrics(conn, video_ids, args.metrics_report, wall, bq_ok, bq_fail)
    conn.close()

    logger.info("Pipeline finished: %s", codes)
    return 0 if all(c == 0 for c in codes.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
