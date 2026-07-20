#!/usr/bin/env python3
"""Safe retry pipeline for partial / low-quality enrichment rows.

- Primary key: video_id (BQ DELETE + INSERT upsert; no duplicates)
- Retries logged to tiktok_pipeline_logs
- Whisper: force download → ffmpeg WAV → Whisper (handles format_not_supported)
- OCR: force Vision re-run; cleaned OCR via postprocess (raw preserved)
- Emoji: re-extract after text layers

Usage (on comm-cme-p01 only):
    python scripts/retry_enrichment_partials.py --from-analysis data/partial_rows_analysis.json
    python scripts/retry_enrichment_partials.py --from-analysis data/partial_rows_analysis.json --priority A
    python scripts/retry_enrichment_partials.py --video-id ID
    python scripts/retry_enrichment_partials.py --backfill-all-from-sqlite
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.enrichment.bigquery_loader import (
    PIPELINE_VERSION,
    append_pipeline_logs,
    bigquery_configured,
    ensure_dataset_and_tables,
    sync_video_from_sqlite,
)
from tiktok.enrichment.store import ensure_enrichment_schema
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def _ensure_ffmpeg_path() -> None:
    home_bin = os.path.join(os.path.expanduser("~"), "bin")
    if os.path.isdir(home_bin):
        path = os.environ.get("PATH", "")
        if home_bin not in path.split(os.pathsep):
            os.environ["PATH"] = home_bin + os.pathsep + path


def _run(script: str, args: List[str]) -> int:
    _ensure_ffmpeg_path()
    cmd = [sys.executable, script] + args
    logger.info("Running: %s", " ".join(cmd))
    return subprocess.call(cmd, env=os.environ.copy())


def _log_retry(
    video_id: str,
    stage: str,
    status: str,
    *,
    retry_count: int,
    duration_seconds: float,
    error_type: str = "",
    error_message: str = "",
) -> None:
    if not bigquery_configured():
        return
    now = datetime.now(timezone.utc).isoformat()
    append_pipeline_logs(
        [
            {
                "log_id": str(uuid.uuid4()),
                "video_id": video_id,
                "stage": stage,
                "status": status,
                "retry_count": retry_count,
                "pipeline_version": PIPELINE_VERSION,
                "start_time": now,
                "end_time": now,
                "duration_seconds": duration_seconds,
                "error_type": (error_type or "")[:80],
                "error_message": (error_message or "")[:500],
                "worker_hostname": socket.gethostname(),
                "created_at": now,
            }
        ]
    )


def _load_targets(path: str, priority: Optional[str]) -> List[Dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rows = data.get("rows") or []
    if priority:
        p = priority.upper()
        rows = [
            r
            for r in rows
            if ((r.get("retry_recommendation") or {}).get("priority") or "").upper()
            == p
        ]
    return rows


def _sqlite_video_ids(conn) -> List[str]:
    return [
        r[0]
        for r in conn.execute("SELECT video_id FROM videos ORDER BY video_id").fetchall()
    ]


def _bq_video_ids() -> List[str]:
    """Only IDs already in tiktok_video_enriched (never the full SQLite catalog)."""
    from google.cloud import bigquery
    from tiktok.enrichment.bigquery_loader import enriched_table_id

    client = bigquery.Client()
    return [
        r.video_id
        for r in client.query(f"SELECT video_id FROM `{enriched_table_id()}`").result()
    ]


def retry_one(
    conn,
    video_id: str,
    *,
    do_whisper: bool,
    do_ocr: bool,
    do_emoji: bool,
    retry_count: int,
    max_frames: int,
    allow_web_fallback: bool = False,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {"video_id": video_id, "steps": {}}
    exists = conn.execute(
        "SELECT 1 FROM videos WHERE video_id=?", (video_id,)
    ).fetchone()
    if not exists:
        result["error"] = "not_in_sqlite"
        _log_retry(
            video_id,
            "retry_enrichment",
            "error",
            retry_count=retry_count,
            duration_seconds=0.0,
            error_type="not_in_sqlite",
            error_message="video missing from SQLite staging",
        )
        return result

    t0 = time.perf_counter()
    if do_whisper:
        code = _run(
            "scripts/transcription_worker.py",
            ["--video-id", video_id, "--force"],
        )
        result["steps"]["whisper"] = code
        _log_retry(
            video_id,
            "whisper_retry",
            "ok" if code == 0 else "error",
            retry_count=retry_count,
            duration_seconds=time.perf_counter() - t0,
            error_type="" if code == 0 else "whisper_retry_failed",
            error_message=f"exit={code}",
        )

    if do_ocr:
        t1 = time.perf_counter()
        ocr_args = ["--video-id", video_id, "--force", "--max-frames", str(max_frames)]
        if allow_web_fallback:
            ocr_args.append("--allow-web-fallback")
        code = _run("scripts/ocr_worker.py", ocr_args)
        result["steps"]["ocr"] = code
        _log_retry(
            video_id,
            "ocr_retry",
            "ok" if code == 0 else "error",
            retry_count=retry_count,
            duration_seconds=time.perf_counter() - t1,
            error_type="" if code == 0 else "ocr_retry_failed",
            error_message=f"exit={code}",
        )

    if do_emoji:
        t2 = time.perf_counter()
        code = _run(
            "scripts/emoji_worker.py",
            ["--video-id", video_id, "--force"],
        )
        result["steps"]["emoji"] = code
        _log_retry(
            video_id,
            "emoji_retry",
            "ok" if code == 0 else "error",
            retry_count=retry_count,
            duration_seconds=time.perf_counter() - t2,
            error_type="" if code == 0 else "emoji_retry_failed",
            error_message=f"exit={code}",
        )

    # Upsert analytics row (DELETE WHERE video_id + INSERT)
    try:
        counts = sync_video_from_sqlite(conn, video_id)
        result["bq_sync"] = counts
        _log_retry(
            video_id,
            "bq_upsert_retry",
            "ok",
            retry_count=retry_count,
            duration_seconds=time.perf_counter() - t0,
        )
    except Exception as e:
        result["bq_sync_error"] = str(e)
        _log_retry(
            video_id,
            "bq_upsert_retry",
            "error",
            retry_count=retry_count,
            duration_seconds=time.perf_counter() - t0,
            error_type="bq_upsert_failed",
            error_message=str(e),
        )
    result["elapsed_seconds"] = round(time.perf_counter() - t0, 2)
    return result


def backfill_from_sqlite(conn, video_ids: List[str]) -> Dict[str, int]:
    """Re-aggregate existing SQLite staging into BQ with v4.1 fields (no re-download)."""
    ok = fail = 0
    for i, vid in enumerate(video_ids, 1):
        logger.info("[%s/%s] backfill %s", i, len(video_ids), vid)
        try:
            counts = sync_video_from_sqlite(conn, vid)
            if counts.get("tiktok_video_enriched", 0) > 0:
                ok += 1
            else:
                fail += 1
        except Exception as e:
            fail += 1
            logger.error("backfill failed %s: %s", vid, e)
    return {"ok": ok, "fail": fail}


def main() -> int:
    parser = argparse.ArgumentParser(description="Retry partial enrichment rows")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--from-analysis", default="data/partial_rows_analysis.json")
    parser.add_argument("--priority", choices=["A", "B", "C"], default=None)
    parser.add_argument("--video-id", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-frames", type=int, default=12)
    parser.add_argument(
        "--backfill-all-from-sqlite",
        action="store_true",
        help="Re-sync all SQLite videos to BQ with new OCR cleaning/fields (no Vision/Whisper)",
    )
    parser.add_argument(
        "--skip-whisper",
        action="store_true",
        help="Do not re-run Whisper even if missing",
    )
    parser.add_argument(
        "--skip-ocr",
        action="store_true",
        help="Do not re-run OCR even if missing",
    )
    parser.add_argument(
        "--allow-web-fallback",
        action="store_true",
        help="Pass --allow-web-fallback to OCR worker (helps download_failed)",
    )
    parser.add_argument(
        "--out",
        default="data/retry_enrichment_partials_report.json",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    _ensure_ffmpeg_path()

    if not bigquery_configured():
        logger.error("BigQuery not configured")
        return 1
    ensure_dataset_and_tables()

    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_version": PIPELINE_VERSION,
        "results": [],
    }

    if args.backfill_all_from_sqlite:
        # Safety: only re-aggregate rows already in BigQuery (SQLite has 30k+ videos)
        ids = _bq_video_ids()
        sqlite_ids = set(_sqlite_video_ids(conn))
        ids = [v for v in ids if v in sqlite_ids]
        if args.limit:
            ids = ids[: args.limit]
        report["mode"] = "backfill_bq_scoped_from_sqlite"
        report["backfill"] = backfill_from_sqlite(conn, ids)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        print(json.dumps(report["backfill"], indent=2))
        conn.close()
        return 0

    targets: List[Dict[str, Any]] = []
    if args.video_id:
        targets = [
            {
                "video_id": args.video_id,
                "missing_layers": {"whisper": True, "OCR": True, "emoji": True},
                "previous_attempts": 0,
            }
        ]
    else:
        if not os.path.isfile(args.from_analysis):
            logger.error("Analysis file missing: %s — run analyze_partial_rows.py", args.from_analysis)
            return 1
        targets = _load_targets(args.from_analysis, args.priority)

    if args.limit is not None:
        targets = targets[: args.limit]

    logger.info("Retrying %s videos", len(targets))
    for i, t in enumerate(targets, 1):
        vid = t["video_id"]
        missing = t.get("missing_layers") or {}
        do_whisper = (not args.skip_whisper) and bool(missing.get("whisper", True))
        do_ocr = (not args.skip_ocr) and bool(missing.get("OCR", True))
        # Always refresh emoji after text retries
        do_emoji = True
        # If analysis says only emoji missing, still refresh emoji; skip workers
        if not missing.get("whisper") and not missing.get("OCR") and missing.get("emoji"):
            do_whisper = False
            do_ocr = False
        retry_count = int(t.get("previous_attempts") or 0) + 1
        logger.info(
            "[%s/%s] %s whisper=%s ocr=%s",
            i,
            len(targets),
            vid,
            do_whisper,
            do_ocr,
        )
        result = retry_one(
            conn,
            vid,
            do_whisper=do_whisper,
            do_ocr=do_ocr,
            do_emoji=do_emoji,
            retry_count=retry_count,
            max_frames=args.max_frames,
            allow_web_fallback=args.allow_web_fallback,
        )
        report["results"].append(result)

    ok = sum(1 for r in report["results"] if r.get("bq_sync"))
    report["summary"] = {
        "attempted": len(report["results"]),
        "bq_upsert_ok": ok,
        "not_in_sqlite": sum(
            1 for r in report["results"] if r.get("error") == "not_in_sqlite"
        ),
    }
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report["summary"], indent=2))
    print(f"Wrote {args.out}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
