"""Controlled single-handle data collection (user profile + bounded video pull).

Does not use group checkpoints; runs one account sequentially with hard caps.
Writes a timestamped JSON summary under ``data/``.

Usage (from project root):
    source venv/bin/activate
    python scripts/single_account_run.py --handle nickdiramio

    # Tighter scope (fewer API calls, shorter run):
    python scripts/single_account_run.py --handle nickdiramio --days 7 --max-videos 30

    # Profile only (no video/query API):
    python scripts/single_account_run.py --handle nickdiramio --user-info-only

Exit code: 0 if user info succeeds; 1 on config/auth errors or user API failure.
Video pull failures are logged; partial video data still exits 0 if user info OK.

See also: ``scripts/pull_videos.py``, ``scripts/pull_user_info.py`` for multi-account runs.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok import auth
from tiktok.api.client import TikTokClient
from tiktok.api.users import get_user_info
from tiktok.api.videos import date_chunks, query_videos_for_chunk
from tiktok.config import load_config
from tiktok.db import get_connection, insert_user, insert_video
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def _normalize_handle(raw: str) -> str:
    h = raw.strip()
    if h.startswith("@"):
        h = h[1:]
    return h


def _timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _write_summary(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _count_videos_for_handle(conn, username: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE username = ?",
        (username,),
    ).fetchone()
    return int(row[0]) if row else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Single-account bounded TikTok data collection (production-like)"
    )
    parser.add_argument(
        "--handle",
        required=True,
        help="One TikTok username (no @ required)",
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Only pull videos from the past N days (default: 14)",
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=50,
        help="Hard cap on video rows to fetch across all date chunks (default: 50)",
    )
    parser.add_argument(
        "--user-info-only",
        action="store_true",
        help="Only fetch user profile; skip video/query (fastest sanity check)",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    started = datetime.now(timezone.utc)
    handle = _normalize_handle(args.handle)
    if not handle:
        logger.error("Empty handle after normalization")
        return 1

    slug = _timestamp_slug()
    summary_path = os.path.join("data", f"single_run_{slug}.json")

    report: Dict[str, Any] = {
        "run_kind": "single_account",
        "handle": handle,
        "started_at": started.isoformat(),
        "config_path": args.config,
        "params": {
            "days": args.days,
            "max_videos": args.max_videos,
            "user_info_only": args.user_info_only,
        },
        "user_info": {},
        "videos": {},
        "status": "running",
    }

    logger.info("Single-account run starting at %s for @%s", report["started_at"], handle)
    logger.info(
        "Scope: days=%s max_videos=%s user_info_only=%s",
        args.days,
        args.max_videos,
        args.user_info_only,
    )

    try:
        cfg = load_config(args.config)
    except Exception as e:
        logger.error("load_config failed (%s): %s", type(e).__name__, e)
        report["status"] = "failed"
        report["error"] = {"stage": "load_config", "type": type(e).__name__, "detail": str(e)[:500]}
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        _write_summary(summary_path, report)
        return 1

    auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)

    conn = get_connection(cfg.paths["database"])
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"], db_conn=conn)

    # --- User info (single request) ---
    user = get_user_info(client, handle)
    insert_user(conn, user)
    conn.commit()

    ui_ok = not bool(user.get("api_failed"))
    report["user_info"] = {
        "ok": ui_ok,
        "api_failed": int(user.get("api_failed", 0)),
        "display_name": user.get("display_name", ""),
        "follower_count": user.get("follower_count", 0),
        "video_count": user.get("video_count", 0),
        "is_verified": bool(user.get("is_verified", False)),
    }

    if ui_ok:
        logger.info(
            "User info OK — display_name=%r followers=%s",
            user.get("display_name", ""),
            user.get("follower_count", 0),
        )
    else:
        logger.error(
            "User info failed for @%s (api_failed=1). Check logs for HTTP 401/403/429.",
            handle,
        )
        report["status"] = "failed"
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["notes"] = [
            "User info endpoint returned failure; see project logs for status code "
            "(401/403 = auth or scope; 429 = rate limit).",
        ]
        _write_summary(summary_path, report)
        conn.close()
        return 1

    # --- Videos (bounded) ---
    video_summary: Dict[str, Any] = {
        "chunks_processed": 0,
        "rows_inserted": 0,
        "stopped_reason": None,
    }

    if args.user_info_only:
        video_summary["stopped_reason"] = "user_info_only"
        report["videos"] = video_summary
        finished = datetime.now(timezone.utc)
        report["finished_at"] = finished.isoformat()
        report["status"] = "success"
        report["duration_seconds"] = (finished - started).total_seconds()
        report["summary_file"] = summary_path
        logger.info(
            "Finished (user-info-only). Wrote %s — duration %.1fs",
            summary_path,
            report["duration_seconds"],
        )
        _write_summary(summary_path, report)
        conn.close()
        return 0

    end_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=max(1, args.days) - 1)).strftime(
        "%Y%m%d"
    )
    chunks = date_chunks(start_date, end_date)
    # date_chunks() returns [] when start == end (e.g. --days 1); still query that day once.
    if not chunks:
        chunks = [(start_date, end_date)]
    report["date_range"] = {"start": start_date, "end": end_date, "chunk_count": len(chunks)}

    db_videos_before = _count_videos_for_handle(conn, handle)
    logger.info("DB video rows for @%s before pull: %s", handle, db_videos_before)

    remaining = args.max_videos
    total_inserted = 0

    for chunk_start, chunk_end in chunks:
        if remaining <= 0:
            video_summary["stopped_reason"] = "max_videos_reached"
            break

        video_summary["chunks_processed"] += 1
        try:
            rows = query_videos_for_chunk(
                client, handle, chunk_start, chunk_end, max_videos=remaining
            )
        except RuntimeError as e:
            if "daily_quota" in str(e).lower() or "quota" in str(e).lower():
                logger.error("TikTok quota exceeded — stop scaling until quota resets: %s", e)
                video_summary["stopped_reason"] = "daily_quota"
            else:
                logger.error("Video query aborted: %s", e)
                video_summary["stopped_reason"] = "runtime_error"
            video_summary["rows_inserted"] = total_inserted
            report["videos"] = video_summary
            report["status"] = "failed"
            report["finished_at"] = datetime.now(timezone.utc).isoformat()
            _write_summary(summary_path, report)
            conn.close()
            return 1

        if not rows and remaining > 0:
            logger.warning(
                "No videos for @%s in chunk %s–%s. "
                "If data/raw/videos/%s.jsonl shows http_status 200 and videos=[], "
                "the account has no posts in this date window. "
                "If TikTokClient logged ERROR with a status code, that is an API failure.",
                handle,
                chunk_start,
                chunk_end,
                handle,
            )

        for v in rows:
            insert_video(conn, v)
        conn.commit()
        n = len(rows)
        total_inserted += n
        remaining -= n
        logger.info(
            "Chunk %s–%s: inserted %s videos (total this run: %s)",
            chunk_start,
            chunk_end,
            n,
            total_inserted,
        )

    if video_summary.get("stopped_reason") is None:
        video_summary["stopped_reason"] = (
            "completed_all_chunks" if remaining > 0 else "max_videos_reached"
        )

    video_summary["api_rows_fetched"] = total_inserted
    video_summary["rows_inserted"] = total_inserted  # alias for backward compatibility

    db_videos_after = _count_videos_for_handle(conn, handle)
    db_net_new = db_videos_after - db_videos_before
    video_summary["db_video_count_before"] = db_videos_before
    video_summary["db_video_count_after"] = db_videos_after
    video_summary["db_net_new_rows"] = db_net_new

    # INSERT OR IGNORE: net new rows can be < api rows if video_ids already existed.
    match = db_net_new == total_inserted
    video_summary["integrity"] = {
        "api_rows_vs_db_net_new_match": match,
        "note": (
            "All fetched rows became new DB rows."
            if match
            else (
                "Some rows were skipped by INSERT OR IGNORE (duplicate video_id) "
                "or inserts were partial — compare api_rows_fetched vs db_net_new_rows."
            )
        ),
    }
    if total_inserted == 0 and db_net_new == 0:
        video_summary["integrity"]["api_rows_vs_db_net_new_match"] = True
        video_summary["integrity"]["note"] = "No API rows; DB unchanged for this handle."

    integrity_failed = False
    if db_net_new > total_inserted:
        logger.error(
            "Integrity anomaly: db_net_new_rows (%s) > api_rows_fetched (%s)",
            db_net_new,
            total_inserted,
        )
        video_summary["integrity"]["fatal"] = True
        integrity_failed = True

    logger.info(
        "DB reconciliation: api_rows_fetched=%s db_net_new_rows=%s (before=%s after=%s)",
        total_inserted,
        db_net_new,
        db_videos_before,
        db_videos_after,
    )

    report["videos"] = video_summary

    finished = datetime.now(timezone.utc)
    report["finished_at"] = finished.isoformat()
    report["duration_seconds"] = (finished - started).total_seconds()
    report["status"] = "failed" if integrity_failed else "success"
    report["summary_file"] = summary_path

    logger.info(
        "Single-account run finished: api_rows_fetched=%s, %.1fs — %s",
        total_inserted,
        report["duration_seconds"],
        summary_path,
    )

    _write_summary(summary_path, report)
    conn.close()
    return 1 if integrity_failed else 0


if __name__ == "__main__":
    sys.exit(main())
