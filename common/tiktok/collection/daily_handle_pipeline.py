"""Username-based daily collection for Pipeline 1 (content creators) and
Pipeline 2 (news accounts).

Keyword collectors must not be treated as production until approved.
v5.0 ``scripts/pull_videos.py`` is unchanged and still uses ``date_chunks``.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from tiktok import auth
from api.client import TikTokClient
from api.users import get_user_info
from api.videos import query_videos_for_chunk
from tiktok.checkpoint import CheckpointStore
from tiktok.collection.date_window import (
    ResearchWindow,
    research_window,
    utc_calendar_window,
)
from tiktok.config import Config
from tiktok.db import get_connection, insert_user, upsert_collected_video
from tiktok.pipelines import (
    PipelineSpec,
    is_collectable_handle,
    normalize_handle,
)

logger = logging.getLogger(__name__)

CONSECUTIVE_API_FAIL_LIMIT = 8
FAIL_RATE_MIN_HANDLES = 20
FAIL_RATE_MAX = 0.25
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_SLEEP_SECONDS = 2.0


def _api_error_code_from_exc(exc: BaseException) -> str:
    msg = str(exc or "").lower()
    if "internal_error" in msg:
        return "internal_error"
    if "daily_quota" in msg:
        return "daily_quota_limit_exceeded"
    if "authentication" in msg:
        return "authentication_failure"
    return "http_error"


def _sync_handle_api_failure(
    *,
    pipeline: PipelineSpec,
    handle: str,
    window: ResearchWindow,
    last_err: BaseException,
) -> None:
    """Write a stub row so failed handles appear in the P1/P2 BigQuery table."""
    try:
        from enrichment.bigquery_loader import (
            bigquery_configured,
            build_handle_api_failure_row,
            upsert_handle_api_failure_row,
        )

        if not bigquery_configured():
            return
        row = build_handle_api_failure_row(
            pipeline_id=pipeline.id,
            handle=handle,
            collection_date=window.research_date,
            collection_window_start=window.collection_window_start,
            collection_window_end=window.collection_window_end,
            api_source=pipeline.resolved_api_source(),
            api_error_code=_api_error_code_from_exc(last_err),
            failure_reason=str(last_err)[:500],
        )
        n = upsert_handle_api_failure_row(row)
        logger.info(
            "pipeline=%s BQ handle_fail @%s date=%s rows=%s code=%s",
            pipeline.id,
            handle,
            window.research_date,
            n,
            row.get("api_error_code"),
        )
    except Exception as e:
        logger.warning(
            "pipeline=%s could not write failed-handle row @%s: %s",
            pipeline.id,
            handle,
            e,
        )


def _abort_reason(exc: BaseException) -> Optional[str]:
    msg = str(exc)
    if "daily_quota_limit_exceeded" in msg:
        return "daily_quota_limit_exceeded"
    if "authentication_failure" in msg:
        return "authentication_failure"
    if "rate_limited" in msg or "HTTP 429" in msg:
        return "rate_limited"
    return None


def _pending_handles(
    collectable: List[str], ckpt: CheckpointStore, chunks: List[tuple]
) -> List[str]:
    pending: List[str] = []
    for handle in collectable:
        if all(ckpt.is_settled(handle, start, end) for start, end in chunks):
            continue
        pending.append(handle)
    return pending

PIPELINE_VIDEO_COLUMNS = [
    "pipeline_id",
    "collection_date",
    "collection_source",
    "api_source",
    "video_id",
    "video_url",
    "handle",
    "posted_at",
    "create_time",
    "caption",
    "hashtags",
    "like_count",
    "share_count",
    "save_count",
    "comment_count",
    "view_count",
    "duration_seconds",
    "region_code",
    "voice_to_text",
    "sticker_overlay_text",
]
NEWS_PIPELINE_VIDEO_COLUMNS = PIPELINE_VIDEO_COLUMNS + [
    "collection_window_start",
    "collection_window_end",
]


def utc_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def count_videos_in_window(
    conn, handles: List[str], window: ResearchWindow
) -> Dict[str, int]:
    if not handles:
        return {"rows": 0, "unique_video_ids": 0}
    placeholders = ",".join("?" for _ in handles)
    row = conn.execute(
        f"""SELECT COUNT(*) AS n, COUNT(DISTINCT video_id) AS u
            FROM videos
            WHERE username IN ({placeholders})
              AND create_time >= ? AND create_time < ?""",
        list(handles) + [window.start_unix, window.end_unix],
    ).fetchone()
    return {"rows": int(row["n"]), "unique_video_ids": int(row["u"])}


def export_pipeline_csv(
    conn,
    *,
    handles: List[str],
    pipeline: PipelineSpec,
    window: ResearchWindow,
    output_path: str,
) -> int:
    placeholders = ",".join("?" for _ in handles)
    rows = conn.execute(
        f"""SELECT video_id, video_url, username, posted_at, create_time, caption,
                   hashtags, like_count, share_count, save_count, comment_count,
                   view_count, duration_seconds, region_code,
                   COALESCE(voice_to_text, '') AS voice_to_text,
                   COALESCE(sticker_overlay_text, '') AS sticker_overlay_text
            FROM videos
            WHERE username IN ({placeholders})
              AND create_time >= ? AND create_time < ?
            ORDER BY username, create_time DESC""",
        list(handles) + [window.start_unix, window.end_unix],
    ).fetchall()

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    fieldnames = PIPELINE_VIDEO_COLUMNS + [
        "collection_window_start",
        "collection_window_end",
    ]
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            row = {
                "pipeline_id": pipeline.id,
                "collection_date": window.research_date,
                "collection_source": pipeline.id,
                "api_source": pipeline.resolved_api_source(),
                "video_id": r["video_id"],
                "video_url": r["video_url"],
                "handle": r["username"],
                "posted_at": r["posted_at"],
                "create_time": r["create_time"],
                "caption": r["caption"] or "",
                "hashtags": r["hashtags"] or "",
                "like_count": r["like_count"],
                "share_count": r["share_count"],
                "save_count": r["save_count"],
                "comment_count": r["comment_count"],
                "view_count": r["view_count"],
                "duration_seconds": r["duration_seconds"],
                "region_code": r["region_code"] or "",
                "voice_to_text": r["voice_to_text"] or "",
                "sticker_overlay_text": r["sticker_overlay_text"] or "",
                "collection_window_start": window.collection_window_start,
                "collection_window_end": window.collection_window_end,
            }
            writer.writerow(row)
    logger.info("pipeline=%s exported %s rows → %s", pipeline.id, len(rows), output_path)
    return len(rows)


def collect_handles(
    *,
    cfg: Config,
    pipeline: PipelineSpec,
    handles: List[str],
    handle_group: str,
    window: ResearchWindow,
    reset_checkpoints: bool,
    batch_size: Optional[int] = None,
    retry_failed: bool = False,
    continue_on_failures: bool = False,
    skip_user_info: bool = False,
) -> Dict[str, Any]:
    client_key, client_secret = pipeline.resolve_credentials(cfg)
    auth.init(cfg.base_url, client_key, client_secret)

    conn = get_connection(cfg.paths["database"])
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"], db_conn=conn)

    ckpt_path = os.path.join(
        pipeline.resolved_checkpoint_dir(cfg),
        f"{pipeline.id}_{handle_group}_{window.research_date}.json",
    )
    ckpt = CheckpointStore(ckpt_path)
    if reset_checkpoints:
        ckpt.reset()
    elif retry_failed:
        ckpt.clear_failed()

    skipped_dirty = [h for h in handles if not is_collectable_handle(h)]
    collectable = [normalize_handle(h) for h in handles if is_collectable_handle(h)]
    for h in skipped_dirty:
        logger.warning(
            "pipeline=%s skipping dirty/unusable handle %r (do not guess replacement)",
            pipeline.id,
            h,
        )

    before = count_videos_in_window(conn, collectable, window)
    chunks = window.api_query_chunks()
    logger.info(
        "pipeline=%s group=%s handles=%s research_date=%s tz=%s "
        "utc=%s..%s api=%s..%s chunks=%s",
        pipeline.id,
        handle_group,
        len(collectable),
        window.research_date,
        window.timezone_name,
        window.collection_window_start,
        window.collection_window_end,
        window.api_start_yyyymmdd,
        window.api_end_yyyymmdd,
        len(chunks),
    )

    api_rows_seen = 0
    outside_window = 0
    inserted_new = 0
    upserted_existing = 0
    duplicate_skips = 0
    api_failures = 0
    handle_coverage: Dict[str, Dict[str, int]] = {}
    seen_ids: Set[str] = set()
    collected_ids: List[str] = []
    stop_reason = ""
    consecutive_failures = 0
    handles_attempted = 0
    handles_query_ok = 0

    pending = _pending_handles(collectable, ckpt, chunks)
    if batch_size is not None and batch_size > 0:
        batch_handles = pending[: int(batch_size)]
    else:
        batch_handles = list(pending)

    provenance = {
        "collection_source": pipeline.id,
        "collection_date": window.research_date,
        "collection_window_start": window.collection_window_start,
        "collection_window_end": window.collection_window_end,
        "pipeline_id": pipeline.id,
        "api_source": pipeline.resolved_api_source(),
    }

    for i, handle in enumerate(batch_handles, 1):
        handle_api = handle_new = handle_upsert = handle_dup = handle_fail = 0
        handle_out = 0
        handles_attempted += 1
        handle_query_failed = False
        for chunk_start, chunk_end in chunks:
            if ckpt.is_done(handle, chunk_start, chunk_end):
                logger.info(
                    "pipeline=%s skip checkpoint @%s %s-%s",
                    pipeline.id,
                    handle,
                    chunk_start,
                    chunk_end,
                )
                continue
            videos = None
            last_err: Optional[BaseException] = None
            for attempt in range(1, HTTP_RETRY_ATTEMPTS + 1):
                try:
                    videos = query_videos_for_chunk(
                        client,
                        handle,
                        chunk_start,
                        chunk_end,
                        extra_fields=True,
                        raise_on_http_error=True,
                        raise_on_rate_limit=True,
                    )
                    last_err = None
                    break
                except RuntimeError as e:
                    last_err = e
                    reason = _abort_reason(e)
                    logger.error(
                        "pipeline=%s API failure @%s %s-%s attempt %s/%s: %s",
                        pipeline.id,
                        handle,
                        chunk_start,
                        chunk_end,
                        attempt,
                        HTTP_RETRY_ATTEMPTS,
                        e,
                    )
                    if reason:
                        api_failures += 1
                        handle_fail += 1
                        handle_query_failed = True
                        stop_reason = reason
                        logger.error(
                            "pipeline=%s aborting collection: %s", pipeline.id, reason
                        )
                        break
                    if attempt < HTTP_RETRY_ATTEMPTS:
                        time.sleep(HTTP_RETRY_SLEEP_SECONDS)
            if stop_reason:
                break
            if last_err is not None:
                api_failures += 1
                handle_fail += 1
                handle_query_failed = True
                ckpt.mark_failed(handle, chunk_start, chunk_end)
                _sync_handle_api_failure(
                    pipeline=pipeline,
                    handle=handle,
                    window=window,
                    last_err=last_err,
                )
                break

            if videos is None:
                videos = []

            for v in videos:
                vid = v.get("video_id") or ""
                api_rows_seen += 1
                handle_api += 1
                if not window.contains_create_time(v.get("create_time")):
                    outside_window += 1
                    handle_out += 1
                    continue
                if vid in seen_ids:
                    duplicate_skips += 1
                    handle_dup += 1
                    continue
                seen_ids.add(vid)
                row = {**v, **provenance}
                is_new = upsert_collected_video(conn, row)
                if is_new:
                    inserted_new += 1
                    handle_new += 1
                else:
                    upserted_existing += 1
                    handle_upsert += 1
                collected_ids.append(vid)
            conn.commit()
            ckpt.mark_done(handle, chunk_start, chunk_end)

        if handle_query_failed:
            consecutive_failures += 1
        else:
            consecutive_failures = 0
            handles_query_ok += 1

        handle_coverage[handle] = {
            "api_rows": handle_api,
            "inserted_new": handle_new,
            "upserted_existing": handle_upsert,
            "duplicates": handle_dup,
            "outside_window": handle_out,
            "api_failures": handle_fail,
        }
        logger.info(
            "pipeline=%s [%s/%s] @%s api_rows=%s new=%s upserts=%s dups=%s "
            "outside_window=%s failures=%s",
            pipeline.id,
            i,
            len(batch_handles),
            handle,
            handle_api,
            handle_new,
            handle_upsert,
            handle_dup,
            handle_out,
            handle_fail,
        )
        if stop_reason:
            break
        if consecutive_failures >= CONSECUTIVE_API_FAIL_LIMIT:
            if continue_on_failures:
                logger.warning(
                    "pipeline=%s consecutive_api_failures=%s "
                    "(continue_on_failures: skip and keep going)",
                    pipeline.id,
                    consecutive_failures,
                )
            else:
                stop_reason = (
                    f"consecutive_api_failures={consecutive_failures} "
                    f"(limit {CONSECUTIVE_API_FAIL_LIMIT})"
                )
                logger.error("pipeline=%s aborting: %s", pipeline.id, stop_reason)
                break
        if (
            handles_attempted >= FAIL_RATE_MIN_HANDLES
            and api_failures / handles_attempted >= FAIL_RATE_MAX
        ):
            if continue_on_failures:
                logger.warning(
                    "pipeline=%s high_api_failure_rate=%s/%s "
                    "(continue_on_failures: skip and keep going)",
                    pipeline.id,
                    api_failures,
                    handles_attempted,
                )
            else:
                stop_reason = (
                    f"high_api_failure_rate={api_failures}/{handles_attempted} "
                    f"(max {FAIL_RATE_MAX:.0%} after {FAIL_RATE_MIN_HANDLES})"
                )
                logger.error("pipeline=%s aborting: %s", pipeline.id, stop_reason)
                break

    user_ok = user_fail = 0
    if skip_user_info:
        logger.info("pipeline=%s skip_user_info (quota: video/query only)", pipeline.id)
    elif not stop_reason or stop_reason not in (
        "daily_quota_limit_exceeded",
        "authentication_failure",
    ):
        for handle in batch_handles:
            if stop_reason in ("daily_quota_limit_exceeded", "authentication_failure"):
                break
            try:
                user = get_user_info(client, handle)
            except RuntimeError as e:
                reason = _abort_reason(e)
                if reason:
                    stop_reason = reason
                    logger.error(
                        "pipeline=%s aborting user/info: %s", pipeline.id, reason
                    )
                    break
                user = {
                    "username": handle,
                    "display_name": "",
                    "bio": "",
                    "is_verified": False,
                    "follower_count": 0,
                    "following_count": 0,
                    "likes_count": 0,
                    "video_count": 0,
                    "api_failed": 1,
                }
            insert_user(conn, user)
            conn.commit()
            if user.get("api_failed"):
                user_fail += 1
            else:
                user_ok += 1

    after = count_videos_in_window(conn, collectable, window)
    remaining = _pending_handles(collectable, ckpt, chunks)
    conn.close()
    return {
        "pipeline_id": pipeline.id,
        "handle_group": handle_group,
        "handles": collectable,
        "handle_count": len(collectable),
        "batch_handles": batch_handles,
        "batch_size": len(batch_handles),
        "handles_attempted": handles_attempted,
        "handles_query_ok": handles_query_ok,
        "handles_pending_after": len(remaining),
        "more_pending": bool(remaining) and not stop_reason,
        "stop_reason": stop_reason,
        "skipped_dirty_handles": skipped_dirty,
        "research_date": window.research_date,
        "timezone": window.timezone_name,
        "collection_window_start": window.collection_window_start,
        "collection_window_end": window.collection_window_end,
        "api_start_yyyymmdd": window.api_start_yyyymmdd,
        "api_end_yyyymmdd": window.api_end_yyyymmdd,
        "api_source": pipeline.resolved_api_source(),
        "api_rows_seen": api_rows_seen,
        "outside_window": outside_window,
        "inserted_new": inserted_new,
        "upserted_existing": upserted_existing,
        "duplicate_skips": duplicate_skips,
        "api_failures": api_failures,
        "user_info_ok": user_ok,
        "user_info_failed": user_fail,
        "db_before": before,
        "db_after": after,
        "handle_coverage": handle_coverage,
        "unique_video_ids_in_api_batch": len(seen_ids),
        "collected_video_ids": collected_ids,
        "checkpoint_path": ckpt_path,
    }


def run_handle_pipeline(
    *,
    cfg: Config,
    pipeline: PipelineSpec,
    sample: bool,
    research_date: str,
    reset_checkpoints: bool,
    skip_collect: bool,
    file_prefix: str,
    batch_size: Optional[int] = None,
    retry_failed: bool = False,
    continue_on_failures: bool = False,
    utc_day: bool = False,
    skip_user_info: bool = False,
) -> Dict[str, Any]:
    handle_group = pipeline.resolve_handle_group_name(sample=sample)
    handles = pipeline.resolve_handles(cfg, sample=sample)
    tz_name = cfg.research_timezone or "America/Chicago"
    if utc_day:
        window = utc_calendar_window(research_date)
    else:
        window = research_window(research_date, timezone_name=tz_name)

    if not skip_collect:
        stats = collect_handles(
            cfg=cfg,
            pipeline=pipeline,
            handles=handles,
            handle_group=handle_group,
            window=window,
            reset_checkpoints=reset_checkpoints,
            batch_size=batch_size,
            retry_failed=retry_failed,
            continue_on_failures=continue_on_failures,
            skip_user_info=skip_user_info,
        )
    else:
        collectable = [normalize_handle(h) for h in handles if is_collectable_handle(h)]
        conn = get_connection(cfg.paths["database"])
        after = count_videos_in_window(conn, collectable, window)
        ids = [
            r["video_id"]
            for r in conn.execute(
                f"""SELECT video_id FROM videos
                    WHERE username IN ({",".join("?" for _ in collectable)})
                      AND create_time >= ? AND create_time < ?""",
                collectable + [window.start_unix, window.end_unix],
            ).fetchall()
        ] if collectable else []
        conn.close()
        stats = {
            "pipeline_id": pipeline.id,
            "handle_group": handle_group,
            "handles": collectable,
            "handle_count": len(collectable),
            "skipped_dirty_handles": [],
            "research_date": window.research_date,
            "timezone": window.timezone_name,
            "collection_window_start": window.collection_window_start,
            "collection_window_end": window.collection_window_end,
            "api_start_yyyymmdd": window.api_start_yyyymmdd,
            "api_end_yyyymmdd": window.api_end_yyyymmdd,
            "api_source": pipeline.resolved_api_source(),
            "api_rows_seen": 0,
            "outside_window": 0,
            "inserted_new": 0,
            "upserted_existing": 0,
            "duplicate_skips": 0,
            "api_failures": 0,
            "user_info_ok": 0,
            "user_info_failed": 0,
            "db_before": after,
            "db_after": after,
            "handle_coverage": {},
            "unique_video_ids_in_api_batch": after["unique_video_ids"],
            "collected_video_ids": ids,
            "skip_collect": True,
            "more_pending": False,
            "stop_reason": "",
            "batch_handles": collectable,
            "handles_attempted": 0,
            "handles_query_ok": 0,
            "handles_pending_after": 0,
        }

    ts = utc_slug()
    export_dir = pipeline.resolved_export_dir(cfg)
    summary_dir = pipeline.resolved_summary_dir(cfg)
    os.makedirs(export_dir, exist_ok=True)
    os.makedirs(summary_dir, exist_ok=True)
    csv_path = os.path.join(export_dir, f"{file_prefix}_videos_{ts}.csv")
    report_path = os.path.join(summary_dir, f"{file_prefix}_run_{ts}.json")
    ids_path = os.path.join(export_dir, f"{file_prefix}_video_ids_{ts}.txt")

    collectable = stats.get("handles") or []
    conn = get_connection(cfg.paths["database"])
    export_rows = export_pipeline_csv(
        conn,
        handles=collectable,
        pipeline=pipeline,
        window=window,
        output_path=csv_path,
    )
    collected_ids = list(stats.get("collected_video_ids") or [])
    unique_csv = len(set(collected_ids))
    with open(ids_path, "w", encoding="utf-8") as f:
        for vid in collected_ids:
            f.write(f"{vid}\n")
    conn.close()

    report = {
        **{k: v for k, v in stats.items() if k != "collected_video_ids"},
        "collected_video_ids": collected_ids,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "csv_path": csv_path,
        "ids_path": ids_path,
        "csv_row_count": export_rows,
        "csv_unique_video_ids": unique_csv,
        "csv_duplicate_rows": export_rows - unique_csv,
        "sample_mode": bool(sample),
        "report_path": report_path,
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    summary = {
        "pipeline_id": pipeline.id,
        "handle_group": handle_group,
        "handles": len(collectable),
        "research_date": window.research_date,
        "collection_window": f"{window.collection_window_start} .. {window.collection_window_end}",
        "inserted_new": stats.get("inserted_new"),
        "upserted_existing": stats.get("upserted_existing"),
        "duplicate_skips": stats.get("duplicate_skips"),
        "api_failures": stats.get("api_failures"),
        "csv_row_count": export_rows,
        "csv_unique_video_ids": unique_csv,
        "csv_path": csv_path,
        "ids_path": ids_path,
        "report_path": report_path,
    }
    print(json.dumps(summary, indent=2), flush=True)
    return report
