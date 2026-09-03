"""Keyword-based daily collection for Pipeline 3 (keyword).

Reuses ``query_videos_by_keyword`` and the shared SQLite / enrichment path.
Does not fork the API client, Whisper, OCR, or emoji workers.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set

from tiktok import auth
from api.client import TikTokClient
from api.videos import (
    fetch_keyword_query_page,
    format_video,
    query_videos_by_keyword,
)
from tiktok.checkpoint import CheckpointStore
from tiktok.collection.date_window import (
    ResearchWindow,
    research_window,
    utc_calendar_window,
)
from tiktok.collection.daily_handle_pipeline import (
    HTTP_RETRY_ATTEMPTS,
    HTTP_RETRY_SLEEP_SECONDS,
    utc_slug,
)
from tiktok.config import Config
from tiktok.db import get_connection, parse_matched_keywords, upsert_collected_video
from tiktok.pipelines import (
    PIPELINE_KEYWORD,
    PipelineSpec,
    normalize_handle,
    require_keyword_search_credentials,
)

logger = logging.getLogger(__name__)

CONSECUTIVE_API_FAIL_LIMIT = 8
FAIL_RATE_MIN_KEYWORDS = 20
FAIL_RATE_MAX = 0.25

KEYWORD_PIPELINE_VIDEO_COLUMNS = [
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
    "matched_keywords",
    "like_count",
    "share_count",
    "save_count",
    "comment_count",
    "view_count",
    "duration_seconds",
    "region_code",
    "voice_to_text",
    "sticker_overlay_text",
    "collection_window_start",
    "collection_window_end",
]


def abort_reason(exc: BaseException) -> Optional[str]:
    msg = str(exc)
    if "daily_quota_limit_exceeded" in msg:
        return "daily_quota_limit_exceeded"
    if "authentication_failure" in msg:
        return "authentication_failure"
    if "rate_limited" in msg or "HTTP 429" in msg:
        return "rate_limited"
    return None


def _coerce_keyword_videos(raw_videos: List[dict]) -> List[dict]:
    """Accept raw Research API dicts or already-formatted rows."""
    out: List[dict] = []
    for item in raw_videos or []:
        if not isinstance(item, dict):
            continue
        if item.get("video_id") and "id" not in item:
            out.append(item)
        else:
            out.append(format_video(item))
    return out


def _page_error(page: Dict[str, Any], keyword: str, chunk_start: str, chunk_end: str) -> RuntimeError:
    code = page.get("error_code") or ""
    status = page.get("http_status")
    msg = (
        page.get("error_message")
        or f"video/query HTTP error for keyword={keyword!r} {chunk_start}-{chunk_end}"
    )
    if code:
        return RuntimeError(f"{code} HTTP {status}: {msg}")
    return RuntimeError(msg)


def query_keyword_chunk_with_retries(
    client: TikTokClient,
    keyword: str,
    chunk_start: str,
    chunk_end: str,
    *,
    max_videos: Optional[int] = None,
    retry_attempts: int = HTTP_RETRY_ATTEMPTS,
    sleep_seconds: float = HTTP_RETRY_SLEEP_SECONDS,
    query_fn=fetch_keyword_query_page,
    resume: Optional[Dict[str, Any]] = None,
    on_page: Optional[Callable[[List[dict], Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """Query one keyword/date chunk, persisting each successful page.

    HTTP 500 is retried for the *current page* only. Pages already handed to
    ``on_page`` are kept. After retries, a keyword with saved pages is
    ``partial`` (not completed). Zero saved pages stays ``failed``.

    429/quota/auth abort immediately. If pages were already persisted, the
    result is also marked partial so resume can continue from the failing
    cursor.
    """
    if max_videos is not None and max_videos <= 0:
        return {
            "videos": [],
            "abort": None,
            "failed": False,
            "partial": False,
            "attempts": 0,
            "error": None,
            "cursor": 0,
            "search_id": None,
            "page": 0,
        }

    cursor = int((resume or {}).get("cursor") or 0)
    search_id = (resume or {}).get("search_id") or None
    pages_ok = int((resume or {}).get("page") or 0)
    all_videos: List[dict] = []
    attempts = 0
    request_cursor = cursor
    request_search_id = search_id

    while True:
        if max_videos is not None and len(all_videos) >= max_videos:
            break
        last_err: Optional[BaseException] = None
        page: Optional[Dict[str, Any]] = None
        request_cursor = cursor
        request_search_id = search_id
        for attempt in range(1, retry_attempts + 1):
            attempts = attempt
            try:
                raw = query_fn(
                    client,
                    keyword,
                    chunk_start,
                    chunk_end,
                    cursor=cursor,
                    search_id=search_id,
                    extra_fields=True,
                    raise_on_rate_limit=True,
                )
            except TypeError:
                raw = query_fn(
                    client,
                    keyword,
                    chunk_start,
                    chunk_end,
                    max_videos=max_videos,
                    extra_fields=True,
                    raise_on_http_error=True,
                    raise_on_rate_limit=True,
                )
            except RuntimeError as e:
                last_err = e
                reason = abort_reason(e)
                logger.error(
                    "pipeline=keyword API failure keyword=%r %s-%s "
                    "cursor=%s attempt %s/%s: %s",
                    keyword,
                    chunk_start,
                    chunk_end,
                    cursor,
                    attempt,
                    retry_attempts,
                    e,
                )
                if reason:
                    return {
                        "videos": all_videos,
                        "abort": reason,
                        "failed": False,
                        "partial": bool(all_videos or pages_ok),
                        "attempts": attempts,
                        "error": e,
                        "cursor": request_cursor,
                        "search_id": request_search_id,
                        "page": pages_ok,
                        "http_status": None,
                        "error_code": reason,
                        "error_message": str(e),
                        "log_id": None,
                    }
                if attempt < retry_attempts:
                    time.sleep(sleep_seconds)
                continue

            if isinstance(raw, list):
                videos = _coerce_keyword_videos(raw)
                if on_page and videos:
                    on_page(
                        videos,
                        {"page": pages_ok + 1, "cursor": cursor, "search_id": search_id},
                    )
                return {
                    "videos": videos,
                    "abort": None,
                    "failed": False,
                    "partial": False,
                    "attempts": attempts,
                    "error": None,
                    "cursor": cursor,
                    "search_id": search_id,
                    "page": pages_ok + (1 if videos else 0),
                }

            page = dict(raw or {})
            if page.get("abort"):
                reason = str(page.get("abort"))
                return {
                    "videos": all_videos,
                    "abort": reason,
                    "failed": False,
                    "partial": bool(all_videos or pages_ok),
                    "attempts": attempts,
                    "error": RuntimeError(reason),
                    "cursor": request_cursor,
                    "search_id": request_search_id,
                    "page": pages_ok,
                    "http_status": page.get("http_status"),
                    "error_code": page.get("error_code") or reason,
                    "error_message": page.get("error_message") or reason,
                    "log_id": page.get("log_id"),
                }
            if page.get("ok"):
                last_err = None
                break
            last_err = _page_error(page, keyword, chunk_start, chunk_end)
            reason = abort_reason(last_err)
            logger.error(
                "pipeline=keyword API failure keyword=%r %s-%s "
                "cursor=%s attempt %s/%s: %s",
                keyword,
                chunk_start,
                chunk_end,
                cursor,
                attempt,
                retry_attempts,
                last_err,
            )
            if reason:
                return {
                    "videos": all_videos,
                    "abort": reason,
                    "failed": False,
                    "partial": bool(all_videos or pages_ok),
                    "attempts": attempts,
                    "error": last_err,
                    "cursor": request_cursor,
                    "search_id": request_search_id,
                    "page": pages_ok,
                    "http_status": page.get("http_status"),
                    "error_code": page.get("error_code") or reason,
                    "error_message": page.get("error_message") or str(last_err),
                    "log_id": page.get("log_id"),
                }
            if attempt < retry_attempts:
                time.sleep(sleep_seconds)

        if last_err is not None or not page or not page.get("ok"):
            err_meta = {
                "cursor": request_cursor,
                "search_id": request_search_id,
                "page": pages_ok,
                "http_status": (page or {}).get("http_status"),
                "error_code": (page or {}).get("error_code") or "internal_error",
                "error_message": str(last_err) if last_err else "HTTP error",
                "log_id": (page or {}).get("log_id"),
            }
            if all_videos or pages_ok:
                return {
                    "videos": all_videos,
                    "abort": None,
                    "failed": False,
                    "partial": True,
                    "attempts": attempts,
                    "error": last_err,
                    **err_meta,
                }
            return {
                "videos": [],
                "abort": None,
                "failed": True,
                "partial": False,
                "attempts": attempts,
                "error": last_err,
                **err_meta,
            }

        batch = _coerce_keyword_videos(list(page.get("videos") or []))
        if max_videos is not None:
            space = max_videos - len(all_videos)
            if space <= 0:
                break
            if len(batch) > space:
                batch = batch[:space]
        pages_ok += 1
        page_meta = {
            "page": pages_ok,
            "cursor": request_cursor,
            "search_id": request_search_id,
            "next_cursor": page.get("cursor"),
            "next_search_id": page.get("search_id"),
        }
        if on_page:
            on_page(batch, page_meta)
        all_videos.extend(batch)

        if max_videos is not None and len(all_videos) >= max_videos:
            break
        if not page.get("has_more"):
            break
        cursor = page.get("cursor") if page.get("cursor") is not None else cursor
        search_id = page.get("search_id") or search_id

    return {
        "videos": all_videos,
        "abort": None,
        "failed": False,
        "partial": False,
        "attempts": attempts,
        "error": None,
        "cursor": cursor,
        "search_id": search_id,
        "page": pages_ok,
    }


def _partial_meta(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "cursor": result.get("cursor"),
        "search_id": result.get("search_id"),
        "page": result.get("page") or 0,
        "http_status": result.get("http_status"),
        "error_code": result.get("error_code"),
        "error_message": result.get("error_message") or (
            str(result.get("error")) if result.get("error") else None
        ),
        "log_id": result.get("log_id"),
    }


def apply_chunk_checkpoint(
    ckpt: CheckpointStore,
    keyword: str,
    chunk_start: str,
    chunk_end: str,
    result: Dict[str, Any],
) -> None:
    """completed / partial / failed. 429 with no pages leaves the key unset."""
    if result.get("abort"):
        if result.get("partial") or result.get("videos"):
            ckpt.mark_partial(
                keyword, chunk_start, chunk_end, meta=_partial_meta(result)
            )
        return
    if result.get("partial"):
        ckpt.mark_partial(keyword, chunk_start, chunk_end, meta=_partial_meta(result))
        return
    if result.get("failed"):
        ckpt.mark_failed(keyword, chunk_start, chunk_end)
        return
    ckpt.mark_done(keyword, chunk_start, chunk_end)


def _pending_keywords(
    keywords: List[str], ckpt: CheckpointStore, chunks: List[tuple]
) -> List[str]:
    pending: List[str] = []
    for term in keywords:
        if all(ckpt.is_settled(term, start, end) for start, end in chunks):
            continue
        pending.append(term)
    return pending


def _classify_exclusion(
    username: str, p1: Set[str], p2: Set[str]
) -> Optional[str]:
    """Return 'pipeline_1', 'pipeline_2', 'overlap', or None.

    Overlap means the username is on both Pipeline 1 and Pipeline 2 lists.
    Any non-None result must be excluded before SQLite/enrichment/BQ.
    """
    n = normalize_handle(username)
    in_p1 = n in p1
    in_p2 = n in p2
    if in_p1 and in_p2:
        return "overlap"
    if in_p1:
        return "pipeline_1"
    if in_p2:
        return "pipeline_2"
    return None


def export_keyword_csv(
    conn,
    *,
    video_ids: List[str],
    pipeline: PipelineSpec,
    window: ResearchWindow,
    output_path: str,
) -> int:
    if not video_ids:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=KEYWORD_PIPELINE_VIDEO_COLUMNS).writeheader()
        return 0
    placeholders = ",".join("?" for _ in video_ids)
    rows = conn.execute(
        f"""SELECT video_id, video_url, username, posted_at, create_time, caption,
                   hashtags, like_count, share_count, save_count, comment_count,
                   view_count, duration_seconds, region_code,
                   COALESCE(voice_to_text, '') AS voice_to_text,
                   COALESCE(sticker_overlay_text, '') AS sticker_overlay_text,
                   COALESCE(matched_keywords, '') AS matched_keywords
            FROM videos
            WHERE video_id IN ({placeholders})
            ORDER BY create_time DESC""",
        list(video_ids),
    ).fetchall()
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=KEYWORD_PIPELINE_VIDEO_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow(
                {
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
                    "matched_keywords": r["matched_keywords"] or "",
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
            )
    logger.info("pipeline=%s exported %s rows → %s", pipeline.id, len(rows), output_path)
    return len(rows)


def collect_keywords(
    *,
    cfg: Config,
    pipeline: PipelineSpec,
    keywords: List[str],
    window: ResearchWindow,
    reset_checkpoints: bool,
    retry_failed: bool = False,
    max_videos: Optional[int] = None,
    query_fn=fetch_keyword_query_page,
    sleep_seconds: float = HTTP_RETRY_SLEEP_SECONDS,
) -> Dict[str, Any]:
    if pipeline.id != PIPELINE_KEYWORD:
        raise ValueError(
            f"collect_keywords requires pipeline_id={PIPELINE_KEYWORD}, "
            f"got {pipeline.id}"
        )
    client_key, client_secret = require_keyword_search_credentials(pipeline)
    # Shared OAuth helper; credentials are Pipeline 3's dedicated pair only.
    auth.init(cfg.base_url, client_key, client_secret)

    conn = get_connection(cfg.paths["database"])
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"], db_conn=conn)

    ckpt_path = os.path.join(
        pipeline.resolved_checkpoint_dir(cfg),
        f"{pipeline.id}_{window.research_date}.json",
    )
    ckpt = CheckpointStore(ckpt_path)
    if reset_checkpoints:
        ckpt.reset()
    elif retry_failed:
        ckpt.clear_failed()

    exclusion_sets = pipeline.exclusion_handle_sets(cfg)
    p1_handles = set(exclusion_sets.get("pipeline_1") or [])
    p2_handles = set(exclusion_sets.get("pipeline_2") or [])
    if not p1_handles and not p2_handles:
        # Files must be the source of truth; do not silently fall back to YAML groups.
        raise RuntimeError(
            "Pipeline 3 exclusion handle files were empty. "
            "Expected p1_content_creators/config/newsfluencer_combined.txt and "
            "p2_news/config/news_accounts.txt."
        )

    chunks = window.api_query_chunks()
    logger.info(
        "pipeline=%s keywords=%s research_date=%s tz=%s utc=%s..%s api=%s..%s "
        "chunks=%s exclude_p1=%s exclude_p2=%s max_videos=%s",
        pipeline.id,
        len(keywords),
        window.research_date,
        window.timezone_name,
        window.collection_window_start,
        window.collection_window_end,
        window.api_start_yyyymmdd,
        window.api_end_yyyymmdd,
        len(chunks),
        len(p1_handles),
        len(p2_handles),
        max_videos,
    )

    api_rows_seen = 0
    outside_window = 0
    inserted_new = 0
    upserted_existing = 0
    duplicate_skips = 0
    api_failures = 0
    keywords_partial = 0
    excluded_p1 = 0
    excluded_p2 = 0
    excluded_overlap = 0
    keyword_coverage: Dict[str, Dict[str, int]] = {}
    seen_ids: Set[str] = set()
    collected_ids: List[str] = []
    stop_reason = ""
    consecutive_failures = 0
    keywords_attempted = 0
    keywords_query_ok = 0

    pending = _pending_keywords(keywords, ckpt, chunks)

    provenance = {
        "collection_source": pipeline.id,
        "collection_date": window.research_date,
        "collection_window_start": window.collection_window_start,
        "collection_window_end": window.collection_window_end,
        "pipeline_id": pipeline.id,
        "api_source": pipeline.resolved_api_source(),
    }

    for i, keyword in enumerate(pending, 1):
        kw_api = kw_new = kw_upsert = kw_dup = kw_fail = kw_out = 0
        kw_ex1 = kw_ex2 = 0
        keywords_attempted += 1
        keyword_query_failed = False
        keyword_partial = False
        for chunk_start, chunk_end in chunks:
            if ckpt.is_done(keyword, chunk_start, chunk_end):
                logger.info(
                    "pipeline=%s skip checkpoint keyword=%r %s-%s",
                    pipeline.id,
                    keyword,
                    chunk_start,
                    chunk_end,
                )
                continue
            resume = ckpt.get_partial(keyword, chunk_start, chunk_end)

            def _persist_page(videos: List[dict], meta: Dict[str, Any]) -> None:
                nonlocal api_rows_seen, outside_window, inserted_new
                nonlocal upserted_existing, duplicate_skips
                nonlocal excluded_p1, excluded_p2, excluded_overlap
                nonlocal kw_api, kw_new, kw_upsert, kw_dup, kw_out, kw_ex1, kw_ex2
                for v in videos:
                    vid = v.get("video_id") or ""
                    api_rows_seen += 1
                    kw_api += 1
                    if not window.contains_create_time(v.get("create_time")):
                        outside_window += 1
                        kw_out += 1
                        continue
                    bucket = _classify_exclusion(
                        v.get("username") or "", p1_handles, p2_handles
                    )
                    if bucket:
                        if bucket in ("pipeline_1", "overlap"):
                            excluded_p1 += 1
                            kw_ex1 += 1
                        if bucket in ("pipeline_2", "overlap"):
                            excluded_p2 += 1
                            kw_ex2 += 1
                        if bucket == "overlap":
                            excluded_overlap += 1
                        continue
                    if not vid:
                        continue
                    if vid in seen_ids:
                        duplicate_skips += 1
                        kw_dup += 1
                    seen_ids.add(vid)
                    row = {**v, **provenance, "matched_keyword": keyword}
                    is_new = upsert_collected_video(conn, row)
                    if vid not in collected_ids:
                        collected_ids.append(vid)
                    if is_new:
                        inserted_new += 1
                        kw_new += 1
                    else:
                        upserted_existing += 1
                        kw_upsert += 1
                conn.commit()

            result = query_keyword_chunk_with_retries(
                client,
                keyword,
                chunk_start,
                chunk_end,
                max_videos=max_videos,
                sleep_seconds=sleep_seconds,
                query_fn=query_fn,
                resume=resume,
                on_page=_persist_page,
            )
            if result.get("abort"):
                api_failures += 1
                kw_fail += 1
                keyword_query_failed = True
                if result.get("partial") or result.get("videos"):
                    keyword_partial = True
                stop_reason = result["abort"]
                apply_chunk_checkpoint(ckpt, keyword, chunk_start, chunk_end, result)
                logger.error(
                    "pipeline=%s aborting collection: %s", pipeline.id, stop_reason
                )
                break
            apply_chunk_checkpoint(ckpt, keyword, chunk_start, chunk_end, result)
            if result.get("partial"):
                kw_fail += 1
                keyword_partial = True
                break
            if result.get("failed"):
                api_failures += 1
                kw_fail += 1
                keyword_query_failed = True
                break

        if keyword_partial:
            consecutive_failures = 0
            keywords_partial += 1
        elif keyword_query_failed:
            consecutive_failures += 1
        else:
            consecutive_failures = 0
            keywords_query_ok += 1

        keyword_coverage[keyword] = {
            "api_rows": kw_api,
            "inserted_new": kw_new,
            "upserted_existing": kw_upsert,
            "duplicates": kw_dup,
            "outside_window": kw_out,
            "excluded_pipeline_1": kw_ex1,
            "excluded_pipeline_2": kw_ex2,
            "api_failures": kw_fail,
        }
        logger.info(
            "pipeline=%s [%s/%s] keyword=%r api_rows=%s new=%s upserts=%s "
            "excluded_p1=%s excluded_p2=%s failures=%s",
            pipeline.id,
            i,
            len(pending),
            keyword,
            kw_api,
            kw_new,
            kw_upsert,
            kw_ex1,
            kw_ex2,
            kw_fail,
        )
        if stop_reason:
            break
        if consecutive_failures >= CONSECUTIVE_API_FAIL_LIMIT:
            stop_reason = (
                f"consecutive_api_failures={consecutive_failures} "
                f"(limit {CONSECUTIVE_API_FAIL_LIMIT})"
            )
            logger.error("pipeline=%s aborting: %s", pipeline.id, stop_reason)
            break
        if (
            keywords_attempted >= FAIL_RATE_MIN_KEYWORDS
            and api_failures / keywords_attempted >= FAIL_RATE_MAX
        ):
            stop_reason = (
                f"high_api_failure_rate={api_failures}/{keywords_attempted} "
                f"(max {FAIL_RATE_MAX:.0%} after {FAIL_RATE_MIN_KEYWORDS})"
            )
            logger.error("pipeline=%s aborting: %s", pipeline.id, stop_reason)
            break

    remaining = _pending_keywords(keywords, ckpt, chunks)
    conn.close()
    return {
        "pipeline_id": pipeline.id,
        "keywords": keywords,
        "keyword_count": len(keywords),
        "keywords_attempted": keywords_attempted,
        "keywords_query_ok": keywords_query_ok,
        "keywords_partial": keywords_partial,
        "keywords_pending_after": len(remaining),
        "more_pending": bool(remaining) and not stop_reason,
        "stop_reason": stop_reason,
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
        "excluded_pipeline_1": excluded_p1,
        "excluded_pipeline_2": excluded_p2,
        "excluded_overlap": excluded_overlap,
        "exclusion_p1_count": len(p1_handles),
        "exclusion_p2_count": len(p2_handles),
        "exclusion_overlap_handles": len(p1_handles & p2_handles),
        "api_failures": api_failures,
        "keyword_coverage": keyword_coverage,
        "unique_video_ids_in_api_batch": len(seen_ids),
        "collected_video_ids": collected_ids,
        "checkpoint_path": ckpt_path,
        "max_videos_per_keyword": max_videos,
    }


def run_keyword_pipeline(
    *,
    cfg: Config,
    pipeline: PipelineSpec,
    sample: bool,
    research_date: str,
    reset_checkpoints: bool,
    skip_collect: bool,
    file_prefix: str = "keyword",
    retry_failed: bool = False,
    keywords_file: Optional[str] = None,
    limit_keywords: Optional[int] = None,
    query_fn=fetch_keyword_query_page,
    sleep_seconds: float = HTTP_RETRY_SLEEP_SECONDS,
    utc_day: bool = False,
    max_videos_per_keyword: Optional[int] = None,
) -> Dict[str, Any]:
    if pipeline.id != PIPELINE_KEYWORD:
        raise ValueError(f"run_keyword_pipeline requires keyword, got {pipeline.id}")
    keywords = pipeline.resolve_keywords(
        cfg,
        sample=sample,
        keywords_file=keywords_file,
        limit_keywords=limit_keywords,
    )
    tz_name = cfg.research_timezone or "America/Chicago"
    if utc_day:
        window = utc_calendar_window(research_date)
    else:
        window = research_window(research_date, timezone_name=tz_name)
    if max_videos_per_keyword is not None:
        if int(max_videos_per_keyword) < 0:
            raise ValueError("max_videos_per_keyword must be >= 0")
        max_videos = (
            int(max_videos_per_keyword) if int(max_videos_per_keyword) > 0 else None
        )
    else:
        max_videos = pipeline.effective_max_videos()

    if not skip_collect:
        stats = collect_keywords(
            cfg=cfg,
            pipeline=pipeline,
            keywords=keywords,
            window=window,
            reset_checkpoints=reset_checkpoints,
            retry_failed=retry_failed,
            max_videos=max_videos,
            query_fn=query_fn,
            sleep_seconds=sleep_seconds,
        )
    else:
        stats = {
            "pipeline_id": pipeline.id,
            "keywords": keywords,
            "keyword_count": len(keywords),
            "keywords_attempted": 0,
            "keywords_query_ok": 0,
            "keywords_partial": 0,
            "keywords_pending_after": 0,
            "more_pending": False,
            "stop_reason": "",
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
            "excluded_pipeline_1": 0,
            "excluded_pipeline_2": 0,
            "excluded_overlap": 0,
            "api_failures": 0,
            "keyword_coverage": {},
            "unique_video_ids_in_api_batch": 0,
            "collected_video_ids": [],
            "skip_collect": True,
            "max_videos_per_keyword": max_videos,
        }

    ts = utc_slug()
    export_dir = pipeline.resolved_export_dir(cfg)
    summary_dir = pipeline.resolved_summary_dir(cfg)
    os.makedirs(export_dir, exist_ok=True)
    os.makedirs(summary_dir, exist_ok=True)
    csv_path = os.path.join(export_dir, f"{file_prefix}_videos_{ts}.csv")
    report_path = os.path.join(summary_dir, f"{file_prefix}_run_{ts}.json")
    ids_path = os.path.join(export_dir, f"{file_prefix}_video_ids_{ts}.txt")

    collected_ids = list(stats.get("collected_video_ids") or [])
    conn = get_connection(cfg.paths["database"])
    export_rows = export_keyword_csv(
        conn,
        video_ids=collected_ids,
        pipeline=pipeline,
        window=window,
        output_path=csv_path,
    )
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
        "csv_unique_video_ids": len(set(collected_ids)),
        "sample_mode": bool(sample),
        "report_path": report_path,
        "matched_keywords_sample": {
            vid: None for vid in collected_ids[:5]
        },
    }
    if collected_ids:
        conn = get_connection(cfg.paths["database"])
        sample_map = {}
        for vid in collected_ids[:5]:
            row = conn.execute(
                "SELECT matched_keywords FROM videos WHERE video_id=?", (vid,)
            ).fetchone()
            sample_map[vid] = parse_matched_keywords(row["matched_keywords"] if row else "")
        conn.close()
        report["matched_keywords_sample"] = sample_map
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    summary = {
        "pipeline_id": pipeline.id,
        "keywords": len(keywords),
        "sample_mode": bool(sample),
        "research_date": window.research_date,
        "collection_window": f"{window.collection_window_start} .. {window.collection_window_end}",
        "inserted_new": stats.get("inserted_new"),
        "upserted_existing": stats.get("upserted_existing"),
        "excluded_pipeline_1": stats.get("excluded_pipeline_1"),
        "excluded_pipeline_2": stats.get("excluded_pipeline_2"),
        "api_failures": stats.get("api_failures"),
        "csv_path": csv_path,
        "ids_path": ids_path,
        "report_path": report_path,
        "stop_reason": stats.get("stop_reason") or "",
        "max_videos_per_keyword": max_videos,
        "utc_day": bool(utc_day),
        "api_start_yyyymmdd": window.api_start_yyyymmdd,
        "api_end_yyyymmdd": window.api_end_yyyymmdd,
        "keyword_terms": keywords,
    }
    print(json.dumps(summary, indent=2), flush=True)
    return report
