"""Username-based daily collection for Pipeline 1 (content creators).

News/keyword collectors must not be treated as production until approved.
v5.0 ``scripts/pull_videos.py`` is unchanged and still uses ``date_chunks``.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from tiktok import auth
from tiktok.api.client import TikTokClient
from tiktok.api.users import get_user_info
from tiktok.api.videos import date_chunks, query_videos_for_chunk
from tiktok.checkpoint import CheckpointStore
from tiktok.collection.date_window import ResearchWindow, research_window
from tiktok.config import Config
from tiktok.db import get_connection, insert_user, upsert_collected_video
from tiktok.pipelines import PipelineSpec, is_collectable_handle, normalize_handle

logger = logging.getLogger(__name__)

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
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=PIPELINE_VIDEO_COLUMNS)
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
                    "like_count": r["like_count"],
                    "share_count": r["share_count"],
                    "save_count": r["save_count"],
                    "comment_count": r["comment_count"],
                    "view_count": r["view_count"],
                    "duration_seconds": r["duration_seconds"],
                    "region_code": r["region_code"] or "",
                    "voice_to_text": r["voice_to_text"] or "",
                    "sticker_overlay_text": r["sticker_overlay_text"] or "",
                }
            )
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
) -> Dict[str, Any]:
    client_key, client_secret = pipeline.resolve_credentials(cfg)
    auth.init(cfg.base_url, client_key, client_secret)

    conn = get_connection(cfg.paths["database"])
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"], db_conn=conn)

    ckpt_path = os.path.join(
        cfg.paths["checkpoints"],
        f"{pipeline.id}_{handle_group}_{window.research_date}.json",
    )
    ckpt = CheckpointStore(ckpt_path)
    if reset_checkpoints:
        ckpt.reset()

    skipped_dirty = [h for h in handles if not is_collectable_handle(h)]
    collectable = [normalize_handle(h) for h in handles if is_collectable_handle(h)]
    for h in skipped_dirty:
        logger.warning(
            "pipeline=%s skipping dirty/unusable handle %r (do not guess replacement)",
            pipeline.id,
            h,
        )

    before = count_videos_in_window(conn, collectable, window)
    chunks = date_chunks(window.api_start_yyyymmdd, window.api_end_yyyymmdd)
    if not chunks:
        raise RuntimeError(
            f"Empty API date chunks for research date {window.research_date} "
            f"({window.api_start_yyyymmdd}..{window.api_end_yyyymmdd})"
        )
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

    provenance = {
        "collection_source": pipeline.id,
        "collection_date": window.research_date,
        "collection_window_start": window.collection_window_start,
        "collection_window_end": window.collection_window_end,
        "pipeline_id": pipeline.id,
        "api_source": pipeline.resolved_api_source(),
    }

    for i, handle in enumerate(collectable, 1):
        handle_api = handle_new = handle_upsert = handle_dup = handle_fail = 0
        handle_out = 0
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
            try:
                videos = query_videos_for_chunk(
                    client, handle, chunk_start, chunk_end, extra_fields=True
                )
            except RuntimeError as e:
                api_failures += 1
                handle_fail += 1
                logger.error(
                    "pipeline=%s API failure @%s %s-%s: %s",
                    pipeline.id,
                    handle,
                    chunk_start,
                    chunk_end,
                    e,
                )
                break

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
            len(collectable),
            handle,
            handle_api,
            handle_new,
            handle_upsert,
            handle_dup,
            handle_out,
            handle_fail,
        )

    user_ok = user_fail = 0
    for handle in collectable:
        user = get_user_info(client, handle)
        insert_user(conn, user)
        conn.commit()
        if user.get("api_failed"):
            user_fail += 1
        else:
            user_ok += 1

    after = count_videos_in_window(conn, collectable, window)
    conn.close()
    return {
        "pipeline_id": pipeline.id,
        "handle_group": handle_group,
        "handles": collectable,
        "handle_count": len(collectable),
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
) -> Dict[str, Any]:
    handle_group = pipeline.resolve_handle_group_name(sample=sample)
    handles = pipeline.resolve_handles(cfg, sample=sample)
    tz_name = cfg.research_timezone or "America/Chicago"
    window = research_window(research_date, timezone_name=tz_name)

    if not skip_collect:
        stats = collect_handles(
            cfg=cfg,
            pipeline=pipeline,
            handles=handles,
            handle_group=handle_group,
            window=window,
            reset_checkpoints=reset_checkpoints,
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
        }

    ts = utc_slug()
    export_dir = pipeline.export_dir
    os.makedirs(export_dir, exist_ok=True)
    csv_path = os.path.join(export_dir, f"{file_prefix}_videos_{ts}.csv")
    report_path = os.path.join(export_dir, f"{file_prefix}_run_{ts}.json")
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
    if not collected_ids:
        collected_ids = [
            r["video_id"]
            for r in csv.DictReader(open(csv_path, newline="", encoding="utf-8-sig"))
        ]
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
