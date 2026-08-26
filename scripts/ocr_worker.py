#!/usr/bin/env python3
"""OCR worker — Google Cloud Vision (preferred) with optional web-hydration fallback.

Usage:
    python scripts/ocr_worker.py --group batch_test --limit 3
    python scripts/ocr_worker.py --video-id ID --force
    python scripts/ocr_worker.py --group batch_test --limit 6 --allow-web-fallback

Env (production OCR):
    GOOGLE_APPLICATION_CREDENTIALS=/home/cme-user1/keys/tiktok-enrichment-worker.json
    VISION_ENABLED=true
    GCP_PROJECT=cfme-mediaengagment-prod

Temp video files are deleted after Vision OCR. Web fallback does not download video.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection, update_video_browser_ocr_text, update_video_onscreen_text
from tiktok.logging_setup import setup_logging
from tiktok.enrichment.bigquery_loader import vision_enabled
from tiktok.enrichment.ocr_google import ocr_video_file
from tiktok.enrichment.store import (
    ensure_enrichment_schema,
    fetch_videos_for_enrichment,
    insert_enrichment_log,
    replace_ocr_rows,
    touch_pipeline_status,
    upsert_ocr_stats,
)
from datetime import datetime, timezone
from tiktok.enrichment.temp_media import temporary_video
from tiktok.enrichment.worker_log import WorkerTimer
from tiktok.web.metadata import fetch_web_onscreen_text

logger = logging.getLogger(__name__)
WORKER = "ocr"


def _vision_ready() -> bool:
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    return bool(vision_enabled() and creds and os.path.isfile(creds))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ocr_via_vision(video_url: str, video_id: str, max_frames: int) -> dict:
    with temporary_video(video_url, video_id) as video_path:
        if not video_path:
            raise RuntimeError("download_failed")
        return ocr_video_file(video_path, max_frames=max_frames)


def _ocr_via_web(video_url: str) -> dict:
    web = fetch_web_onscreen_text(video_url)
    if web.get("error"):
        raise RuntimeError(web["error"])
    text = (web.get("text") or "").strip()
    rows = []
    if text:
        rows = [
            {
                "frame_number": 0,
                "frame_timestamp": 0.0,
                "ocr_text": text,
                "confidence": None,
                "source": "browser_hydration",
            }
        ]
    return {
        "rows": rows,
        "stats": {
            "number_of_frames_processed": 1 if text else 0,
            "frames_with_text": len(rows),
            "average_text_per_frame": float(len(text)) if text else 0.0,
            "ocr_confidence_avg": None,
            "ocr_language": "",
        },
    }


def process_one(conn, row: dict, max_frames: int, allow_web_fallback: bool) -> bool:
    video_id = row["video_id"]
    video_url = row.get("video_url") or ""
    with WorkerTimer(WORKER, video_id) as timer:
        touch_pipeline_status(conn, video_id, ocr_started=_now())
        conn.commit()
        if not video_url:
            timer.fail("missing_url")
            touch_pipeline_status(conn, video_id, ocr_completed=_now())
            insert_enrichment_log(conn, timer.to_result().to_dict())
            conn.commit()
            return False

        result = {"rows": [], "stats": {}}
        engine = "google_vision"
        try:
            if _vision_ready():
                result = _ocr_via_vision(video_url, video_id, max_frames)
            elif allow_web_fallback:
                logger.warning(
                    "Vision credentials missing — using web hydration fallback for %s",
                    video_id,
                )
                result = _ocr_via_web(video_url)
                engine = "browser_hydration"
            else:
                raise RuntimeError(
                    "GOOGLE_APPLICATION_CREDENTIALS missing. "
                    "Place tiktok-enrichment-worker.json on the server, or pass --allow-web-fallback."
                )
        except Exception as e:
            # Second chance: web fallback after Vision/download failure
            if allow_web_fallback and engine == "google_vision":
                try:
                    logger.warning("Vision failed (%s); trying web fallback", e)
                    result = _ocr_via_web(video_url)
                    engine = "browser_hydration"
                except Exception as e2:
                    timer.fail(f"{e}; web_fallback={e2}")
                    touch_pipeline_status(conn, video_id, ocr_completed=_now())
                    insert_enrichment_log(conn, timer.to_result().to_dict())
                    conn.commit()
                    return False
            else:
                timer.fail(str(e))
                touch_pipeline_status(conn, video_id, ocr_completed=_now())
                insert_enrichment_log(conn, timer.to_result().to_dict())
                conn.commit()
                return False

        rows = result.get("rows") or []
        stats = result.get("stats") or {}
        n = replace_ocr_rows(conn, video_id, rows)
        upsert_ocr_stats(
            conn,
            video_id,
            number_of_frames_processed=int(stats.get("number_of_frames_processed") or n),
            frames_with_text=int(stats.get("frames_with_text") or n),
            average_text_per_frame=stats.get("average_text_per_frame"),
            ocr_confidence_avg=stats.get("ocr_confidence_avg"),
            ocr_language=stats.get("ocr_language") or "",
        )
        if stats.get("duration_seconds"):
            try:
                dur = float(stats["duration_seconds"])
                if dur > 0:
                    conn.execute(
                        """UPDATE videos SET duration_seconds=?
                           WHERE video_id=? AND (duration_seconds IS NULL OR duration_seconds=0)""",
                        (int(round(dur)), video_id),
                    )
            except (TypeError, ValueError):
                pass
        combined = "\n---\n".join(r["ocr_text"] for r in rows if r.get("ocr_text"))
        if combined:
            if engine == "google_vision":
                update_video_onscreen_text(
                    conn, video_id, combined, meta={"engine": engine, "frames": n}
                )
            else:
                update_video_browser_ocr_text(conn, video_id, combined)

        touch_pipeline_status(conn, video_id, ocr_completed=_now())
        timer.success(
            frames_with_text=n,
            frames_processed=stats.get("number_of_frames_processed"),
            chars=len(combined),
            engine=engine,
            ocr_language=stats.get("ocr_language"),
        )
        insert_enrichment_log(conn, timer.to_result().to_dict())
        conn.commit()
        return True


def main() -> int:
    parser = argparse.ArgumentParser(description="OCR worker (Vision preferred)")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--group", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-frames", type=int, default=12)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--allow-web-fallback",
        action="store_true",
        help="If Vision SA key is missing, use TikTok page hydration for on-screen text",
    )
    from tiktok.collection.video_ids import add_video_id_args, resolve_video_ids

    add_video_id_args(parser)
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)

    if not _vision_ready():
        logger.warning(
            "Vision not ready (key=%s, VISION_ENABLED=%s). "
            "Use --allow-web-fallback until SA JSON is installed.",
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
            vision_enabled(),
        )

    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    handles = cfg.get_handles(args.group) if args.group else None
    video_ids = resolve_video_ids(args)
    rows = fetch_videos_for_enrichment(
        conn,
        handles=handles,
        video_ids=video_ids,
        limit=args.limit,
        need_ocr=not args.force,
    )
    logger.info("OCR candidates: %s", len(rows))

    ok = fail = 0
    for i, row in enumerate(rows, 1):
        logger.info("[%s/%s] %s", i, len(rows), row["video_id"])
        if process_one(conn, row, args.max_frames, args.allow_web_fallback):
            ok += 1
        else:
            fail += 1

    conn.close()
    logger.info("Done. ok=%s fail=%s", ok, fail)
    return 0


if __name__ == "__main__":
    sys.exit(main())
