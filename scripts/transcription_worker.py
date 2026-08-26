#!/usr/bin/env python3
"""Transcription worker — temporary download → ffmpeg convert → Whisper → delete.

Usage:
    python scripts/transcription_worker.py --group batch_test --limit 5
    python scripts/transcription_worker.py --video-id ID --force

Env:
    WHISPER_BACKEND=faster-whisper|openai
    WHISPER_MODEL=base (for faster-whisper)
    OPENAI_API_KEY (for openai backend)

Does not permanently store audio. Failures are logged; batch continues.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import build_text_for_nlp, get_connection
from tiktok.logging_setup import setup_logging
from datetime import datetime, timezone

from tiktok.enrichment.store import (
    ensure_enrichment_schema,
    fetch_videos_for_enrichment,
    insert_enrichment_log,
    touch_pipeline_status,
    upsert_transcript,
)
from tiktok.enrichment.temp_media import temporary_audio
from tiktok.enrichment.whisper_backend import transcribe_audio
from tiktok.enrichment.worker_log import WorkerTimer

logger = logging.getLogger(__name__)
WORKER = "transcription"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def process_one(conn, row: dict) -> bool:
    video_id = row["video_id"]
    video_url = row.get("video_url") or ""
    with WorkerTimer(WORKER, video_id) as timer:
        touch_pipeline_status(conn, video_id, transcription_started=_now())
        conn.commit()
        if not video_url:
            timer.fail("missing_url")
            touch_pipeline_status(conn, video_id, transcription_completed=_now())
            insert_enrichment_log(conn, timer.to_result().to_dict())
            conn.commit()
            return False

        with temporary_audio(video_url, video_id) as audio_path:
            if not audio_path:
                timer.fail("download_failed")
                upsert_transcript(
                    conn, video_id=video_id, transcript="", status="error", error="download_failed"
                )
                touch_pipeline_status(conn, video_id, transcription_completed=_now())
                insert_enrichment_log(conn, timer.to_result().to_dict())
                conn.commit()
                return False
            try:
                result = transcribe_audio(video_id, audio_path)
            except Exception as e:
                timer.fail(str(e))
                upsert_transcript(
                    conn,
                    video_id=video_id,
                    transcript="",
                    status="error",
                    error=str(e)[:500],
                )
                touch_pipeline_status(conn, video_id, transcription_completed=_now())
                insert_enrichment_log(conn, timer.to_result().to_dict())
                conn.commit()
                return False

        upsert_transcript(
            conn,
            video_id=video_id,
            transcript=result.transcript,
            language=result.language,
            whisper_model=result.whisper_model,
            confidence=result.confidence,
            status="ok",
            audio_duration_seconds=result.duration_seconds,
            original_audio_format=result.original_audio_format,
            converted_audio_format=result.converted_audio_format,
        )
        # Persist duration for cost estimates + analytics
        if result.duration_seconds and result.duration_seconds > 0:
            conn.execute(
                """UPDATE videos SET duration_seconds=?
                   WHERE video_id=? AND (duration_seconds IS NULL OR duration_seconds=0)""",
                (int(round(result.duration_seconds)), video_id),
            )
        caption = row.get("caption") or ""
        if result.transcript and not (row.get("voice_to_text") or "").strip():
            conn.execute(
                """UPDATE videos SET transcript=?, transcript_source='asr', text_for_nlp=?
                   WHERE video_id=?""",
                (
                    result.transcript,
                    build_text_for_nlp(caption, result.transcript),
                    video_id,
                ),
            )

        touch_pipeline_status(conn, video_id, transcription_completed=_now())
        timer.success(
            chars=len(result.transcript or ""),
            model=result.whisper_model,
            language=result.language,
            duration_seconds=result.duration_seconds,
            original_audio_format=result.original_audio_format,
            converted_audio_format=result.converted_audio_format,
        )
        insert_enrichment_log(conn, timer.to_result().to_dict())
        conn.commit()
        return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Whisper transcription worker (temp media)")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--group", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    from tiktok.collection.video_ids import add_video_id_args, resolve_video_ids

    add_video_id_args(parser)
    args = parser.parse_args()

    setup_logging()
    home_bin = os.path.join(os.path.expanduser("~"), "bin")
    if os.path.isdir(home_bin):
        path = os.environ.get("PATH", "")
        if home_bin not in path.split(os.pathsep):
            os.environ["PATH"] = home_bin + os.pathsep + path
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)

    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    handles = cfg.get_handles(args.group) if args.group else None
    video_ids = resolve_video_ids(args)
    rows = fetch_videos_for_enrichment(
        conn,
        handles=handles,
        video_ids=video_ids,
        limit=args.limit,
        need_transcript=not args.force,
    )
    logger.info("Transcription candidates: %s", len(rows))

    ok = fail = 0
    for i, row in enumerate(rows, 1):
        logger.info("[%s/%s] %s", i, len(rows), row["video_id"])
        if process_one(conn, row):
            ok += 1
        else:
            fail += 1

    conn.close()
    logger.info("Done. ok=%s fail=%s", ok, fail)
    return 0


if __name__ == "__main__":
    sys.exit(main())
