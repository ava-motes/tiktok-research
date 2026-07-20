"""Download audio in parallel, transcribe with OpenAI Whisper API, then delete audio.

Usage:
    python scripts/download_and_transcribe.py --start-date 2026-02-22 --end-date 2026-02-28
    python scripts/download_and_transcribe.py --group complete --workers 5
"""

import sys
import os
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.logging_setup import setup_logging
from tiktok.db import get_connection, build_text_for_nlp
from tiktok.api.download import download_audio
from tiktok.transcription.service import WhisperAPITranscriptionService
from tiktok.transcription.vad import has_enough_speech

logger = logging.getLogger(__name__)


def process_video(v, audio_dir, svc, db_path, idx, total, min_speech_ratio=0.10, vad_only=False):
    """Download, VAD-check, optionally transcribe, then delete audio."""
    video_id = v["video_id"]
    video_url = v["video_url"]
    username = v["username"]

    # Download
    audio_path = download_audio(video_url, video_id, audio_dir)
    if not audio_path:
        logger.warning(f"[{idx}/{total}] @{username} — {video_id} — download failed")
        return "failed"

    # VAD: check speech content
    speech_ok = has_enough_speech(audio_path, min_ratio=min_speech_ratio)

    if not speech_ok:
        try:
            os.remove(audio_path)
        except OSError:
            pass
        conn = get_connection(db_path)
        conn.execute(
            "UPDATE videos SET transcript_failure_reason='no_speech' WHERE video_id=?",
            (video_id,),
        )
        conn.commit()
        conn.close()
        logger.info(f"[{idx}/{total}] @{username} — {video_id} — no_speech")
        return "skipped"

    if vad_only:
        try:
            os.remove(audio_path)
        except OSError:
            pass
        logger.info(f"[{idx}/{total}] @{username} — {video_id} — speech detected")
        return "ok"

    # Transcribe
    result = svc.transcribe(video_id, audio_path)

    # Delete audio regardless of transcription outcome
    try:
        os.remove(audio_path)
    except OSError:
        pass

    if result is None:
        logger.warning(f"[{idx}/{total}] @{username} — {video_id} — transcription failed")
        return "failed"

    text_for_nlp = build_text_for_nlp(v["caption"], result.text)

    # Each thread opens its own connection (SQLite connections are not thread-safe)
    conn = get_connection(db_path)
    conn.execute(
        """INSERT OR REPLACE INTO transcripts
        (video_id, transcript_text, language, transcript_source, model_name,
         audio_path, duration_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (result.video_id, result.text, result.language, result.source,
         result.model_name, result.audio_path, result.duration_seconds),
    )
    conn.execute(
        """UPDATE videos SET transcript=?, transcript_source=?, text_for_nlp=?
        WHERE video_id=?""",
        (result.text, result.source, text_for_nlp, result.video_id),
    )
    conn.commit()
    conn.close()

    logger.info(f"[{idx}/{total}] @{username} — {video_id} — transcribed and audio deleted")
    return "ok"


def main():
    parser = argparse.ArgumentParser(description="Parallel download + transcribe + delete audio")
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--start-date", default=None, help="Only videos posted on or after (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="Only videos posted on or before (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel download workers (default: 4)")
    parser.add_argument("--min-speech", type=float, default=0.10,
                        help="Minimum speech ratio to transcribe (0.0-1.0, default: 0.10)")
    parser.add_argument("--vad-only", action="store_true",
                        help="Only run VAD (download + check + delete audio), do not transcribe")
    parser.add_argument("--max-recent", type=int, default=None,
                        help="Only transcribe the N most recent videos per handle")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    audio_dir = cfg.transcription["audio_dir"]

    db_path = cfg.paths["database"]
    svc = WhisperAPITranscriptionService()

    # Build query for videos missing voice_to_text and transcript
    conditions = [
        "(voice_to_text IS NULL OR length(voice_to_text) = 0)",
        "(transcript_source IS NULL OR transcript_source != 'asr')",
        "(transcript_failure_reason IS NULL OR transcript_failure_reason != 'no_speech')",
    ]
    params = []

    if args.group:
        handles = cfg.get_handles(args.group)
        placeholders = ",".join("?" for _ in handles)
        conditions.append(f"username IN ({placeholders})")
        params.extend(handles)

    if args.start_date:
        conditions.append("posted_at >= ?")
        params.append(args.start_date)

    if args.end_date:
        conditions.append("posted_at <= ?")
        params.append(args.end_date + " 23:59:59")

    where = " AND ".join(conditions)
    conn = get_connection(db_path)

    if args.max_recent:
        # Build handle filter separately for the inner subquery
        if args.group:
            handles = cfg.get_handles(args.group)
            placeholders = ",".join("?" for _ in handles)
            handle_filter = f"username IN ({placeholders})"
            handle_params = handles
        else:
            handle_filter = "1=1"
            handle_params = []

        # ROW_NUMBER over ALL videos per handle so we get the N most recent overall,
        # then apply transcription-needed filters in the outer query.
        transcription_filter = (
            "(voice_to_text IS NULL OR length(voice_to_text) = 0) "
            "AND (transcript_source IS NULL OR transcript_source != 'asr')"
        )
        query = f"""
            SELECT video_id, video_url, username, caption FROM (
                SELECT video_id, video_url, username, caption,
                       voice_to_text, transcript_source,
                       ROW_NUMBER() OVER (PARTITION BY username ORDER BY create_time DESC) as rn
                FROM videos
                WHERE {handle_filter}
            ) WHERE rn <= ? AND {transcription_filter}
            ORDER BY username
        """
        rows = conn.execute(query, handle_params + [args.max_recent]).fetchall()
    else:
        rows = conn.execute(
            f"SELECT video_id, video_url, username, caption FROM videos WHERE {where} ORDER BY username",
            params,
        ).fetchall()
    conn.close()

    videos = [dict(r) for r in rows]
    logger.info(f"Found {len(videos)} videos to download and transcribe ({args.workers} workers)")

    if not videos:
        logger.info("Nothing to do.")
        return

    done = 0
    failed = 0
    skipped = 0
    total = len(videos)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_video, v, audio_dir, svc, db_path, idx, total,
                            min_speech_ratio=args.min_speech, vad_only=args.vad_only): v
            for idx, v in enumerate(videos, 1)
        }
        for future in as_completed(futures):
            outcome = future.result()
            if outcome == "ok":
                done += 1
            elif outcome == "skipped":
                skipped += 1
            else:
                failed += 1

    if args.vad_only:
        logger.info(f"VAD complete. {done} have speech, {skipped} marked no_speech, {failed} download failed.")
    else:
        logger.info(f"Done. {done} transcribed, {skipped} skipped (no_speech), {failed} failed.")


if __name__ == "__main__":
    main()
