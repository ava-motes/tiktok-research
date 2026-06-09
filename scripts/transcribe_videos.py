"""Transcribe videos that have audio files in the audio/ directory.

Usage:
    python scripts/transcribe_videos.py --group complete           # local faster-whisper
    python scripts/transcribe_videos.py --group complete --openai  # OpenAI Whisper API
"""

import sys
import os
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.logging_setup import setup_logging
from tiktok.db import get_connection, build_text_for_nlp
from tiktok.transcription.service import TranscriptionService, WhisperAPITranscriptionService
from tiktok.api.download import _find_existing

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Transcribe videos with faster-whisper or OpenAI Whisper API")
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--openai", action="store_true", help="Use OpenAI Whisper API instead of local faster-whisper")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)

    conn = get_connection(cfg.paths["database"])
    t_cfg = cfg.transcription

    if args.openai:
        svc = WhisperAPITranscriptionService()
        logger.info("Using OpenAI Whisper API for transcription")
    else:
        svc = TranscriptionService(
            model_size=t_cfg["model_size"],
            audio_dir=t_cfg["audio_dir"],
            compute_type=t_cfg["compute_type"],
        )

    # Find videos that need transcription:
    # Only transcribe videos where voice_to_text is empty/null (no API transcript)
    # and no ASR transcript exists yet
    if args.group:
        handles = cfg.get_handles(args.group)
        placeholders = ",".join("?" for _ in handles)
        rows = conn.execute(
            f"""SELECT video_id, caption, voice_to_text FROM videos
            WHERE (voice_to_text IS NULL OR voice_to_text = '')
            AND (transcript_source IS NULL OR transcript_source != 'asr')
            AND username IN ({placeholders})""",
            handles,
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT video_id, caption, voice_to_text FROM videos
            WHERE (voice_to_text IS NULL OR voice_to_text = '')
            AND (transcript_source IS NULL OR transcript_source != 'asr')"""
        ).fetchall()

    videos = [dict(r) for r in rows]
    logger.info(f"Found {len(videos)} videos to check for audio files")

    transcribed = 0
    for v in videos:
        audio_path = _find_existing(t_cfg["audio_dir"], v["video_id"])
        if not audio_path:
            continue

        result = svc.transcribe(v["video_id"], audio_path)
        if result is None:
            continue

        text_for_nlp = build_text_for_nlp(v["caption"], result.text)

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
        transcribed += 1

    engine = "OpenAI Whisper API" if args.openai else "faster-whisper"
    logger.info(f"Transcribed {transcribed} videos with {engine}")
    conn.close()


if __name__ == "__main__":
    main()
