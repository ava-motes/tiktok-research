"""Download audio from TikTok videos using yt-dlp.

Usage:
    python scripts/download_audio.py                     # All videos, default group
    python scripts/download_audio.py --group test        # Specific group
    python scripts/download_audio.py --group test --limit 10  # First 10 only
"""

import sys
import os
import time
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.logging_setup import setup_logging
from tiktok.db import get_connection
from tiktok.api.download import download_audio, _find_existing

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Download audio from TikTok videos")
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--limit", type=int, default=0, help="Max number of videos to download (0=all)")
    parser.add_argument("--missing-vtt-only", action="store_true", help="Only download videos that have no voice_to_text from API")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)

    group_name = args.group or cfg.default_group("pull_videos")
    handles = cfg.get_handles(group_name)
    audio_dir = cfg.transcription["audio_dir"]
    delay = cfg.transcription.get("download_delay", 1.5)

    conn = get_connection(cfg.paths["database"])

    # Get videos that don't have audio files yet
    placeholders = ",".join("?" for _ in handles)
    vtt_filter = "AND (voice_to_text IS NULL OR voice_to_text = '')" if args.missing_vtt_only else ""
    rows = conn.execute(
        f"""SELECT video_id, video_url, username FROM videos
        WHERE username IN ({placeholders})
        {vtt_filter}
        ORDER BY username, create_time DESC""",
        handles,
    ).fetchall()

    videos = [dict(r) for r in rows]

    # Filter out videos that already have audio (any format)
    videos = [v for v in videos if not _find_existing(audio_dir, v["video_id"])]

    if args.limit > 0:
        videos = videos[:args.limit]

    logger.info(f"Downloading audio for {len(videos)} videos (group: {group_name})")

    downloaded = 0
    failed = 0

    for i, v in enumerate(videos, 1):
        t0 = time.time()
        result = download_audio(v["video_url"], v["video_id"], audio_dir)
        elapsed = time.time() - t0

        if result:
            downloaded += 1
            logger.info(f"[{i}/{len(videos)}] @{v['username']} — {v['video_id']} — downloaded ({elapsed:.1f}s)")
        else:
            failed += 1
            logger.warning(f"[{i}/{len(videos)}] @{v['username']} — {v['video_id']} — failed")

        if i < len(videos) and delay > 0:
            time.sleep(delay)

    logger.info(f"Done. {downloaded} downloaded, {failed} failed (group: {group_name})")
    conn.close()


if __name__ == "__main__":
    main()
