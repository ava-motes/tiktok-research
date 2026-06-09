"""Pull TikTok videos for tracked handles.

Usage:
    python scripts/pull_videos.py                  # Uses default group from config
    python scripts/pull_videos.py --group sample   # Specific group
    python scripts/pull_videos.py --days 7         # Only past 7 days
    python scripts/pull_videos.py --reset-checkpoints  # Re-pull everything
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timezone, timedelta

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.logging_setup import setup_logging
from tiktok import auth
from tiktok.db import get_connection, insert_video
from tiktok.checkpoint import CheckpointStore
from tiktok.api.client import TikTokClient
from tiktok.api.videos import date_chunks, query_videos_for_chunk

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Pull TikTok videos for tracked handles")
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--days", type=int, default=None,
                        help="Only pull videos from the past N days (overrides config start_date)")
    parser.add_argument("--start-date", default=None, help="Start date YYYYMMDD (overrides config)")
    parser.add_argument("--end-date", default=None, help="End date YYYYMMDD (overrides config)")
    parser.add_argument("--reset-checkpoints", action="store_true", help="Clear checkpoints and re-pull")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)

    group_name = args.group or cfg.default_group("pull_videos")
    handles = cfg.get_handles(group_name)
    logger.info(f"Pulling videos for group '{group_name}' ({len(handles)} handles)")

    conn = get_connection(cfg.paths["database"])
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"], db_conn=conn)

    ckpt_path = os.path.join(cfg.paths["checkpoints"], f"pull_videos_{group_name}.json")
    ckpt = CheckpointStore(ckpt_path)
    if args.reset_checkpoints:
        ckpt.reset()

    start_date = cfg.start_date
    end_date = cfg.end_date
    if args.days:
        start_date = (datetime.now(timezone.utc) - timedelta(days=args.days - 1)).strftime("%Y%m%d")
    if args.start_date:
        start_date = args.start_date
    if args.end_date:
        end_date = args.end_date
    if args.days or args.start_date or args.end_date:
        logger.info(f"Date range overridden: {start_date} to {end_date}")

    chunks = date_chunks(start_date, end_date)
    total_videos = 0

    for i, handle in enumerate(handles, 1):
        handle_videos = 0
        for chunk_start, chunk_end in chunks:
            if ckpt.is_done(handle, chunk_start, chunk_end):
                continue

            videos = query_videos_for_chunk(client, handle, chunk_start, chunk_end)
            for v in videos:
                insert_video(conn, v)
            conn.commit()

            handle_videos += len(videos)
            ckpt.mark_done(handle, chunk_start, chunk_end)

        total_videos += handle_videos
        logger.info(f"[{i}/{len(handles)}] @{handle} — {handle_videos} videos")

    logger.info(f"Done. {total_videos} new videos inserted (group: {group_name})")
    conn.close()


if __name__ == "__main__":
    main()
