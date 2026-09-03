"""Pull the N most recent TikTok videos for tracked handles.

Queries date chunks in reverse-chronological order, stopping per handle once
N videos have been collected, then inserts only the N most recent.

Usage:
    python scripts/pull_recent_videos.py --group influencer_classification_r1
    python scripts/pull_recent_videos.py --group influencer_classification_r1 --max-videos 10
    python scripts/pull_recent_videos.py --group influencer_classification_r1 --lookback-days 365
    python scripts/pull_recent_videos.py --group influencer_classification_r1 --reset-checkpoints
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timezone, timedelta

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
    parser = argparse.ArgumentParser(description="Pull N most recent TikTok videos per handle")
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--max-videos", type=int, default=10,
                        help="Max videos to collect per handle (default: 10)")
    parser.add_argument("--lookback-days", type=int, default=365,
                        help="How many days back to search (default: 365)")
    parser.add_argument("--reset-checkpoints", action="store_true", help="Clear checkpoints and re-pull")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)

    group_name = args.group or cfg.default_group("pull_videos")
    handles = cfg.get_handles(group_name)
    logger.info(
        f"Pulling {args.max_videos} most recent videos for group '{group_name}' ({len(handles)} handles)"
    )

    conn = get_connection(cfg.paths["database"])
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"], db_conn=conn)

    ckpt_path = os.path.join(cfg.paths["checkpoints"], f"pull_recent_videos_{group_name}.json")
    ckpt = CheckpointStore(ckpt_path)
    if args.reset_checkpoints:
        ckpt.reset()

    # Build chunks from lookback window, reversed so we query most recent first
    end_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=args.lookback_days)).strftime("%Y%m%d")
    chunks = list(reversed(date_chunks(start_date, end_date)))

    total_videos = 0

    for i, handle in enumerate(handles, 1):
        if ckpt.is_done(handle):
            logger.info(f"[{i}/{len(handles)}] @{handle} — already done, skipping")
            continue

        collected = []
        for chunk_start, chunk_end in chunks:
            if len(collected) >= args.max_videos:
                break

            videos = query_videos_for_chunk(client, handle, chunk_start, chunk_end)
            collected.extend(videos)

        # Sort descending by create_time and keep only the N most recent
        collected.sort(key=lambda v: v.get("create_time", 0), reverse=True)
        recent = collected[:args.max_videos]

        for v in recent:
            insert_video(conn, v)
        conn.commit()
        ckpt.mark_done(handle)

        total_videos += len(recent)
        logger.info(f"[{i}/{len(handles)}] @{handle} — {len(recent)} videos inserted")

    logger.info(f"Done. {total_videos} total videos inserted (group: {group_name})")
    conn.close()


if __name__ == "__main__":
    main()
