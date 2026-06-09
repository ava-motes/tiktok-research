"""Classify videos as news/politics using OpenAI.

Usage:
    python scripts/classify_videos.py --group complete --start-date 2026-02-22 --end-date 2026-02-28
    python scripts/classify_videos.py --group sample
    python scripts/classify_videos.py --days 7
"""

import sys
import os
import argparse
import logging
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openai import OpenAI
from tiktok.config import load_config
from tiktok.logging_setup import setup_logging
from tiktok.db import get_connection, update_video_classification
from tiktok.classify.videos import classify_batch

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Classify videos as news/politics")
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--days", type=int, default=None, help="Only videos from past N days")
    parser.add_argument("--start-date", default=None, help="Only videos posted on or after (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="Only videos posted on or before (YYYY-MM-DD)")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)

    group_name = args.group or cfg.default_group("classify_videos")
    handles = cfg.get_handles(group_name)

    conn = get_connection(cfg.paths["database"])
    client = OpenAI(api_key=cfg.openai_api_key)

    cls_cfg = cfg.classification
    batch_size = cls_cfg["video_batch_size"]
    model = cls_cfg["openai_model"]
    temp = cls_cfg["temperature"]

    # Build query for unclassified videos
    placeholders = ",".join("?" for _ in handles)
    conditions = [f"username IN ({placeholders})", "news IS NULL"]
    params = list(handles)

    if args.days:
        min_create_time = int(time.time()) - args.days * 86400
        conditions.append("create_time >= ?")
        params.append(min_create_time)
    if args.start_date:
        conditions.append("posted_at >= ?")
        params.append(args.start_date)
    if args.end_date:
        conditions.append("posted_at <= ?")
        params.append(args.end_date + " 23:59:59")

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"SELECT video_id, caption, hashtags FROM videos WHERE {where} ORDER BY username",
        params,
    ).fetchall()
    videos = [dict(r) for r in rows]

    logger.info(f"Found {len(videos)} unclassified videos for group '{group_name}'")

    if not videos:
        logger.info("Nothing to classify.")
        conn.close()
        return

    total_batches = (len(videos) + batch_size - 1) // batch_size
    for i in range(0, len(videos), batch_size):
        batch = videos[i : i + batch_size]
        batch_num = i // batch_size + 1
        logger.info(f"Classifying batch {batch_num}/{total_batches} ({len(batch)} videos)")

        try:
            labels = classify_batch(client, batch, model=model, temperature=temp)
            for video, label in zip(batch, labels):
                update_video_classification(
                    conn, video["video_id"],
                    news=label.get("news", 0),
                    politics=label.get("politics", 0),
                    model_version=model,
                )
            conn.commit()
        except Exception as e:
            logger.error(f"Error on batch {batch_num}: {e}")

        if i + batch_size < len(videos):
            time.sleep(0.5)

    logger.info("Classification complete.")
    conn.close()


if __name__ == "__main__":
    main()
