"""Pull TikTok user profile info for tracked handles.

Usage:
    python scripts/pull_user_info.py                     # Uses default group from config
    python scripts/pull_user_info.py --group sample      # Specific group
    python scripts/pull_user_info.py --reset-checkpoints # Re-pull everything
"""

import sys
import os
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.logging_setup import setup_logging
from tiktok import auth
from tiktok.db import get_connection, insert_user
from tiktok.checkpoint import CheckpointStore
from tiktok.api.client import TikTokClient
from tiktok.api.users import get_user_info

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Pull TikTok user profile info")
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--reset-checkpoints", action="store_true", help="Clear checkpoints and re-pull")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)

    group_name = args.group or cfg.default_group("pull_user_info")
    handles = cfg.get_handles(group_name)
    logger.info(f"Pulling user info for group '{group_name}' ({len(handles)} handles)")

    conn = get_connection(cfg.paths["database"])
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"], db_conn=conn)

    ckpt_path = os.path.join(cfg.paths["checkpoints"], f"pull_user_info_{group_name}.json")
    ckpt = CheckpointStore(ckpt_path)
    if args.reset_checkpoints:
        ckpt.reset()

    success = 0
    failed = 0

    for i, handle in enumerate(handles, 1):
        if ckpt.is_done(handle):
            continue

        user = get_user_info(client, handle)
        insert_user(conn, user)
        conn.commit()
        ckpt.mark_done(handle)

        if user["api_failed"]:
            failed += 1
            logger.info(f"[{i}/{len(handles)}] @{handle} — FAILED")
        else:
            success += 1
            logger.info(f"[{i}/{len(handles)}] @{handle} — {user['follower_count']} followers")

    logger.info(f"Done. {success} succeeded, {failed} failed (group: {group_name})")
    conn.close()


if __name__ == "__main__":
    main()
