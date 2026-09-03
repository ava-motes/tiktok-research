"""Classify account types using OpenAI.

Usage:
    python scripts/classify_accounts.py                # Uses default group
    python scripts/classify_accounts.py --group sample
"""

import sys
import os
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openai import OpenAI
from tiktok.config import load_config
from tiktok.logging_setup import setup_logging
from tiktok.db import get_connection, update_user_classification
from tiktok.classify.accounts import classify_accounts_batch, ACCOUNT_TYPES

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Classify TikTok account types")
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)

    group_name = args.group or cfg.default_group("classify_accounts")
    handles = cfg.get_handles(group_name)

    conn = get_connection(cfg.paths["database"])
    client = OpenAI(api_key=cfg.openai_api_key)

    cls_cfg = cfg.classification
    batch_size = cls_cfg["account_batch_size"]
    model = cls_cfg["openai_model"]
    temp = cls_cfg["temperature"]

    # Get users that haven't been classified yet
    placeholders = ",".join("?" for _ in handles)
    rows = conn.execute(
        f"SELECT * FROM users WHERE account_type_code IS NULL AND username IN ({placeholders})",
        handles,
    ).fetchall()
    accounts = [dict(r) for r in rows]

    logger.info(f"Found {len(accounts)} unclassified accounts for group '{group_name}'")
    if not accounts:
        logger.info("Nothing to classify.")
        conn.close()
        return

    total_batches = (len(accounts) + batch_size - 1) // batch_size
    for i in range(0, len(accounts), batch_size):
        batch = accounts[i : i + batch_size]
        batch_num = i // batch_size + 1
        logger.info(f"Classifying batch {batch_num}/{total_batches} ({len(batch)} accounts)")

        try:
            codes = classify_accounts_batch(client, batch, model=model, temperature=temp)
            for account, code in zip(batch, codes):
                label = ACCOUNT_TYPES.get(code, "unsure")
                update_user_classification(conn, account["username"], code, label, model)
            conn.commit()
        except Exception as e:
            logger.error(f"Error on batch {batch_num}: {e}")

    logger.info("Account classification complete.")
    conn.close()


if __name__ == "__main__":
    main()
