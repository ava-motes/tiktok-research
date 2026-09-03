#!/usr/bin/env python3
"""Emoji worker — extract emojis + CLDR/Unicode names from all text layers.

Usage:
    python scripts/emoji_worker.py --group batch_test --limit 50
    python scripts/emoji_worker.py --video-id 7660301464664870157 --force

Sources: caption, hashtags, transcript, OCR/visual layers, stickers.
No media download required.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from pathlib import Path
import importlib.util

def _setup_repo():
    for p in Path(__file__).resolve().parents:
        boot = p / "common" / "bootstrap.py"
        if boot.is_file():
            spec = importlib.util.spec_from_file_location("_tiktok_bootstrap", boot)
            mod = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(mod)
            return mod.setup()
    raise RuntimeError("common/bootstrap.py not found")

ROOT = _setup_repo()

from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.logging_setup import setup_logging
from enrichment.emoji_extract import extract_emoji_rows_for_video
from datetime import datetime, timezone

from enrichment.store import (
    ensure_enrichment_schema,
    fetch_videos_for_enrichment,
    insert_enrichment_log,
    replace_emoji_rows,
    touch_pipeline_status,
)
from enrichment.worker_log import WorkerTimer

logger = logging.getLogger(__name__)
WORKER = "emoji"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def process_one(conn, row: dict) -> bool:
    video_id = row["video_id"]
    with WorkerTimer(WORKER, video_id) as timer:
        touch_pipeline_status(conn, video_id, emoji_started=_now())
        conn.commit()
        try:
            # Prefer latest OCR staging + videos columns
            ocr_bits = conn.execute(
                "SELECT ocr_text FROM video_ocr WHERE video_id=?", (video_id,)
            ).fetchall()
            if ocr_bits:
                row = dict(row)
                row["onscreen_text"] = "\n".join(r["ocr_text"] for r in ocr_bits if r["ocr_text"])

            tr = conn.execute(
                "SELECT transcript FROM video_transcripts WHERE video_id=? AND status='ok'",
                (video_id,),
            ).fetchone()
            if tr and tr["transcript"]:
                row = dict(row)
                row["transcript"] = tr["transcript"]

            emoji_rows = extract_emoji_rows_for_video(row)
            n = replace_emoji_rows(conn, video_id, emoji_rows)
            touch_pipeline_status(conn, video_id, emoji_completed=_now())
            timer.success(emoji_rows=n)
            insert_enrichment_log(conn, timer.to_result().to_dict())
            conn.commit()
            return True
        except Exception as e:
            timer.fail(str(e))
            touch_pipeline_status(conn, video_id, emoji_completed=_now())
            insert_enrichment_log(conn, timer.to_result().to_dict())
            conn.commit()
            return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Emoji extraction worker")
    parser.add_argument("--config", default="common/config.yaml")
    parser.add_argument("--group", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    from tiktok.collection.video_ids import add_video_id_args, resolve_video_ids

    add_video_id_args(parser)
    args = parser.parse_args()

    setup_logging()
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
        need_emoji=not args.force,
    )
    logger.info("Emoji candidates: %s", len(rows))

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
