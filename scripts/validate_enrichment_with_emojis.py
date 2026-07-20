#!/usr/bin/env python3
"""Validate enrichment (OCR/Whisper/emoji) for six research URLs + ice-cube example.

Run on comm-cme-p01 only:
    python scripts/validate_enrichment_with_emojis.py

Writes:
    data/enrichment_validation_with_emojis.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.api.download import extract_video_metadata
from tiktok.config import load_config
from tiktok.db import get_connection, insert_video
from tiktok.enrichment.bigquery_loader import (
    bigquery_configured,
    build_enriched_row,
    ensure_dataset_and_tables,
    sync_video_from_sqlite,
)
from tiktok.enrichment.store import ensure_enrichment_schema
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)

TEST_CASES: List[Dict[str, Any]] = [
    {
        "id": 1,
        "url": "https://www.tiktok.com/t/ZP8gL1VxH/",
        "account": "harryjsisson",
        "expect": ["captions", "onscreen_text"],
    },
    {
        "id": 2,
        "url": "https://www.tiktok.com/t/ZP8g8vtWu/",
        "account": "jaysworld411",
        "expect": ["tweet_screenshot", "overlay"],
    },
    {
        "id": 3,
        "url": "https://www.tiktok.com/t/ZP8g8wJBr/",
        "account": "joeycontino2",
        "expect": ["green_screen", "overlay"],
    },
    {
        "id": 4,
        "url": "https://www.tiktok.com/t/ZP8g8sS7p/",
        "account": "cnn",
        "expect": ["edited_text", "tweets"],
    },
    {
        "id": 5,
        "url": "https://www.tiktok.com/t/ZP8g8W5XY/",
        "account": "simpleblacktheory",
        "expect": ["stitch", "screenshots", "captions"],
    },
    {
        "id": 6,
        "url": "https://www.tiktok.com/t/ZP8g8gkTK/",
        "account": "pauletteonthemic",
        "expect": ["screenshots", "lyrics", "no_speech"],
    },
    {
        "id": 7,
        "url": "https://www.tiktok.com/t/ZP8pqWj12/",
        "account": None,  # resolve from metadata
        "expect": ["ice_cube_emoji"],
        "label": "ice_cube_example",
    },
]


def resolve_url(url: str) -> Dict[str, Any]:
    info = extract_video_metadata(url) or {}
    video_id = str(info.get("id") or "")
    uploader = (
        info.get("uploader") or info.get("creator") or info.get("channel") or ""
    ).lstrip("@")
    return {
        "video_id": video_id,
        "username": uploader,
        "title": info.get("title") or "",
        "description": info.get("description") or "",
        "duration": info.get("duration"),
        "webpage_url": info.get("webpage_url") or url,
    }


def ensure_stub_row(conn, meta: Dict[str, Any], account: str) -> None:
    vid = meta.get("video_id")
    if not vid:
        return
    if conn.execute("SELECT 1 FROM videos WHERE video_id=?", (vid,)).fetchone():
        # Refresh caption if stub/empty and we have description (emoji cases)
        desc = meta.get("description") or meta.get("title") or ""
        if desc:
            conn.execute(
                "UPDATE videos SET caption=COALESCE(NULLIF(caption,''), ?) WHERE video_id=?",
                (desc, vid),
            )
            conn.commit()
        return
    username = meta.get("username") or account or "unknown"
    insert_video(
        conn,
        {
            "video_id": vid,
            "username": username,
            "video_url": meta.get("webpage_url")
            or f"https://www.tiktok.com/@{username}/video/{vid}",
            "create_time": 0,
            "posted_at": "",
            "caption": meta.get("description") or meta.get("title") or "",
            "hashtags": "",
            "like_count": 0,
            "share_count": 0,
            "comment_count": 0,
            "save_count": 0,
            "duration_seconds": int(meta.get("duration") or 0),
            "voice_to_text": "",
            "sticker_overlay_text": "",
            "sticker_info_list": "",
        },
    )
    conn.commit()


def run_workers(video_id: str) -> Dict[str, int]:
    py = sys.executable
    return {
        "transcript": os.system(
            f"{py} scripts/transcription_worker.py --video-id {video_id} --force"
        ),
        "ocr": os.system(
            f"{py} scripts/ocr_worker.py --video-id {video_id} --force --max-frames 12"
        ),
        "emoji": os.system(f"{py} scripts/emoji_worker.py --video-id {video_id} --force"),
    }


def summarize_case(conn, case: Dict[str, Any], meta: Dict[str, Any], codes: Dict[str, int]) -> Dict[str, Any]:
    vid = meta["video_id"]
    row = build_enriched_row(conn, vid) or {}
    emojis = row.get("emoji_characters") or row.get("emojis") or ""
    ice = "🧊" in emojis or "ice cube" in (row.get("emoji_descriptions") or "").lower()
    try:
        sources = json.loads(row.get("ocr_sources") or "[]")
    except Exception:
        sources = []
    try:
        segments = json.loads(row.get("ocr_text_segments") or "[]")
    except Exception:
        segments = []

    ocr_ok = int(row.get("ocr_frames_processed") or 0) > 0
    tr_len = len(row.get("transcript") or "")
    tr_ok = bool(row.get("audio_available"))
    emoji_count = int(row.get("emoji_count") or 0)
    errors = []
    for k, code in (codes or {}).items():
        if code != 0:
            errors.append(f"{k}_exit_{code}")
    if row.get("failure_reason"):
        errors.append(row["failure_reason"])

    return {
        "video_id": vid,
        "creator": row.get("creator_handle") or meta.get("username") or case.get("account"),
        "url": case["url"],
        "expect": case.get("expect"),
        "ocr_success": ocr_ok,
        "transcription_success": tr_ok,
        "emoji_success": (codes or {}).get("emoji", 1) == 0,
        "emoji_count": emoji_count,
        "emoji_characters": emojis,
        "emoji_descriptions": row.get("emoji_descriptions") or "",
        "emoji_sources": row.get("emoji_sources") or "[]",
        "ocr_text_length": len(row.get("cleaned_ocr_text") or row.get("ocr_text") or ""),
        "raw_ocr_text_length": len(row.get("raw_ocr_text") or ""),
        "transcript_length": tr_len,
        "text_sources_detected": sources,
        "ocr_segment_count": len(segments),
        "ocr_segment_source_types": sorted(
            {s.get("source_type") for s in segments if s.get("source_type")}
        ),
        "cleaned_ocr_preview": (row.get("cleaned_ocr_text") or "")[:280],
        "raw_ocr_preview": (row.get("raw_ocr_text") or "")[:280],
        "transcript_preview": (row.get("transcript") or "")[:200],
        "ice_cube_detected": ice if case.get("label") == "ice_cube_example" or "ice_cube_emoji" in (case.get("expect") or []) else None,
        "enrichment_status": row.get("enrichment_status"),
        "pipeline_version": row.get("pipeline_version"),
        "errors": errors,
        "worker_codes": codes,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--report", default="data/enrichment_validation_with_emojis.json"
    )
    parser.add_argument("--skip-bigquery", action="store_true")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    sync = not args.skip_bigquery and bigquery_configured()
    if sync:
        ensure_dataset_and_tables()

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "host": "comm-cme-p01.moody.utexas.edu",
        "bigquery_table": "cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched",
        "cases": [],
    }

    for case in TEST_CASES:
        label = case.get("label") or case.get("account") or case["url"]
        logger.info("=== CASE %s %s ===", case["id"], label)
        entry: Dict[str, Any] = {"case": case}
        try:
            meta = resolve_url(case["url"])
            entry["resolve"] = meta
            if not meta.get("video_id"):
                entry["error"] = "could_not_resolve_video_id"
                report["cases"].append(entry)
                continue
            account = case.get("account") or meta.get("username") or "unknown"
            ensure_stub_row(conn, meta, account)
            codes = run_workers(meta["video_id"])
            summary = summarize_case(conn, case, meta, codes)
            if sync:
                summary["bq_sync"] = sync_video_from_sqlite(conn, meta["video_id"])
            entry["result"] = summary
        except Exception as e:
            entry["error"] = str(e)
            logger.exception("Case %s failed", case["id"])
        report["cases"].append(entry)

    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", args.report)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
