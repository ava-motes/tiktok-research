#!/usr/bin/env python3
"""Validate enrichment pipeline against the six research test TikTok URLs.

Resolves short links via yt-dlp metadata, maps into SQLite when present,
runs transcript / OCR / emoji, upserts one row each into
``tiktok_video_enriched``, and writes a JSON validation report.

Usage (on comm-cme-p01 only):
    python scripts/validate_enrichment_6videos.py
    python scripts/validate_enrichment_6videos.py --skip-ocr --skip-transcript
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection, insert_video
from tiktok.logging_setup import setup_logging
from tiktok.api.download import extract_video_metadata
from tiktok.enrichment.store import ensure_enrichment_schema

logger = logging.getLogger(__name__)

TEST_CASES = [
    {
        "id": 1,
        "url": "https://www.tiktok.com/t/ZP8gL1VxH/",
        "account": "harryjsisson",
        "expect": ["onscreen_text", "closed_captions", "audio_transcription"],
    },
    {
        "id": 2,
        "url": "https://www.tiktok.com/t/ZP8g8vtWu/",
        "account": "jaysworld411",
        "expect": ["onscreen_text", "closed_captions", "twitter_screenshots"],
    },
    {
        "id": 3,
        "url": "https://www.tiktok.com/t/ZP8g8wJBr/",
        "account": "joeycontino2",
        "expect": ["onscreen_text", "closed_captions", "greenscreen_twitter"],
    },
    {
        "id": 4,
        "url": "https://www.tiktok.com/t/ZP8g8sS7p/",
        "account": "cnn",
        "expect": ["edited_text", "closed_captions", "tweets"],
    },
    {
        "id": 5,
        "url": "https://www.tiktok.com/t/ZP8g8W5XY/",
        "account": "simpleblacktheory",
        "expect": ["stitch", "closed_captions", "onscreen_text", "screenshots"],
    },
    {
        "id": 6,
        "url": "https://www.tiktok.com/t/ZP8g8gkTK/",
        "account": "pauletteonthemic",
        "expect": ["screenshots", "music_lyrics"],
    },
]


def resolve_url(url: str) -> Dict[str, Any]:
    info = extract_video_metadata(url) or {}
    video_id = str(info.get("id") or "")
    uploader = (info.get("uploader") or info.get("creator") or info.get("channel") or "").lstrip("@")
    return {
        "video_id": video_id,
        "username": uploader,
        "title": info.get("title") or "",
        "description": info.get("description") or "",
        "duration": info.get("duration"),
        "webpage_url": info.get("webpage_url") or url,
    }


def ensure_stub_row(conn, meta: Dict[str, Any], account: str) -> None:
    """Insert a minimal videos row so workers have a URL if Research API hasn't ingested it."""
    vid = meta.get("video_id")
    if not vid:
        return
    existing = conn.execute("SELECT 1 FROM videos WHERE video_id=?", (vid,)).fetchone()
    if existing:
        return
    username = meta.get("username") or account
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


def run_workers(video_id: str, skip_ocr: bool, skip_transcript: bool) -> Dict[str, int]:
    py = sys.executable
    codes = {}
    if not skip_transcript:
        codes["transcript"] = os.system(
            f"{py} scripts/transcription_worker.py --video-id {video_id} --force"
        )
    if not skip_ocr:
        codes["ocr"] = os.system(
            f"{py} scripts/ocr_worker.py --video-id {video_id} --force --max-frames 12"
        )
    codes["emoji"] = os.system(
        f"{py} scripts/emoji_worker.py --video-id {video_id} --force"
    )
    return codes


def summarize(conn, video_id: str) -> Dict[str, Any]:
    from tiktok.enrichment.bigquery_loader import build_enriched_row

    row = build_enriched_row(conn, video_id) or {"video_id": video_id}
    transcript = row.get("whisper_transcript") or ""
    ocr_text = row.get("cleaned_ocr_text") or row.get("ocr_text") or ""
    raw_ocr = row.get("raw_ocr_text") or ""
    return {
        "video_id": video_id,
        "creator": row.get("creator_username") or "",
        "processing_status": row.get("enrichment_status") or "ok",
        "quality_score": row.get("enrichment_quality_score"),
        "ocr": {
            "source_count": row.get("ocr_source_count") or 0,
            "quality_score": row.get("ocr_quality_score"),
            "unique_text_ratio": row.get("ocr_unique_text_ratio"),
            "character_count": row.get("ocr_character_count") or len(ocr_text),
            "chars": len(ocr_text),
            "raw_chars": len(raw_ocr),
            "example_text": ocr_text[:400],
            "raw_sample": raw_ocr[:400],
        },
        "transcription": {
            "whisper_status": row.get("whisper_status") or "",
            "whisper_latency_seconds": row.get("whisper_latency_seconds"),
            "char_count": len(transcript),
            "first_200": transcript[:200],
        },
        "emoji": {
            "found": bool(row.get("emoji_characters")),
            "emoji_characters": row.get("emoji_characters") or "",
            "emoji_descriptions": row.get("emoji_descriptions") or "",
            "emoji_category": row.get("emoji_category") or "",
            "emoji_source": row.get("emoji_source") or "",
        },
        "enriched_row_preview": {
            "creator_username": row.get("creator_username"),
            "video_url": row.get("video_url"),
            "posted_at": row.get("posted_at"),
            "failure_reason": row.get("failure_reason") or "",
            "pipeline_version": row.get("pipeline_version"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate 6 TikTok enrichment test cases")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--skip-ocr", action="store_true")
    parser.add_argument("--skip-transcript", action="store_true")
    parser.add_argument(
        "--no-sync-bigquery",
        action="store_true",
        help="Skip upsert into tiktok_video_enriched",
    )
    parser.add_argument(
        "--report",
        default="data/final_six_video_validation_v4_1.json",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    from tiktok.enrichment.bigquery_loader import (
        PIPELINE_VERSION,
        ENRICHED_TABLE,
        bigquery_configured,
        count_enriched_rows,
        ensure_dataset_and_tables,
        enriched_table_id,
        sync_video_from_sqlite,
    )

    sync_bq = not args.no_sync_bigquery
    if sync_bq:
        if not bigquery_configured():
            logger.error("BigQuery not configured; continuing without sync")
            sync_bq = False
        else:
            ensure_dataset_and_tables()
            logger.info("Target BigQuery table: %s", enriched_table_id())

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "host": "comm-cme-p01.moody.utexas.edu",
        "pipeline_version": PIPELINE_VERSION,
        "bigquery_table": "cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched",
        "cases": [],
        "synced_video_ids": [],
    }

    for case in TEST_CASES:
        logger.info("=== TEST %s @%s ===", case["id"], case["account"])
        entry: Dict[str, Any] = {
            "case": case,
            "resolve": None,
            "worker_codes": None,
            "result": None,
            "bq_sync": None,
        }
        try:
            meta = resolve_url(case["url"])
            entry["resolve"] = meta
            if not meta.get("video_id"):
                entry["error"] = "could_not_resolve_video_id"
                entry["processing_status"] = "failed"
                report["cases"].append(entry)
                continue
            ensure_stub_row(conn, meta, case["account"])
            entry["worker_codes"] = run_workers(
                meta["video_id"], args.skip_ocr, args.skip_transcript
            )
            entry["result"] = summarize(conn, meta["video_id"])
            res = entry["result"]
            checks = {
                "has_transcript": (res.get("transcription") or {}).get("char_count", 0) > 0,
                "has_ocr": (res.get("ocr") or {}).get("chars", 0) > 0,
                "has_emojis": bool((res.get("emoji") or {}).get("found")),
            }
            entry["checks"] = checks
            entry["processing_status"] = res.get("processing_status") or "ok"
            entry["validation_summary"] = {
                "creator": res.get("creator") or case["account"],
                "video_id": meta["video_id"],
                "ocr_sources_detected": (res.get("ocr") or {}).get("source_count"),
                "ocr_text_sample": (res.get("ocr") or {}).get("example_text") or "",
                "whisper_length": (res.get("transcription") or {}).get("char_count") or 0,
                "emoji_detected": (res.get("emoji") or {}).get("emoji_characters") or "",
                "quality_score": res.get("quality_score"),
                "ocr_quality_score": (res.get("ocr") or {}).get("quality_score"),
            }

            if sync_bq:
                counts = sync_video_from_sqlite(conn, meta["video_id"])
                entry["bq_sync"] = counts
                report["synced_video_ids"].append(meta["video_id"])
                logger.info("BQ upsert %s → %s", meta["video_id"], counts)
        except Exception as e:
            entry["error"] = str(e)
            entry["processing_status"] = "failed"
            logger.exception("Case %s failed", case["id"])
        report["cases"].append(entry)

    if sync_bq and report["synced_video_ids"]:
        report["bigquery_row_count"] = count_enriched_rows(report["synced_video_ids"])
        report["bigquery_table_total_rows"] = count_enriched_rows()
        logger.info(
            "BigQuery %s validation rows=%s total=%s",
            ENRICHED_TABLE,
            report["bigquery_row_count"],
            report["bigquery_table_total_rows"],
        )
    else:
        report["bigquery_row_count"] = 0

    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", args.report)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
