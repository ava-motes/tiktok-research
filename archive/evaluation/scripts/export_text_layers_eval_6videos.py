"""One-off unified text-layer comparison export for six fixed eval TikTok URLs.

Combines Research API fields, browser hydration stickers, and EasyOCR fallback
into a single research CSV. Does not modify the production ingestion pipeline.

Usage (from project root):
    source venv/bin/activate
    pip install -r requirements-ocr.txt   # only if OCR may run
    python scripts/export_text_layers_eval_6videos.py
    python scripts/export_text_layers_eval_6videos.py --force   # re-run OCR
    python scripts/export_text_layers_eval_6videos.py --skip-ocr  # API + browser only
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok import auth
from tiktok.api.client import TikTokClient
from tiktok.api.download import download_video_file, extract_video_metadata
from tiktok.api.videos import fetch_video_by_id
from tiktok.config import load_config
from tiktok.db import get_connection
from tiktok.logging_setup import setup_logging
from tiktok.web.metadata import collect_web_video_metadata

logger = logging.getLogger(__name__)

EVAL_URLS: List[str] = [
    "https://www.tiktok.com/t/ZP8gL1VxH/",
    "https://www.tiktok.com/t/ZP8g8vtWu/",
    "https://www.tiktok.com/t/ZP8g8wJBr/",
    "https://www.tiktok.com/t/ZP8g8sS7p/",
    "https://www.tiktok.com/t/ZP8g8W5XY/",
    "https://www.tiktok.com/t/ZP8g8gkTK/",
]

OUTPUT_CSV = os.path.join("data", "text_layers_eval_6videos.csv")

API_META_COLUMNS = [
    "eval_url",
    "video_url",
    "username",
    "posted_at",
    "hashtags",
    "like_count",
    "share_count",
    "comment_count",
    "save_count",
    "duration_seconds",
]

TEXT_COLUMNS = [
    "caption",
    "voice_to_text",
    "sticker_overlay_text",
    "browser_ocr_text",
    "onscreen_text",
    "onscreen_text_raw",
    "visual_text_combined",
    "visual_text_source_priority",
]

COLUMNS = ["video_id"] + API_META_COLUMNS + TEXT_COLUMNS


def _load_onscreen_from_db(conn, video_id: str) -> Tuple[str, str]:
    row = conn.execute(
        "SELECT onscreen_text FROM videos WHERE video_id = ?",
        (video_id,),
    ).fetchone()
    if not row:
        return "", ""
    text = (row["onscreen_text"] or "").strip()
    return text, text


def _onscreen_from_record(rec: Dict[str, Any]) -> Tuple[str, str]:
    deduped = (rec.get("onscreen_text") or "").strip()
    raw = (rec.get("onscreen_text_raw") or deduped).strip()
    return deduped, raw


def _load_onscreen_from_batch_json(video_id: str) -> Tuple[str, str]:
    """Reuse newest ocr_eval_batch_*.json if present."""
    paths = sorted(
        glob.glob(os.path.join("data", "ocr_eval_batch_*.json")),
        key=os.path.getmtime,
        reverse=True,
    )
    for path in paths:
        try:
            with open(path, encoding="utf-8") as f:
                records = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        for rec in records:
            if str(rec.get("video_id")) == str(video_id):
                deduped, raw = _onscreen_from_record(rec)
                if deduped or raw:
                    return deduped, raw
    return "", ""


def _load_onscreen_from_batch_csv(video_id: str) -> Tuple[str, str]:
    """Reuse newest ocr_eval_batch_*.csv if present."""
    paths = sorted(
        glob.glob(os.path.join("data", "ocr_eval_batch_*.csv")),
        key=os.path.getmtime,
        reverse=True,
    )
    for path in paths:
        try:
            with open(path, newline="", encoding="utf-8") as f:
                for rec in csv.DictReader(f):
                    if str(rec.get("video_id")) == str(video_id):
                        deduped, raw = _onscreen_from_record(rec)
                        if deduped or raw:
                            return deduped, raw
        except OSError:
            continue
    return "", ""


def _load_onscreen_from_frames_json(video_id: str) -> Tuple[str, str]:
    """Reuse data/ocr_eval_frames_<video_id>.json if present."""
    path = os.path.join("data", f"ocr_eval_frames_{video_id}.json")
    if not os.path.isfile(path):
        return "", ""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return "", ""
    deduped = (data.get("onscreen_text_deduped") or data.get("onscreen_text") or "").strip()
    raw = (data.get("onscreen_text_raw") or deduped).strip()
    return deduped, raw


def _load_cached_onscreen(conn, video_id: str) -> Tuple[str, str, str]:
    """Try DB, batch JSON/CSV, then per-video frames JSON. Returns (text, raw, source)."""
    onscreen, raw = _load_onscreen_from_db(conn, video_id)
    if onscreen:
        return onscreen, raw, "reused_db"

    onscreen, raw = _load_onscreen_from_batch_json(video_id)
    if onscreen:
        return onscreen, raw, "reused_batch_json"

    onscreen, raw = _load_onscreen_from_batch_csv(video_id)
    if onscreen:
        return onscreen, raw, "reused_batch_csv"

    onscreen, raw = _load_onscreen_from_frames_json(video_id)
    if onscreen:
        return onscreen, raw, "reused_frames_json"

    return "", "", ""


def _normalize_block(s: str) -> str:
    return (s or "").strip()


def _blocks_equal(a: str, b: str) -> bool:
    return _normalize_block(a).lower() == _normalize_block(b).lower()


def build_visual_merged(
    sticker_api: str,
    browser: str,
    ocr: str,
) -> Tuple[str, str]:
    """Merge visual layers; report which sources contributed."""
    parts: List[str] = []
    sources: List[str] = []

    api = _normalize_block(sticker_api)
    br = _normalize_block(browser)
    oc = _normalize_block(ocr)

    if api:
        parts.append(api)
        sources.append("api_sticker")
    if br and not _blocks_equal(br, api):
        parts.append(br)
        sources.append("browser_sticker")
    if oc:
        if api and _blocks_equal(oc, api):
            pass
        elif br and _blocks_equal(oc, br) and not api:
            pass
        else:
            parts.append(oc)
            sources.append("easyocr")

    combined = "\n---\n".join(parts)
    priority = "+".join(sources) if sources else "none"
    return combined, priority


def _api_meta_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "video_url": row.get("video_url") or "",
        "username": row.get("username") or "",
        "posted_at": row.get("posted_at") or "",
        "hashtags": row.get("hashtags") or "",
        "like_count": row.get("like_count", 0) or 0,
        "share_count": row.get("share_count", 0) or 0,
        "comment_count": row.get("comment_count", 0) or 0,
        "save_count": row.get("save_count", 0) or 0,
        "duration_seconds": row.get("duration_seconds", 0) or 0,
        "caption": row.get("caption") or "",
        "voice_to_text": row.get("voice_to_text") or "",
        "sticker_overlay_text": row.get("sticker_overlay_text") or "",
    }


def _load_api_from_db(conn, video_id: str) -> Dict[str, Any]:
    row = conn.execute(
        """SELECT video_url, username, posted_at, hashtags,
                  like_count, share_count, comment_count, save_count,
                  duration_seconds, caption, voice_to_text, sticker_overlay_text
           FROM videos WHERE video_id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return {}
    return _api_meta_from_row(dict(row))


def _fetch_api(
    client: TikTokClient,
    video_id: str,
    username: str,
) -> Tuple[Dict[str, Any], str]:
    try:
        row = fetch_video_by_id(client, username, video_id)
        if not row:
            return {}, "not_found"
        return _api_meta_from_row(row), "ok"
    except Exception as e:
        logger.exception("API fetch failed for %s", video_id)
        return {}, str(e)


def _fetch_browser(url: str) -> Tuple[str, str]:
    try:
        web = collect_web_video_metadata(url)
        if web.get("error"):
            return "", web.get("error") or "error"
        return web.get("web_sticker_text") or "", "ok"
    except Exception as e:
        logger.exception("Browser fetch failed for %s", url)
        return "", str(e)


def _run_ocr(
    canonical_url: str,
    video_id: str,
    cache_dir: str,
    max_frames: int,
) -> Tuple[str, str, str]:
    """Return (onscreen_text, onscreen_text_raw, status)."""
    try:
        import easyocr  # noqa: F401
    except ImportError:
        return "", "", "easyocr_not_installed"

    from tiktok.ocr.pipeline import extract_onscreen_text

    path = download_video_file(canonical_url, video_id, cache_dir)
    if not path:
        return "", "", "download_failed"
    try:
        result = extract_onscreen_text(
            path, video_id, max_frames=max_frames, seconds_between_samples=1.0
        )
        return (
            result.onscreen_text or "",
            result.onscreen_text_raw or "",
            "ok",
        )
    except Exception as e:
        return "", "", str(e)


def process_url(
    eval_url: str,
    *,
    client: TikTokClient,
    conn,
    cache_dir: str,
    max_frames: int,
    force_ocr: bool,
    skip_ocr: bool,
) -> Dict[str, Any]:
    info = extract_video_metadata(eval_url)
    if not info:
        return {"url": eval_url, "error": "yt_dlp_resolve_failed"}

    video_id = str(info.get("id") or "")
    username = (info.get("uploader") or info.get("channel") or "").strip()
    canonical = (
        info.get("webpage_url") or info.get("url") or eval_url
    )
    if username and video_id:
        canonical = f"https://www.tiktok.com/@{username}/video/{video_id}"

    api_fields, api_status = _fetch_api(client, video_id, username)
    if not api_fields:
        api_fields = _load_api_from_db(conn, video_id)
        if api_fields and api_status == "not_found":
            api_status = "reused_db"

    browser_text, browser_status = _fetch_browser(canonical)

    caption = api_fields.get("caption") or (info.get("description") or "")[:4000]
    voice_to_text = api_fields.get("voice_to_text") or ""
    sticker_overlay = api_fields.get("sticker_overlay_text") or ""
    video_url = api_fields.get("video_url") or canonical

    onscreen_text = ""
    onscreen_raw = ""
    ocr_triggered = False
    ocr_status = "none"

    if not force_ocr:
        onscreen_text, onscreen_raw, ocr_status = _load_cached_onscreen(conn, video_id)

    if skip_ocr:
        if not onscreen_text:
            ocr_status = "skipped_no_cache"
    elif force_ocr or not onscreen_text:
        ocr_triggered = True
        onscreen_text, onscreen_raw, ocr_status = _run_ocr(
            canonical, video_id, cache_dir, max_frames
        )

    combined, priority = build_visual_merged(
        sticker_overlay, browser_text, onscreen_text
    )

    likes = api_fields.get("like_count", 0)
    print(
        f"  video_id={video_id} @{username} likes={likes}\n"
        f"    API: {api_status} | browser: {browser_status} | OCR: {ocr_status}"
        f" (triggered={ocr_triggered})\n"
        f"    lens  caption={len(caption)} vtt={len(voice_to_text)} "
        f"api_sticker={len(sticker_overlay)} browser={len(browser_text)} "
        f"ocr={len(onscreen_text)} combined={len(combined)}"
    )

    return {
        "video_id": video_id,
        "eval_url": eval_url,
        "video_url": video_url,
        "username": api_fields.get("username") or username,
        "posted_at": api_fields.get("posted_at") or "",
        "hashtags": api_fields.get("hashtags") or "",
        "like_count": likes,
        "share_count": api_fields.get("share_count", 0),
        "comment_count": api_fields.get("comment_count", 0),
        "save_count": api_fields.get("save_count", 0),
        "duration_seconds": api_fields.get("duration_seconds", 0),
        "caption": caption,
        "voice_to_text": voice_to_text,
        "sticker_overlay_text": sticker_overlay,
        "browser_ocr_text": browser_text,
        "onscreen_text": onscreen_text,
        "onscreen_text_raw": onscreen_raw,
        "visual_text_combined": combined,
        "visual_text_source_priority": priority,
        "api_status": api_status,
        "browser_status": browser_status,
        "ocr_status": ocr_status,
        "ocr_triggered": ocr_triggered,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export unified text-layer comparison for 6 eval videos"
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--output", default=OUTPUT_CSV)
    parser.add_argument("--cache-dir", default="data/ocr_eval_cache")
    parser.add_argument("--max-frames", type=int, default=40)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run EasyOCR even if onscreen_text already in DB/batch JSON",
    )
    parser.add_argument(
        "--skip-ocr",
        action="store_true",
        help="Never run OCR; still load cached onscreen_text from DB/batch/frames",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    os.makedirs("data", exist_ok=True)

    cfg = load_config(args.config)
    auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"])
    conn = get_connection(cfg.paths["database"])

    rows: List[Dict[str, Any]] = []
    print(f"Processing {len(EVAL_URLS)} eval URLs...\n")

    for url in EVAL_URLS:
        print(f"URL: {url}")
        row = process_url(
            url,
            client=client,
            conn=conn,
            cache_dir=args.cache_dir,
            max_frames=args.max_frames,
            force_ocr=args.force,
            skip_ocr=args.skip_ocr,
        )
        rows.append(row)
        print()

    conn.close()

    with open(args.output, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {args.output} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
