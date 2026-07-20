"""Collect multi-source text samples for the fixed OCR evaluation URLs.

Sources per video:
  - Research API: full video row including voice_to_text (spoken captions)
  - Web hydration: stickersOnItem.stickerText (TikTok-stored on-screen overlays)
  - Web hydration: WebVTT closed captions (video.claInfo / subtitleInfos URLs)
  - yt-dlp: video id, username, caption fallback
  - Tesseract frame OCR (optional): onscreen_text from downloaded MP4

Usage (from project root):
    source venv/bin/activate
    pip install -r requirements-ocr.txt   # for --with-tesseract
    python scripts/collect_ocr_eval_samples.py
    python scripts/collect_ocr_eval_samples.py --api-only
    python scripts/collect_ocr_eval_samples.py --no-api --no-tesseract
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok import auth
from tiktok.api.client import TikTokClient
from tiktok.api.download import extract_video_metadata
from tiktok.api.videos import fetch_video_by_id
from tiktok.config import load_config
from tiktok.logging_setup import setup_logging
from tiktok.web.metadata import collect_web_video_metadata

logger = logging.getLogger(__name__)

EVAL_SAMPLES: List[Dict[str, str]] = [
    {
        "url": "https://www.tiktok.com/t/ZP8gL1VxH/",
        "expected_username": "harryjsisson",
        "notes": "both on-screen text and closed captions",
    },
    {
        "url": "https://www.tiktok.com/t/ZP8g8vtWu/",
        "expected_username": "jaysworld411",
        "notes": "on-screen text, closed captions, and twitter screenshots",
    },
    {
        "url": "https://www.tiktok.com/t/ZP8g8wJBr/",
        "expected_username": "joeycontino2",
        "notes": "on-screen text, closed captions, and green screen of twitter",
    },
    {
        "url": "https://www.tiktok.com/t/ZP8g8sS7p/",
        "expected_username": "cnn",
        "notes": "on-screen text edited outside TikTok, closed captions, and tweets",
    },
    {
        "url": "https://www.tiktok.com/t/ZP8g8W5XY/",
        "expected_username": "simpleblacktheory",
        "notes": "stitch; closed captions, on-screen text, and screenshots",
    },
    {
        "url": "https://www.tiktok.com/t/ZP8g8gkTK/",
        "expected_username": "pauletteonthemic",
        "notes": "screenshots and music/lyrics only, no other on-screen text",
    },
]


def _tesseract_ocr(url: str, video_id: str, cache_dir: str, max_frames: int) -> Dict[str, Any]:
    from tiktok.api.download import download_video_file
    from tiktok.ocr.pipeline import extract_onscreen_text

    out: Dict[str, Any] = {"onscreen_text": "", "error": None}
    path = download_video_file(url, video_id, cache_dir)
    if not path:
        out["error"] = "video_download_failed"
        return out
    try:
        result = extract_onscreen_text(path, video_id, max_frames=max_frames)
        out["onscreen_text"] = result.onscreen_text
        out["onscreen_text_raw"] = result.onscreen_text_raw
        out["frames_sampled"] = result.frames_sampled
        out["mean_confidence"] = result.mean_confidence_overall
    except Exception as e:
        out["error"] = str(e)
    return out


def collect_one(
    sample: Dict[str, str],
    *,
    client: Optional[TikTokClient],
    with_api: bool,
    with_web: bool,
    with_ytdlp_resolve: bool,
    with_tesseract: bool,
    cache_dir: str,
    max_frames: int,
) -> Dict[str, Any]:
    url = sample["url"]
    row: Dict[str, Any] = {
        "eval_url": url,
        "expected_username": sample["expected_username"],
        "eval_notes": sample["notes"],
    }

    web: Dict[str, Any] = {}
    if with_web:
        logger.info("Web metadata: %s", url)
        try:
            web = collect_web_video_metadata(url)
        except Exception as e:
            logger.exception("Web fetch failed for %s", url)
            web = {"url": url, "error": str(e)}
    row["web"] = web if with_web else None

    ytdlp: Dict[str, Any] = {}
    if with_ytdlp_resolve:
        info = extract_video_metadata(url)
        if info:
            ytdlp = {
                "video_id": str(info.get("id") or ""),
                "username": info.get("uploader") or info.get("channel") or "",
                "caption": (info.get("description") or info.get("title") or "")[:4000],
                "webpage_url": info.get("webpage_url") or info.get("url") or url,
            }
        else:
            ytdlp = {"error": "yt_dlp_metadata_failed"}
        row["ytdlp"] = ytdlp

    video_id = (
        sample.get("video_id")
        or (web.get("video_id") if with_web else "")
        or ytdlp.get("video_id")
        or ""
    ).strip()
    username = (
        sample.get("expected_username")
        or (web.get("username") if with_web else "")
        or ytdlp.get("username")
        or ""
    ).strip()
    row["video_id"] = video_id
    row["username"] = username
    row["canonical_url"] = (
        (web.get("canonical_url") if with_web else None)
        or ytdlp.get("webpage_url")
        or (f"https://www.tiktok.com/@{username}/video/{video_id}" if username and video_id else url)
    )

    row["caption"] = (web.get("caption") if with_web else "") or ytdlp.get("caption") or ""
    row["web_sticker_text"] = (web.get("web_sticker_text") or "") if with_web else ""
    row["web_closed_captions"] = (web.get("web_closed_captions") or "") if with_web else ""

    row["api"] = None
    row["api_voice_to_text"] = ""
    row["api_caption"] = ""
    if with_api and client and video_id and username:
        try:
            api_row = fetch_video_by_id(client, username, video_id)
            row["api"] = api_row
            if api_row:
                row["api_voice_to_text"] = api_row.get("voice_to_text") or ""
                row["api_caption"] = api_row.get("caption") or ""
            else:
                row["api_error"] = "video_not_found_in_research_api"
        except Exception as e:
            row["api_error"] = str(e)

    row["tesseract_onscreen_text"] = ""
    row["tesseract"] = None
    if with_tesseract and video_id:
        canonical = row["canonical_url"]
        ocr = _tesseract_ocr(canonical, video_id, cache_dir, max_frames)
        row["tesseract"] = ocr
        row["tesseract_onscreen_text"] = ocr.get("onscreen_text") or ""

    row["sources_present"] = {
        "api_voice_to_text": bool((row.get("api_voice_to_text") or "").strip()),
        "api_record": row.get("api") is not None,
    }
    if with_web:
        row["sources_present"]["web_sticker_text"] = bool(row["web_sticker_text"].strip())
        row["sources_present"]["web_closed_captions"] = bool(
            row["web_closed_captions"].strip()
        )
    if with_tesseract:
        row["sources_present"]["tesseract_onscreen_text"] = bool(
            (row.get("tesseract_onscreen_text") or "").strip()
        )
    return row


def _flat_row(full: Dict[str, Any], *, api_only: bool = False) -> Dict[str, Any]:
    sp = full.get("sources_present") or {}
    api = full.get("api") or {}
    if api_only:
        return {
            "video_id": full.get("video_id"),
            "username": full.get("username"),
            "video_url": api.get("video_url") or full.get("canonical_url"),
            "eval_notes": full.get("eval_notes"),
            "caption": api.get("caption") or "",
            "voice_to_text": api.get("voice_to_text") or "",
            "sticker_overlay_text": api.get("sticker_overlay_text") or "",
            "hashtags": api.get("hashtags") or "",
            "like_count": api.get("like_count"),
            "share_count": api.get("share_count"),
            "comment_count": api.get("comment_count"),
            "save_count": api.get("save_count"),
            "duration_seconds": api.get("duration_seconds"),
            "posted_at": api.get("posted_at"),
            "create_time": api.get("create_time"),
            "has_voice_to_text": sp.get("api_voice_to_text"),
            "has_sticker_overlay_text": bool((api.get("sticker_overlay_text") or "").strip()),
            "api_error": full.get("api_error"),
        }
    tess = full.get("tesseract") or {}
    return {
        "video_id": full.get("video_id"),
        "username": full.get("username"),
        "canonical_url": full.get("canonical_url"),
        "eval_notes": full.get("eval_notes"),
        "caption": full.get("caption") or "",
        "api_caption": full.get("api_caption") or "",
        "web_sticker_text": full.get("web_sticker_text") or "",
        "web_closed_captions": full.get("web_closed_captions") or "",
        "api_voice_to_text": full.get("api_voice_to_text") or "",
        "tesseract_onscreen_text": full.get("tesseract_onscreen_text") or "",
        "api_like_count": api.get("like_count"),
        "api_view_count": api.get("view_count") if "view_count" in api else None,
        "api_duration_seconds": api.get("duration_seconds"),
        "tesseract_frames_sampled": tess.get("frames_sampled"),
        "tesseract_mean_confidence": tess.get("mean_confidence"),
        "has_web_sticker_text": sp.get("web_sticker_text"),
        "has_web_closed_captions": sp.get("web_closed_captions"),
        "has_api_voice_to_text": sp.get("api_voice_to_text"),
        "has_api_record": sp.get("api_record"),
        "has_tesseract": sp.get("tesseract_onscreen_text"),
        "api_error": full.get("api_error"),
        "tesseract_error": (tess.get("error") if tess else None),
        "web_error": (full.get("web") or {}).get("error") if full.get("web") else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect OCR eval sample metadata")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--with-api",
        action="store_true",
        default=True,
        help="Fetch Research API record (default: on)",
    )
    parser.add_argument("--no-api", action="store_true", help="Skip Research API")
    parser.add_argument(
        "--api-only",
        action="store_true",
        help="Research API only (no web scrape, no Tesseract; yt-dlp used only to resolve short URLs)",
    )
    parser.add_argument(
        "--with-tesseract",
        action="store_true",
        default=True,
        help="Run Tesseract frame OCR (default: on)",
    )
    parser.add_argument("--no-tesseract", action="store_true", help="Skip Tesseract OCR")
    parser.add_argument("--max-frames", type=int, default=80)
    parser.add_argument("--cache-dir", default="data/ocr_eval_cache")
    parser.add_argument("--out-dir", default="data/ocr_eval_samples")
    args = parser.parse_args()

    api_only = args.api_only
    with_api = (args.with_api and not args.no_api) or api_only
    with_web = not api_only
    with_ytdlp_resolve = not api_only or True  # need video_id from short URL unless pre-set
    with_tesseract = (args.with_tesseract and not args.no_tesseract) and not api_only

    if api_only:
        with_ytdlp_resolve = True

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    os.makedirs(args.out_dir, exist_ok=True)

    if with_tesseract and not shutil.which("tesseract"):
        logger.warning(
            "tesseract not on PATH — skipping frame OCR. "
            "Install: brew install tesseract (macOS) or apt install tesseract-ocr"
        )
        with_tesseract = False

    cfg = load_config(args.config)
    client = None
    if with_api:
        auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)
        client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"])

    records: List[Dict[str, Any]] = []
    for sample in EVAL_SAMPLES:
        if api_only:
            logger.info("Research API: %s (@%s)", sample["url"], sample["expected_username"])
        rec = collect_one(
            sample,
            client=client,
            with_api=with_api,
            with_web=with_web,
            with_ytdlp_resolve=with_ytdlp_resolve,
            with_tesseract=with_tesseract,
            cache_dir=args.cache_dir,
            max_frames=args.max_frames,
        )
        records.append(rec)
        vid = rec.get("video_id") or "unknown"
        per_path = os.path.join(args.out_dir, f"{vid}.json")
        with open(per_path, "w", encoding="utf-8") as pf:
            json.dump(rec, pf, indent=2, ensure_ascii=False)
        if api_only:
            logger.info(
                "API %s (@%s): caption=%s chars, voice_to_text=%s chars",
                vid,
                rec.get("username"),
                len((rec.get("api") or {}).get("caption") or ""),
                len(rec.get("api_voice_to_text") or ""),
            )
        else:
            logger.info(
                "Collected %s (@%s): api_vtt=%s web_sticker=%s web_vtt=%s tesseract=%s",
                vid,
                rec.get("username"),
                len(rec.get("api_voice_to_text") or ""),
                len(rec.get("web_sticker_text") or ""),
                len(rec.get("web_closed_captions") or ""),
                len(rec.get("tesseract_onscreen_text") or ""),
            )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    prefix = "ocr_eval_api_only" if api_only else "ocr_eval_samples"
    bundle = os.path.join("data", f"{prefix}_{stamp}.json")
    summary_csv = os.path.join("data", f"{prefix}_{stamp}.csv")
    full_csv = os.path.join("data", f"{prefix}_{stamp}_full.csv")

    with open(bundle, "w", encoding="utf-8") as bf:
        json.dump(records, bf, indent=2, ensure_ascii=False)

    flat = [_flat_row(r, api_only=api_only) for r in records]
    if flat:
        with open(summary_csv, "w", encoding="utf-8", newline="") as cf:
            w = csv.DictWriter(cf, fieldnames=list(flat[0].keys()))
            w.writeheader()
            w.writerows(flat)
        with open(full_csv, "w", encoding="utf-8", newline="") as cf:
            w = csv.DictWriter(cf, fieldnames=list(flat[0].keys()), extrasaction="ignore")
            w.writeheader()
            w.writerows(flat)

    logger.info(
        "Wrote %s, %s, %s and per-video JSON in %s/",
        bundle,
        summary_csv,
        full_csv,
        args.out_dir,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
