"""Multimodal visual-text enrichment for ingested TikTok videos.

Architecture (API-first, OCR fallback only when needed):

    Research API sticker_overlay_text
        → browser hydration (stickersOnItem[].stickerText)
        → EasyOCR fallback (sparse combined text only)
        → normalize + merge → visual_text_combined

Raw modality columns are preserved separately. EasyOCR is optional (--web-only).

Usage (from project root):
    python scripts/enrich_videos_with_ocr.py --group sample --limit 20
    python scripts/enrich_videos_with_ocr.py --video-id 7620288673065553183
    python scripts/enrich_videos_with_ocr.py --group sample --web-only
    python scripts/enrich_videos_with_ocr.py --group sample --merge-only
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.api.download import download_video_file
from tiktok.config import load_config
from tiktok.db import (
    get_connection,
    update_video_browser_ocr_text,
    update_video_onscreen_text,
    update_video_visual_text,
)
from tiktok.logging_setup import setup_logging
from tiktok.text.normalize import (
    DEFAULT_VISUAL_TEXT_THRESHOLD,
    has_sufficient_visual_text,
    line_overlap_ratio,
    merge_visual_text_sources,
    needs_easyocr_fallback,
)
from tiktok.web.metadata import fetch_web_onscreen_text

logger = logging.getLogger(__name__)

_DEFAULT_MAX_FRAMES = 100
_DEFAULT_SAMPLE_SECONDS = 1.0
_DEFAULT_CACHE_DIR = "data/ocr_cache"

_VIDEO_COLS = """video_id, username, video_url, caption,
    COALESCE(sticker_overlay_text, '') AS sticker_overlay_text,
    COALESCE(browser_ocr_text, '') AS browser_ocr_text,
    COALESCE(onscreen_text, '') AS onscreen_text,
    COALESCE(visual_text_combined, '') AS visual_text_combined,
    COALESCE(voice_to_text, '') AS voice_to_text"""


def _merge_and_persist(
    conn,
    video_id: str,
    *,
    sticker: str,
    browser: str,
    onscreen: str,
) -> Dict[str, Any]:
    merged = merge_visual_text_sources(
        sticker_overlay_text=sticker,
        browser_ocr_text=browser,
        onscreen_text=onscreen,
    )
    n = update_video_visual_text(
        conn,
        video_id,
        merged["visual_text_combined"],
        merged["visual_text_sources"],
    )
    return {**merged, "updated": n}


def should_process_row(
    row: Dict[str, Any],
    *,
    force: bool,
    merge_only: bool,
    explicit_video_id: bool,
) -> bool:
    if force:
        return True
    if explicit_video_id:
        return True
    if merge_only:
        return True
    sticker = row.get("sticker_overlay_text") or ""
    browser = row.get("browser_ocr_text") or ""
    combined = row.get("visual_text_combined") or ""
    if not has_sufficient_visual_text(combined):
        return True
    if not has_sufficient_visual_text(sticker) and not has_sufficient_visual_text(browser):
        return True
    return False


def _fetch_candidates(
    conn,
    *,
    handles: Optional[List[str]],
    video_id: Optional[str],
    limit: Optional[int],
) -> List[Dict[str, Any]]:
    if video_id:
        rows = conn.execute(
            f"SELECT {_VIDEO_COLS} FROM videos WHERE video_id = ?",
            (video_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    conditions: List[str] = []
    params: List[Any] = []
    if handles:
        placeholders = ",".join("?" for _ in handles)
        conditions.append(f"username IN ({placeholders})")
        params.extend(handles)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT {_VIDEO_COLS} FROM videos {where} ORDER BY create_time DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _try_browser_hydration(
    row: Dict[str, Any],
    *,
    db_path: str,
    started_at: str,
) -> Dict[str, Any]:
    video_id = row["video_id"]
    video_url = row.get("video_url") or ""
    username = row.get("username") or ""

    if not video_url:
        return {"status": "skipped", "reason": "missing_url", "browser_text": ""}

    web = fetch_web_onscreen_text(video_url)
    text = (web.get("text") or "").strip()
    if web.get("error"):
        logger.info("@%s — %s — hydration: %s", username, video_id, web["error"])
        return {"status": "skipped", "reason": web["error"], "browser_text": ""}

    if not text:
        return {"status": "skipped", "reason": "hydration_empty", "browser_text": ""}

    conn = get_connection(db_path)
    try:
        update_video_browser_ocr_text(conn, video_id, text)
        conn.commit()
        logger.info("@%s — %s — browser OCR text (%s chars)", username, video_id, len(text))
        return {"status": "success", "browser_text": text, "tier": "browser"}
    finally:
        conn.close()


def _try_easyocr(
    row: Dict[str, Any],
    *,
    db_path: str,
    cache_dir: str,
    max_frames: int,
    sample_every_seconds: float,
    started_at: str,
) -> Dict[str, Any]:
    from tiktok.ocr.pipeline import extract_onscreen_text

    video_id = row["video_id"]
    video_url = row.get("video_url") or ""
    username = row.get("username") or ""

    if not video_url:
        return {"status": "failed", "reason": "missing_url", "onscreen_text": ""}

    vid_path = download_video_file(video_url, video_id, cache_dir)
    if not vid_path:
        logger.warning("@%s — %s — download failed", username, video_id)
        return {"status": "failed", "reason": "download_failed", "onscreen_text": ""}

    try:
        ocr = extract_onscreen_text(
            vid_path,
            video_id,
            max_frames=max_frames,
            seconds_between_samples=sample_every_seconds,
        )
    except Exception as e:
        logger.exception("@%s — %s — EasyOCR failed: %s", username, video_id, e)
        return {"status": "failed", "reason": str(e), "onscreen_text": ""}

    text = ocr.onscreen_text or ""
    meta = {
        "source": "easyocr",
        "engine": "easyocr",
        "mean_confidence": ocr.mean_confidence_overall,
        "frames_sampled": ocr.frames_sampled,
        "video_fps": ocr.video_fps,
        "seconds_between_samples": ocr.seconds_between_samples,
        "enriched_at": datetime.now(timezone.utc).isoformat(),
        "batch_started_at": started_at,
    }
    conn = get_connection(db_path)
    try:
        update_video_onscreen_text(conn, video_id, text, meta)
        conn.commit()
        logger.info(
            "@%s — %s — EasyOCR (%s chars)",
            username,
            video_id,
            len(text),
        )
        return {"status": "success", "onscreen_text": text, "tier": "easyocr"}
    finally:
        conn.close()


def _process_one(
    row: Dict[str, Any],
    *,
    db_path: str,
    cache_dir: str,
    max_frames: int,
    sample_every_seconds: float,
    started_at: str,
    merge_only: bool,
    web_only: bool,
    easyocr_only: bool,
    threshold: int,
) -> Dict[str, Any]:
    video_id = row["video_id"]
    sticker = (row.get("sticker_overlay_text") or "").strip()
    browser = (row.get("browser_ocr_text") or "").strip()
    onscreen = (row.get("onscreen_text") or "").strip()

    tiers_used: List[str] = []

    need_browser = (
        not easyocr_only
        and not has_sufficient_visual_text(sticker)
        and not has_sufficient_visual_text(browser)
    )
    if not merge_only and need_browser:
        br = _try_browser_hydration(row, db_path=db_path, started_at=started_at)
        if br.get("status") == "success":
            browser = br["browser_text"]
            tiers_used.append("browser")

    conn = get_connection(db_path)
    try:
        merged = _merge_and_persist(
            conn,
            video_id,
            sticker=sticker,
            browser=browser,
            onscreen=onscreen,
        )
        conn.commit()
    finally:
        conn.close()

    combined = merged["visual_text_combined"]
    run_easyocr = not merge_only and not web_only and (
        easyocr_only or needs_easyocr_fallback(combined, threshold)
    )

    if run_easyocr:
        eo = _try_easyocr(
            row,
            db_path=db_path,
            cache_dir=cache_dir,
            max_frames=max_frames,
            sample_every_seconds=sample_every_seconds,
            started_at=started_at,
        )
        if eo.get("status") == "success":
            onscreen = (eo.get("onscreen_text") or "").strip()
            tiers_used.append("easyocr")
            conn = get_connection(db_path)
            try:
                merged = _merge_and_persist(
                    conn,
                    video_id,
                    sticker=sticker,
                    browser=browser,
                    onscreen=onscreen,
                )
                conn.commit()
                combined = merged["visual_text_combined"]
            finally:
                conn.close()

    sources = merged.get("visual_text_sources") or {}
    return {
        "status": "success",
        "tiers_used": tiers_used,
        "combined_len": len(combined),
        "primary_source": sources.get("primary_source"),
        "has_api_sticker": has_sufficient_visual_text(sticker),
        "has_browser": has_sufficient_visual_text(browser),
        "has_easyocr": has_sufficient_visual_text(onscreen),
        "used_easyocr": "easyocr" in tiers_used,
        "api_browser_overlap": line_overlap_ratio(sticker, browser),
        "api_easyocr_overlap": line_overlap_ratio(sticker, onscreen),
    }


def _print_report(stats: Dict[str, Any], elapsed: float) -> None:
    scanned = stats["scanned"]
    processed = stats["processed"]
    print("\n--- Multimodal visual text enrichment ---")
    print(f"Videos scanned:              {scanned}")
    print(f"Videos processed:            {processed}")
    if processed == 0:
        print("---\n")
        return
    print(f"With API sticker text:       {stats['with_api_sticker']}")
    print(f"With browser hydration text: {stats['with_browser']}")
    print(f"EasyOCR fallback run:        {stats['easyocr_runs']}")
    print(f"With visual_text_combined:   {stats['with_combined']}")
    if processed:
        print(
            f"OCR fallback rate:           {stats['easyocr_runs'] / processed:.1%}"
        )
    if stats["overlap_pairs"] > 0:
        print(
            f"Avg API↔browser line overlap: {stats['overlap_sum'] / stats['overlap_pairs']:.2f}"
        )
    print(f"Avg combined chars:          {stats['avg_combined']:.0f}")
    print(f"Elapsed:                     {elapsed:.1f}s")
    print("---\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Multimodal visual text enrichment (API → hydration → EasyOCR)",
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--group", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--video-id", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="Only merge existing raw layers into visual_text_combined (no fetches)",
    )
    parser.add_argument("--web-only", action="store_true")
    parser.add_argument("--easyocr-only", action="store_true")
    parser.add_argument("--cache-dir", default=_DEFAULT_CACHE_DIR)
    parser.add_argument("--max-frames", type=int, default=_DEFAULT_MAX_FRAMES)
    parser.add_argument("--sample-every-seconds", type=float, default=_DEFAULT_SAMPLE_SECONDS)
    parser.add_argument(
        "--visual-threshold",
        type=int,
        default=DEFAULT_VISUAL_TEXT_THRESHOLD,
        help="Min chars in visual_text_combined before skipping EasyOCR",
    )
    args = parser.parse_args()

    if args.web_only and args.easyocr_only:
        parser.error("Use at most one of --web-only and --easyocr-only")
    if args.merge_only and (args.web_only or args.easyocr_only):
        parser.error("--merge-only cannot combine with --web-only or --easyocr-only")

    setup_logging()
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    if not args.web_only and not args.merge_only:
        try:
            import easyocr  # noqa: F401
        except ImportError:
            logger.error(
                "EasyOCR not installed. pip install -r requirements-ocr.txt "
                "or use --web-only / --merge-only"
            )
            return 1

    cfg = load_config(args.config)
    db_path = cfg.paths["database"]

    handles: Optional[List[str]] = None
    if args.group:
        handles = cfg.get_handles(args.group)
    elif not args.video_id:
        group = cfg.default_group("pull_videos")
        handles = cfg.get_handles(group)

    conn = get_connection(db_path)
    candidates = _fetch_candidates(
        conn,
        handles=handles,
        video_id=args.video_id,
        limit=args.limit,
    )
    conn.close()

    explicit_id = bool(args.video_id)
    to_process = [
        r
        for r in candidates
        if should_process_row(
            r,
            force=args.force,
            merge_only=args.merge_only,
            explicit_video_id=explicit_id,
        )
    ]

    if not to_process and candidates:
        print(
            "\nNo videos selected (coverage OK). Use --force, --merge-only, or --video-id.\n"
        )
        return 0

    t0 = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat()
    stats: Dict[str, Any] = {
        "scanned": len(candidates),
        "processed": 0,
        "with_api_sticker": 0,
        "with_browser": 0,
        "with_combined": 0,
        "easyocr_runs": 0,
        "overlap_sum": 0.0,
        "overlap_pairs": 0,
        "combined_lens": [],
        "failures": 0,
    }

    for idx, row in enumerate(to_process, 1):
        logger.info(
            "[%s/%s] %s (@%s)",
            idx,
            len(to_process),
            row["video_id"],
            row.get("username"),
        )
        try:
            outcome = _process_one(
                row,
                db_path=db_path,
                cache_dir=args.cache_dir,
                max_frames=args.max_frames,
                sample_every_seconds=args.sample_every_seconds,
                started_at=started_at,
                merge_only=args.merge_only,
                web_only=args.web_only,
                easyocr_only=args.easyocr_only,
                threshold=args.visual_threshold,
            )
            stats["processed"] += 1
            if outcome.get("has_api_sticker"):
                stats["with_api_sticker"] += 1
            if outcome.get("has_browser"):
                stats["with_browser"] += 1
            if outcome.get("used_easyocr"):
                stats["easyocr_runs"] += 1
            if outcome.get("combined_len", 0) >= args.visual_threshold:
                stats["with_combined"] += 1
            stats["combined_lens"].append(outcome.get("combined_len", 0))
            if outcome.get("has_api_sticker") and outcome.get("has_browser"):
                stats["overlap_sum"] += outcome.get("api_browser_overlap", 0.0)
                stats["overlap_pairs"] += 1
        except Exception:
            logger.exception("Failed %s", row["video_id"])
            stats["failures"] += 1

    stats["avg_combined"] = (
        sum(stats["combined_lens"]) / len(stats["combined_lens"])
        if stats["combined_lens"]
        else 0.0
    )
    _print_report(stats, time.perf_counter() - t0)

    return 0 if stats["failures"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
