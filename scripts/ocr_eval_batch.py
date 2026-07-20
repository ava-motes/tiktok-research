"""Small-batch on-screen text (OCR) evaluation for fixed TikTok URLs.

Downloads each video once, samples ~1 frame/sec, runs EasyOCR, deduplicates
lines, and writes a table plus optional DB updates.

Prerequisites (Python only — no system Tesseract):
    pip install -r requirements-ocr.txt

Usage (from project root):
    source venv/bin/activate
    pip install -r requirements-ocr.txt
    python scripts/ocr_eval_batch.py
    python scripts/ocr_eval_batch.py --max-frames 120 --no-db-update
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.db import get_connection, update_video_onscreen_text
from tiktok.logging_setup import setup_logging
from tiktok.api.download import download_video_file, extract_video_metadata
from tiktok.ocr.pipeline import extract_onscreen_text

logger = logging.getLogger(__name__)

# Evaluation set (short links; yt-dlp resolves to video IDs)
EVAL_URLS: List[str] = [
    "https://www.tiktok.com/t/ZP8gL1VxH/",
    "https://www.tiktok.com/t/ZP8g8vtWu/",
    "https://www.tiktok.com/t/ZP8g8wJBr/",
    "https://www.tiktok.com/t/ZP8g8sS7p/",
    "https://www.tiktok.com/t/ZP8g8W5XY/",
    "https://www.tiktok.com/t/ZP8g8gkTK/",
]


def _row_from_db(conn, video_id: str) -> Optional[Dict[str, Any]]:
    r = conn.execute(
        """SELECT v.caption, v.voice_to_text,
                  COALESCE(v.sticker_overlay_text, '') AS sticker_overlay_text,
                  COALESCE(t.transcript_text, '') AS whisper_transcript,
                  t.transcript_source AS whisper_source
           FROM videos v
           LEFT JOIN transcripts t ON v.video_id = t.video_id
           WHERE v.video_id = ?""",
        (video_id,),
    ).fetchone()
    if not r:
        return None
    return {
        "caption": r["caption"] or "",
        "voice_to_text": r["voice_to_text"] or "",
        "sticker_overlay_text": r["sticker_overlay_text"] or "",
        "whisper_transcript": r["whisper_transcript"] or "",
        "whisper_source": r["whisper_source"],
    }


def _source_flags(db_row: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """Explicit provenance so eval rows are not silently mixed."""
    if db_row:
        cap_src = "api"
        vt_src = "api" if (db_row.get("voice_to_text") or "").strip() else "missing"
        w = (db_row.get("whisper_transcript") or "").strip()
        if w:
            ws = (db_row.get("whisper_source") or "unknown").strip()
            wh_src = ws if ws else "unknown"
        else:
            wh_src = "missing"
    else:
        cap_src = "yt_dlp_fallback"
        vt_src = "missing"
        wh_src = "missing"
    return {
        "caption_source": cap_src,
        "voice_to_text_source": vt_src,
        "whisper_transcript_source": wh_src,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="OCR eval batch for fixed TikTok URLs")
    parser.add_argument("--config", default="config.yaml", help="Config path (for DB location)")
    parser.add_argument(
        "--cache-dir",
        default="data/ocr_eval_cache",
        help="Where to store downloaded videos",
    )
    parser.add_argument(
        "--max-frames",
        type=int,
        default=100,
        help="Max frames to OCR; caps long videos (default 100)",
    )
    parser.add_argument(
        "--sample-every-seconds",
        type=float,
        default=1.0,
        help="Wall-clock spacing between sampled frames: 1.0≈1 Hz, 0.5≈2 Hz, 2.0≈0.5 Hz",
    )
    parser.add_argument(
        "--no-db-update",
        action="store_true",
        help="Do not write onscreen_text to SQLite (still export CSV/JSON)",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    try:
        import easyocr  # noqa: F401
    except ImportError:
        logger.error(
            "EasyOCR not installed. Run: pip install -r requirements-ocr.txt"
        )
        return 1

    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])
    cache_dir = args.cache_dir

    rows_out: List[Dict[str, Any]] = []
    t0 = datetime.now(timezone.utc)

    for url in EVAL_URLS:
        logger.info("Processing %s", url)
        info = extract_video_metadata(url)
        if not info:
            rows_out.append(
                {
                    "video_id": "",
                    "url": url,
                    "onscreen_text": "",
                    "caption": "",
                    "voice_to_text": "",
                    "whisper_transcript": "",
                    "error": "metadata_extract_failed",
                }
            )
            continue

        video_id = str(info.get("id") or "")
        canonical = (
            info.get("webpage_url")
            or info.get("original_url")
            or info.get("url")
            or url
        )

        vid_path = download_video_file(canonical, video_id, cache_dir)
        if not vid_path:
            rows_out.append(
                {
                    "video_id": video_id,
                    "url": canonical,
                    "onscreen_text": "",
                    "caption": (info.get("description") or info.get("title") or "")[:2000],
                    "voice_to_text": "",
                    "whisper_transcript": "",
                    "error": "video_download_failed",
                }
            )
            continue

        try:
            ocr = extract_onscreen_text(
                vid_path,
                video_id,
                max_frames=args.max_frames,
                seconds_between_samples=args.sample_every_seconds,
            )
        except Exception as e:
            logger.exception("OCR pipeline failed for %s", video_id)
            rows_out.append(
                {
                    "video_id": video_id,
                    "url": canonical,
                    "onscreen_text": "",
                    "caption": "",
                    "voice_to_text": "",
                    "whisper_transcript": "",
                    "error": str(e),
                }
            )
            continue

        db_row = _row_from_db(conn, video_id)
        yt_fallback_caption = (info.get("description") or info.get("title") or "") or ""
        sticker_overlay = ""
        if db_row:
            caption = db_row["caption"]
            vtt = db_row["voice_to_text"]
            whisper = db_row["whisper_transcript"]
            sticker_overlay = db_row.get("sticker_overlay_text") or ""
        else:
            caption = yt_fallback_caption
            vtt = ""
            whisper = ""
            logger.info(
                "No DB row for video_id=%s — using yt-dlp caption only",
                video_id,
            )

        flags = _source_flags(db_row)

        meta = {
            "mean_confidence": ocr.mean_confidence_overall,
            "frames_sampled": ocr.frames_sampled,
            "engine": "easyocr",
            "eval_started_at": t0.isoformat(),
            "video_fps": ocr.video_fps,
            "seconds_between_samples": ocr.seconds_between_samples,
            "onscreen_text_raw_char_count": len(ocr.onscreen_text_raw or ""),
            "caption_source": flags["caption_source"],
            "whisper_transcript_source": flags["whisper_transcript_source"],
        }
        detail_path = os.path.join(
            "data",
            f"ocr_eval_frames_{video_id}.json",
        )
        with open(detail_path, "w", encoding="utf-8") as df:
            json.dump(
                {
                    "video_id": video_id,
                    "video_fps": ocr.video_fps,
                    "seconds_between_samples": ocr.seconds_between_samples,
                    "onscreen_text_deduped": ocr.onscreen_text,
                    "onscreen_text_raw": ocr.onscreen_text_raw,
                    "mean_confidence_overall": ocr.mean_confidence_overall,
                    "frames": [
                        {
                            "frame_index": fr.frame_index,
                            "timestamp_sec": fr.timestamp_sec,
                            "mean_confidence": fr.mean_confidence,
                            "lines": fr.text_lines,
                        }
                        for fr in ocr.frame_results
                    ],
                },
                df,
                indent=2,
                ensure_ascii=False,
            )

        if not args.no_db_update:
            n = update_video_onscreen_text(conn, video_id, ocr.onscreen_text, meta)
            if n:
                conn.commit()
                logger.info("Updated DB onscreen_text for video_id=%s", video_id)
            else:
                logger.info("Skipped DB update (no videos row for %s)", video_id)

        logger.info(
            "OCR @%s: frames=%s mean_conf=%s chars=%s",
            video_id,
            ocr.frames_sampled,
            f"{ocr.mean_confidence_overall:.1f}" if ocr.mean_confidence_overall else "n/a",
            len(ocr.onscreen_text or ""),
        )

        rows_out.append(
            {
                "video_id": video_id,
                "url": canonical,
                "onscreen_text": ocr.onscreen_text,
                "onscreen_text_raw": ocr.onscreen_text_raw,
                "caption": caption,
                "voice_to_text": vtt,
                "sticker_overlay_text": sticker_overlay,
                "whisper_transcript": whisper,
                **flags,
                "ocr_mean_confidence": ocr.mean_confidence_overall,
                "frames_sampled": ocr.frames_sampled,
                "sample_every_seconds": ocr.seconds_between_samples,
                "frame_detail_json": detail_path,
            }
        )

    conn.close()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join("data", f"ocr_eval_batch_{stamp}.csv")
    json_path = os.path.join("data", f"ocr_eval_batch_{stamp}.json")

    os.makedirs("data", exist_ok=True)

    try:
        import pandas as pd

        df = pd.DataFrame(rows_out)
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient="records", indent=2, force_ascii=False)
        logger.info("Wrote %s and %s (%s rows)", csv_path, json_path, len(df))
    except ImportError:
        with open(json_path, "w", encoding="utf-8") as jf:
            json.dump(rows_out, jf, indent=2, ensure_ascii=False)
        logger.info("pandas not installed; wrote %s only (%s rows)", json_path, len(rows_out))
        import csv

        if rows_out:
            with open(csv_path, "w", encoding="utf-8", newline="") as cf:
                w = csv.DictWriter(cf, fieldnames=list(rows_out[0].keys()))
                w.writeheader()
                w.writerows(rows_out)
            logger.info("Wrote %s via csv module", csv_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
