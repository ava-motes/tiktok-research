#!/usr/bin/env python3
"""Collect full enrichment for the six research validation TikTok URLs.

Run on comm-cme-p01 only:
    python scripts/collect_six_validation_videos.py

Captures: metadata, Whisper transcript, on-screen OCR (Vision or web),
closed captions (WebVTT), stickers, emojis — then exports CSV + JSON.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.api.download import extract_video_metadata
from tiktok.config import load_config
from tiktok.db import get_connection, insert_video, update_video_browser_ocr_text
from tiktok.enrichment.emoji_extract import extract_emoji_rows_for_video
from tiktok.enrichment.store import (
    ensure_enrichment_schema,
    insert_enrichment_log,
    replace_emoji_rows,
    replace_ocr_rows,
    upsert_transcript,
)
from tiktok.enrichment.temp_media import temporary_audio
from tiktok.enrichment.whisper_backend import transcribe_audio
from tiktok.enrichment.worker_log import WorkerTimer
from tiktok.logging_setup import setup_logging
from tiktok.web.metadata import (
    extract_onscreen_text_from_item,
    extract_sticker_texts,
    fetch_closed_captions,
    fetch_hydration_html,
    parse_item_struct,
)

logger = logging.getLogger(__name__)

CASES = [
    {
        "id": 1,
        "url": "https://www.tiktok.com/t/ZP8gL1VxH/",
        "account": "harryjsisson",
        "expect": "on-screen text + closed captions",
    },
    {
        "id": 2,
        "url": "https://www.tiktok.com/t/ZP8g8vtWu/",
        "account": "jaysworld411",
        "expect": "on-screen text + closed captions + twitter screenshots",
    },
    {
        "id": 3,
        "url": "https://www.tiktok.com/t/ZP8g8wJBr/",
        "account": "joeycontino2",
        "expect": "on-screen text + closed captions + green screen twitter",
    },
    {
        "id": 4,
        "url": "https://www.tiktok.com/t/ZP8g8sS7p/",
        "account": "cnn",
        "expect": "edited outside TikTok + closed captions + tweets",
    },
    {
        "id": 5,
        "url": "https://www.tiktok.com/t/ZP8g8W5XY/",
        "account": "simpleblacktheory",
        "expect": "stitch + closed captions + on-screen text + screenshots",
    },
    {
        "id": 6,
        "url": "https://www.tiktok.com/t/ZP8g8gkTK/",
        "account": "pauletteonthemic",
        "expect": "screenshots + music lyrics, little other overlay",
    },
]


def resolve(url: str, account: str) -> Dict[str, Any]:
    info = extract_video_metadata(url) or {}
    video_id = str(info.get("id") or "")
    username = (
        (info.get("uploader") or info.get("creator") or info.get("channel") or account)
        .lstrip("@")
    )
    webpage = info.get("webpage_url") or (
        f"https://www.tiktok.com/@{username}/video/{video_id}" if video_id else url
    )
    return {
        "video_id": video_id,
        "username": username,
        "webpage_url": webpage,
        "title": info.get("title") or "",
        "description": info.get("description") or "",
        "duration": info.get("duration"),
    }


def ensure_row(conn, meta: Dict[str, Any], account: str) -> None:
    vid = meta["video_id"]
    if not vid:
        return
    if conn.execute("SELECT 1 FROM videos WHERE video_id=?", (vid,)).fetchone():
        # refresh URL/caption if empty
        conn.execute(
            """UPDATE videos SET video_url=COALESCE(NULLIF(video_url,''), ?),
               caption=CASE WHEN caption IS NULL OR caption='' THEN ? ELSE caption END
               WHERE video_id=?""",
            (meta["webpage_url"], meta.get("description") or meta.get("title") or "", vid),
        )
        conn.commit()
        return
    insert_video(
        conn,
        {
            "video_id": vid,
            "username": meta.get("username") or account,
            "video_url": meta["webpage_url"],
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


def collect_web_layers(url: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "onscreen_text": "",
        "sticker_texts": [],
        "closed_captions": "",
        "caption_tracks": [],
        "error": None,
    }
    try:
        html = fetch_hydration_html(url)
        item = parse_item_struct(html)
        if not item:
            out["error"] = "hydration_item_struct_missing"
            return out
        stickers = extract_sticker_texts(item)
        out["sticker_texts"] = stickers
        out["onscreen_text"] = extract_onscreen_text_from_item(item)
        caps = fetch_closed_captions(item)
        out["closed_captions"] = caps.get("text") or ""
        out["caption_tracks"] = [
            {
                "language": t.get("language"),
                "format": t.get("format"),
                "char_count": t.get("char_count"),
                "text_preview": (t.get("text") or "")[:300],
                "error": t.get("error"),
            }
            for t in (caps.get("tracks") or [])
        ]
    except Exception as e:
        out["error"] = str(e)
    return out


def run_transcript(conn, video_id: str, video_url: str) -> Dict[str, Any]:
    with WorkerTimer("transcription", video_id) as timer:
        with temporary_audio(video_url, video_id) as audio_path:
            if not audio_path:
                timer.fail("download_failed")
                upsert_transcript(
                    conn, video_id=video_id, transcript="", status="error", error="download_failed"
                )
                insert_enrichment_log(conn, timer.to_result().to_dict())
                conn.commit()
                return {"status": "error", "error": "download_failed", "chars": 0}
            try:
                result = transcribe_audio(video_id, audio_path)
            except Exception as e:
                timer.fail(str(e))
                upsert_transcript(
                    conn, video_id=video_id, transcript="", status="error", error=str(e)[:500]
                )
                insert_enrichment_log(conn, timer.to_result().to_dict())
                conn.commit()
                return {"status": "error", "error": str(e), "chars": 0}
        upsert_transcript(
            conn,
            video_id=video_id,
            transcript=result.transcript,
            language=result.language,
            whisper_model=result.whisper_model,
            confidence=result.confidence,
            status="ok",
        )
        timer.success(chars=len(result.transcript or ""), model=result.whisper_model)
        insert_enrichment_log(conn, timer.to_result().to_dict())
        conn.commit()
        return {
            "status": "ok",
            "chars": len(result.transcript or ""),
            "transcript": result.transcript,
            "model": result.whisper_model,
            "language": result.language,
        }


def store_text_layers(conn, video_id: str, web: Dict[str, Any]) -> int:
    """Persist on-screen + closed captions into video_ocr (multiple sources)."""
    rows: List[Dict[str, Any]] = []
    if (web.get("onscreen_text") or "").strip():
        rows.append(
            {
                "frame_number": 0,
                "frame_timestamp": 0.0,
                "ocr_text": web["onscreen_text"].strip(),
                "confidence": None,
                "source": "browser_hydration",
            }
        )
    if (web.get("closed_captions") or "").strip():
        rows.append(
            {
                "frame_number": 0,
                "frame_timestamp": 0.0,
                "ocr_text": web["closed_captions"].strip(),
                "confidence": None,
                "source": "closed_captions",
            }
        )
    # Also try Vision if credentials present
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if creds and os.path.isfile(creds):
        try:
            from tiktok.enrichment.ocr_google import ocr_video_file
            from tiktok.enrichment.temp_media import temporary_video

            video_url = conn.execute(
                "SELECT video_url FROM videos WHERE video_id=?", (video_id,)
            ).fetchone()[0]
            with temporary_video(video_url, video_id) as path:
                if path:
                    vision_out = ocr_video_file(path, max_frames=12)
                    rows.extend(vision_out.get("rows") or [])
        except Exception as e:
            logger.warning("Vision OCR skipped for %s: %s", video_id, e)

    n = replace_ocr_rows(conn, video_id, rows)
    if web.get("onscreen_text"):
        update_video_browser_ocr_text(conn, video_id, web["onscreen_text"])
    conn.commit()
    return n


def main() -> int:
    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    cfg = load_config("config.yaml")
    conn = get_connection(cfg.paths["database"])
    ensure_enrichment_schema(conn)

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cases": [],
    }

    for case in CASES:
        logger.info("======== CASE %s @%s ========", case["id"], case["account"])
        entry: Dict[str, Any] = {"case": case}
        try:
            meta = resolve(case["url"], case["account"])
            entry["resolved"] = meta
            if not meta.get("video_id"):
                entry["error"] = "resolve_failed"
                report["cases"].append(entry)
                continue

            ensure_row(conn, meta, case["account"])
            video_id = meta["video_id"]
            video_url = meta["webpage_url"]

            # 1) Web layers (onscreen + closed captions)
            web = collect_web_layers(video_url)
            entry["web"] = {
                "onscreen_chars": len(web.get("onscreen_text") or ""),
                "onscreen_preview": (web.get("onscreen_text") or "")[:400],
                "closed_caption_chars": len(web.get("closed_captions") or ""),
                "closed_caption_preview": (web.get("closed_captions") or "")[:400],
                "sticker_count": len(web.get("sticker_texts") or []),
                "caption_tracks": web.get("caption_tracks"),
                "error": web.get("error"),
            }
            store_text_layers(conn, video_id, web)

            # 2) Whisper
            entry["transcript"] = run_transcript(conn, video_id, video_url)

            # 3) Emojis from all layers
            row = dict(
                conn.execute(
                    """SELECT video_id, username, caption, hashtags, voice_to_text, transcript,
                              sticker_overlay_text, browser_ocr_text, onscreen_text,
                              visual_text_combined FROM videos WHERE video_id=?""",
                    (video_id,),
                ).fetchone()
            )
            # Prefer enrichment transcript + OCR combined
            tr = conn.execute(
                "SELECT transcript FROM video_transcripts WHERE video_id=? AND status='ok'",
                (video_id,),
            ).fetchone()
            if tr:
                row["transcript"] = tr["transcript"]
            ocr_bits = [
                r["ocr_text"]
                for r in conn.execute(
                    "SELECT ocr_text FROM video_ocr WHERE video_id=?", (video_id,)
                ).fetchall()
            ]
            row["onscreen_text"] = "\n".join(ocr_bits)
            row["browser_ocr_text"] = web.get("onscreen_text") or row.get("browser_ocr_text")
            # Include closed captions in emoji scan via onscreen_text already
            emoji_rows = extract_emoji_rows_for_video(row)
            # Also scan closed captions explicitly
            from tiktok.enrichment.emoji_extract import extract_emoji_rows_from_text

            emoji_rows.extend(
                extract_emoji_rows_from_text(web.get("closed_captions") or "", "closed_captions")
            )
            emoji_rows.extend(
                extract_emoji_rows_from_text(
                    entry.get("transcript", {}).get("transcript") or "", "transcript"
                )
            )
            n_em = replace_emoji_rows(conn, video_id, emoji_rows)
            conn.commit()
            entry["emojis"] = {"count": n_em, "rows": emoji_rows[:30]}

            # Final snapshot
            ocr_all = [
                dict(r)
                for r in conn.execute(
                    """SELECT source, length(ocr_text) chars, substr(ocr_text,1,500) preview
                       FROM video_ocr WHERE video_id=?""",
                    (video_id,),
                ).fetchall()
            ]
            entry["ocr_layers"] = ocr_all
            entry["checks"] = {
                "has_transcript": (entry.get("transcript") or {}).get("status") == "ok",
                "has_onscreen": len(web.get("onscreen_text") or "") > 0,
                "has_closed_captions": len(web.get("closed_captions") or "") > 0,
                "has_emojis": n_em > 0,
            }
        except Exception as e:
            logger.exception("Case %s failed", case["id"])
            entry["error"] = str(e)
        report["cases"].append(entry)

    # Exports
    os.makedirs("data/exports", exist_ok=True)
    json_path = "data/exports/six_validation_full.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # Flat CSV
    csv_path = "data/exports/six_validation_full.csv"
    flat = []
    for entry in report["cases"]:
        case = entry.get("case") or {}
        resolved = entry.get("resolved") or {}
        tr = entry.get("transcript") or {}
        web = entry.get("web") or {}
        checks = entry.get("checks") or {}
        em = entry.get("emojis") or {}
        ocr_layers = entry.get("ocr_layers") or []
        onscreen = next((x for x in ocr_layers if x.get("source") == "browser_hydration"), {})
        captions = next((x for x in ocr_layers if x.get("source") == "closed_captions"), {})
        vision = [x for x in ocr_layers if x.get("source") == "google_vision"]
        flat.append(
            {
                "case_id": case.get("id"),
                "account": case.get("account"),
                "short_url": case.get("url"),
                "expect": case.get("expect"),
                "video_id": resolved.get("video_id"),
                "video_url": resolved.get("webpage_url"),
                "caption": resolved.get("description") or resolved.get("title"),
                "transcript_status": tr.get("status"),
                "transcript_chars": tr.get("chars"),
                "transcript": tr.get("transcript") or "",
                "whisper_model": tr.get("model"),
                "onscreen_chars": web.get("onscreen_chars"),
                "onscreen_text": web.get("onscreen_preview") or onscreen.get("preview") or "",
                "closed_caption_chars": web.get("closed_caption_chars"),
                "closed_captions": web.get("closed_caption_preview") or captions.get("preview") or "",
                "vision_ocr_frames": len(vision),
                "emoji_count": em.get("count"),
                "emojis": " ".join(
                    f"{r.get('emoji')}({r.get('emoji_name')})" for r in (em.get("rows") or [])
                ),
                "has_transcript": checks.get("has_transcript"),
                "has_onscreen": checks.get("has_onscreen"),
                "has_closed_captions": checks.get("has_closed_captions"),
                "has_emojis": checks.get("has_emojis"),
                "error": entry.get("error") or web.get("error"),
            }
        )

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        if flat:
            w = csv.DictWriter(f, fieldnames=list(flat[0].keys()))
            w.writeheader()
            w.writerows(flat)

    logger.info("Wrote %s and %s", json_path, csv_path)
    for entry in report["cases"]:
        c = entry.get("case", {})
        ch = entry.get("checks", {})
        logger.info(
            "Case %s @%s transcript=%s onscreen=%s captions=%s emojis=%s",
            c.get("id"),
            c.get("account"),
            ch.get("has_transcript"),
            ch.get("has_onscreen"),
            ch.get("has_closed_captions"),
            ch.get("has_emojis"),
        )
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
