"""Export a comparison CSV of all video text layers for manual inspection.

Columns: video_id, url, caption, voice_to_text, sticker_overlay_text,
transcript (Whisper), onscreen_text (OCR fallback).

Usage:
    source venv/bin/activate
    python scripts/export_text_layers_sample.py --eval
    python scripts/export_text_layers_sample.py --eval --pull --max-per-handle 5
    python scripts/export_text_layers_sample.py --group sample --max-per-handle 3
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok import auth
from tiktok.api.client import TikTokClient
from tiktok.api.videos import date_chunks, fetch_video_by_id, query_videos_for_chunk
from tiktok.config import load_config
from tiktok.db import get_connection, insert_video
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)

# Fixed eval set (from OCR evaluation URLs)
EVAL_VIDEO_IDS = [
    "7625948114075012382",
    "7625797371405847822",
    "7625279111681920286",
    "7626031439900904718",
    "7626047205018701087",
    "7625992856901111070",
]

EVAL_HANDLES = [
    "harryjsisson",
    "jaysworld411",
    "joeycontino2",
    "cnn",
    "simpleblacktheory",
    "pauletteonthemic",
]

EVAL_NOTES = {
    "7625948114075012382": "on-screen text + closed captions",
    "7625797371405847822": "on-screen text, captions, twitter screenshots",
    "7625279111681920286": "on-screen text, captions, green screen twitter",
    "7626031439900904718": "external edits, captions, tweets",
    "7626047205018701087": "stitch, captions, on-screen text, screenshots",
    "7625992856901111070": "screenshots + music/lyrics only",
}

TEXT_LAYER_COLUMNS = [
    "video_id",
    "url",
    "username",
    "posted_at",
    "eval_notes",
    "caption",
    "voice_to_text",
    "sticker_overlay_text",
    "transcript",
    "onscreen_text",
    "caption_len",
    "voice_to_text_len",
    "sticker_overlay_len",
    "transcript_len",
    "onscreen_text_len",
    "needs_ocr_fallback",
]


def pull_recent_for_handles(
    client: TikTokClient,
    conn,
    handles: list,
    max_per_handle: int,
    lookback_days: int,
) -> int:
    end_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    start_date = (
        datetime.now(timezone.utc) - timedelta(days=lookback_days)
    ).strftime("%Y%m%d")
    chunks = list(reversed(date_chunks(start_date, end_date)))
    total = 0

    for handle in handles:
        collected = []
        for chunk_start, chunk_end in chunks:
            if len(collected) >= max_per_handle:
                break
            collected.extend(
                query_videos_for_chunk(client, handle, chunk_start, chunk_end)
            )
        collected.sort(key=lambda v: v.get("create_time", 0), reverse=True)
        recent = collected[:max_per_handle]
        for v in recent:
            insert_video(conn, v)
        conn.commit()
        total += len(recent)
        logger.info("@%s — inserted/updated %s videos", handle, len(recent))
    return total


def refresh_eval_videos(client: TikTokClient, conn) -> None:
    """Re-fetch the six eval videos so sticker_overlay_text is current."""
    pairs = list(zip(EVAL_HANDLES, EVAL_VIDEO_IDS))
    for username, video_id in pairs:
        row = fetch_video_by_id(client, username, video_id)
        if row:
            insert_video(conn, row)
            logger.info("Refreshed eval video %s (@%s)", video_id, username)
        else:
            logger.warning("Eval video not found in API: %s (@%s)", video_id, username)
    conn.commit()


def export_text_layers(
    conn,
    *,
    video_ids: Optional[List[str]] = None,
    handles: Optional[List[str]] = None,
    max_per_handle: Optional[int] = None,
    output_path: str,
) -> int:
    if video_ids:
        placeholders = ",".join("?" for _ in video_ids)
        sql = f"""
            SELECT v.video_id, v.video_url, v.username, v.posted_at, v.caption,
                   COALESCE(v.voice_to_text, '') AS voice_to_text,
                   COALESCE(v.sticker_overlay_text, '') AS sticker_overlay_text,
                   COALESCE(t.transcript_text, '') AS transcript,
                   COALESCE(v.onscreen_text, '') AS onscreen_text
            FROM videos v
            LEFT JOIN transcripts t ON v.video_id = t.video_id
            WHERE v.video_id IN ({placeholders})
            ORDER BY v.username, v.create_time DESC
        """
        rows = conn.execute(sql, video_ids).fetchall()
    elif handles:
        placeholders = ",".join("?" for _ in handles)
        params: list = list(handles)
        if max_per_handle:
            sql = f"""
                SELECT v.video_id, v.video_url, v.username, v.posted_at, v.caption,
                       COALESCE(v.voice_to_text, '') AS voice_to_text,
                       COALESCE(v.sticker_overlay_text, '') AS sticker_overlay_text,
                       COALESCE(t.transcript_text, '') AS transcript,
                       COALESCE(v.onscreen_text, '') AS onscreen_text
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY username ORDER BY create_time DESC
                    ) AS rn
                    FROM videos WHERE username IN ({placeholders})
                ) v
                LEFT JOIN transcripts t ON v.video_id = t.video_id
                WHERE v.rn <= ?
                ORDER BY v.username, v.create_time DESC
            """
            params.append(max_per_handle)
            rows = conn.execute(sql, params).fetchall()
        else:
            sql = f"""
                SELECT v.video_id, v.video_url, v.username, v.posted_at, v.caption,
                       COALESCE(v.voice_to_text, '') AS voice_to_text,
                       COALESCE(v.sticker_overlay_text, '') AS sticker_overlay_text,
                       COALESCE(t.transcript_text, '') AS transcript,
                       COALESCE(v.onscreen_text, '') AS onscreen_text
                FROM videos v
                LEFT JOIN transcripts t ON v.video_id = t.video_id
                WHERE v.username IN ({placeholders})
                ORDER BY v.username, v.create_time DESC
            """
            rows = conn.execute(sql, params).fetchall()
    else:
        raise ValueError("Provide video_ids or handles")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TEXT_LAYER_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            cap = r["caption"] or ""
            vtt = r["voice_to_text"] or ""
            stk = r["sticker_overlay_text"] or ""
            wh = r["transcript"] or ""
            ocr = r["onscreen_text"] or ""
            writer.writerow(
                {
                    "video_id": r["video_id"],
                    "url": r["video_url"] or "",
                    "username": r["username"],
                    "posted_at": r["posted_at"] or "",
                    "eval_notes": EVAL_NOTES.get(r["video_id"], ""),
                    "caption": cap,
                    "voice_to_text": vtt,
                    "sticker_overlay_text": stk,
                    "transcript": wh,
                    "onscreen_text": ocr,
                    "caption_len": len(cap),
                    "voice_to_text_len": len(vtt),
                    "sticker_overlay_len": len(stk),
                    "transcript_len": len(wh),
                    "onscreen_text_len": len(ocr),
                    "needs_ocr_fallback": (
                        "yes" if not stk.strip() and not ocr.strip() else "maybe"
                        if not stk.strip() else "no"
                    ),
                }
            )

    logger.info("Exported %s rows to %s", len(rows), output_path)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export text-layer comparison CSV for manual inspection"
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--eval",
        action="store_true",
        help="Export the six fixed OCR eval videos (and refresh from API if --pull)",
    )
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument(
        "--pull",
        action="store_true",
        help="Pull recent videos from API before export",
    )
    parser.add_argument(
        "--max-per-handle",
        type=int,
        default=5,
        help="When pulling/exporting by group, max videos per handle",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=120,
        help="Days to search when pulling",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default: data/text_layers_comparison_<stamp>.csv)",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    cfg = load_config(args.config)
    conn = get_connection(cfg.paths["database"])

    if args.pull:
        auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)
        client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"], db_conn=conn)
        if args.eval:
            refresh_eval_videos(client, conn)
            pull_recent_for_handles(
                client, conn, EVAL_HANDLES, args.max_per_handle, args.lookback_days
            )
        elif args.group:
            handles = cfg.get_handles(args.group)
            pull_recent_for_handles(
                client, conn, handles, args.max_per_handle, args.lookback_days
            )
        else:
            logger.error("--pull requires --eval or --group")
            return 1

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output = args.output or os.path.join(
        "data", f"text_layers_comparison_{stamp}.csv"
    )

    if args.eval:
        n = export_text_layers(conn, video_ids=EVAL_VIDEO_IDS, output_path=output)
    elif args.group:
        handles = cfg.get_handles(args.group)
        n = export_text_layers(
            conn,
            handles=handles,
            max_per_handle=args.max_per_handle,
            output_path=output,
        )
    else:
        parser.error("Specify --eval or --group")
        return 1

    conn.close()
    print(f"Wrote {n} rows to {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
