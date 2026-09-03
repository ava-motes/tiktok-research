"""Export SQLite data to CSV files matching the old output formats.

Video exports include all multimodal text layers plus unified visual_text_combined.

Usage:
    python scripts/export_csv.py                                        # Export all
    python scripts/export_csv.py --videos --group sample                # Videos only
    python scripts/export_csv.py --users --group complete               # Users only
    python scripts/export_csv.py --start-date 2026-03-04 --end-date 2026-03-11  # Date range
"""

import sys
import os
import csv
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.config import load_config
from tiktok.logging_setup import setup_logging
from tiktok.db import get_connection, get_video_counts_by_user

from tiktok.text.normalize import extract_emojis, extract_emojis_from_sticker_info

logger = logging.getLogger(__name__)

VIDEO_COLUMNS = [
    "video_url",
    "handle",
    "posted_at",
    "caption",
    "hashtags",
    "like_count",
    "share_count",
    "save_count",
    "comment_count",
    "duration_seconds",
    "voice_to_text",
    "transcript",
    "sticker_overlay_text",
    "browser_ocr_text",
    "onscreen_text",
    "visual_text_combined",
    "visual_text_source_priority",
    "caption_emojis",
    "sticker_emojis",
    "visual_text_emojis",
    "all_text_emojis",
    "news",
    "politics",
]

USER_COLUMNS = [
    "handle",
    "display_name",
    "follower_count",
    "following_count",
    "likes_count",
    "total_videos_posted",
    "is_verified",
    "bio",
    "videos_pulled",
    "api_failed",
]

_VIDEO_SELECT = """
    v.video_url, v.username, v.posted_at, v.caption, v.hashtags,
    v.like_count, v.share_count, v.save_count, v.comment_count,
    v.duration_seconds,
    COALESCE(v.voice_to_text, '') AS voice_to_text,
    COALESCE(t.transcript_text, '') AS transcript,
    COALESCE(v.sticker_overlay_text, '') AS sticker_overlay_text,
    COALESCE(v.browser_ocr_text, '') AS browser_ocr_text,
    COALESCE(v.onscreen_text, '') AS onscreen_text,
    COALESCE(v.visual_text_combined, '') AS visual_text_combined,
    COALESCE(v.visual_text_source_priority, '') AS visual_text_source_priority,
    COALESCE(v.sticker_info_list, '') AS sticker_info_list,
    v.news, v.politics
"""


def _write_video_rows(rows, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    # utf-8-sig (BOM) so Excel on Windows/Mac opens punctuation correctly
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(VIDEO_COLUMNS)
        for r in rows:
            row = dict(r)
            caption = row["caption"] or ""
            sticker = row["sticker_overlay_text"] or ""
            browser = row["browser_ocr_text"] or ""
            onscreen = row["onscreen_text"] or ""
            visual = row["visual_text_combined"] or ""
            voice = row["voice_to_text"] or ""
            transcript = row["transcript"] or ""
            caption_emojis = extract_emojis(caption)
            sticker_emojis = extract_emojis(
                sticker,
                extract_emojis_from_sticker_info(row.get("sticker_info_list")),
            )
            visual_emojis = extract_emojis(sticker, browser, onscreen, visual)
            all_emojis = extract_emojis(
                caption, voice, transcript, sticker, browser, onscreen, visual,
                extract_emojis_from_sticker_info(row.get("sticker_info_list")),
            )
            writer.writerow([
                row["video_url"],
                f"@{row['username']}",
                row["posted_at"],
                caption,
                row["hashtags"],
                row["like_count"],
                row["share_count"],
                row["save_count"],
                row["comment_count"],
                row["duration_seconds"],
                voice,
                transcript,
                sticker,
                browser,
                onscreen,
                visual,
                row["visual_text_source_priority"],
                caption_emojis,
                sticker_emojis,
                visual_emojis,
                all_emojis,
                row["news"],
                row["politics"],
            ])
    logger.info("Exported %s videos to %s", len(rows), output_path)


def export_videos(conn, handles, output_path, start_date=None, end_date=None, max_recent=None):
    """Export videos to a research-ready multimodal CSV."""
    placeholders = ",".join("?" for _ in handles)
    params = list(handles)

    if max_recent:
        rows = conn.execute(
            f"""SELECT {_VIDEO_SELECT}
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY username ORDER BY create_time DESC) AS rn
                FROM videos WHERE username IN ({placeholders})
            ) v
            LEFT JOIN transcripts t ON v.video_id = t.video_id
            WHERE v.rn <= ?
            ORDER BY v.username, v.create_time DESC""",
            params + [max_recent],
        ).fetchall()
    else:
        date_filter = ""
        if start_date:
            date_filter += " AND v.posted_at >= ?"
            params.append(start_date)
        if end_date:
            date_filter += " AND v.posted_at <= ?"
            params.append(end_date + " 23:59:59")
        rows = conn.execute(
            f"""SELECT {_VIDEO_SELECT}
            FROM videos v
            LEFT JOIN transcripts t ON v.video_id = t.video_id
            WHERE v.username IN ({placeholders})
            {date_filter}
            ORDER BY v.username, v.create_time DESC""",
            params,
        ).fetchall()

    _write_video_rows(rows, output_path)


def export_users(conn, handles, output_path):
    """Export users to CSV with video count aggregates."""
    counts = get_video_counts_by_user(conn)

    placeholders = ",".join("?" for _ in handles)
    rows = conn.execute(
        f"SELECT * FROM users WHERE username IN ({placeholders}) ORDER BY username",
        handles,
    ).fetchall()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    # utf-8-sig (BOM) so Excel on Windows/Mac opens punctuation correctly
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(USER_COLUMNS)
        for r in rows:
            c = counts.get(r["username"], {})
            writer.writerow([
                f"@{r['username']}",
                r["display_name"],
                r["follower_count"],
                r["following_count"],
                r["likes_count"],
                r["video_count"],
                r["is_verified"],
                r["bio"],
                c.get("videos_pulled", 0),
                r["api_failed"],
            ])

    logger.info("Exported %s users to %s", len(rows), output_path)


def main():
    parser = argparse.ArgumentParser(description="Export SQLite data to CSV")
    parser.add_argument("--group", default=None, help="Handle group from config.yaml")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--videos", action="store_true", help="Export videos only")
    parser.add_argument("--users", action="store_true", help="Export users only")
    parser.add_argument("--output-prefix", default=None, help="Output filename prefix (default: group name)")
    parser.add_argument("--start-date", default=None, help="Only export videos posted on or after this date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="Only export videos posted on or before this date (YYYY-MM-DD)")
    parser.add_argument("--max-recent", type=int, default=None, help="Only export the N most recent videos per handle")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)

    group_name = args.group or cfg.default_group("pull_videos")
    handles = cfg.get_handles(group_name)
    prefix = args.output_prefix or group_name
    exports_dir = cfg.paths["exports"]

    conn = get_connection(cfg.paths["database"])

    export_both = not args.videos and not args.users

    if args.videos or export_both:
        export_videos(
            conn,
            handles,
            os.path.join(exports_dir, f"{prefix}_videos_by_handle.csv"),
            start_date=args.start_date,
            end_date=args.end_date,
            max_recent=args.max_recent,
        )

    if args.users or export_both:
        export_users(
            conn,
            handles,
            os.path.join(exports_dir, f"{prefix}_handle_info.csv"),
        )

    conn.close()


if __name__ == "__main__":
    main()
