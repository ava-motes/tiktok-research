"""Export SQLite data to CSV files matching the old output formats.

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

logger = logging.getLogger(__name__)

VIDEO_COLUMNS = [
    "video_url", "handle", "posted_at", "caption", "hashtags",
    "like_count", "share_count", "save_count", "comment_count",
    "duration_seconds", "voice_to_text", "transcript", "news", "politics",
]

USER_COLUMNS = [
    "handle", "display_name", "follower_count", "following_count",
    "likes_count", "total_videos_posted", "is_verified", "bio",
    "videos_pulled", "api_failed",
]


def export_videos(conn, handles, output_path, start_date=None, end_date=None, max_recent=None):
    """Export videos to CSV."""
    placeholders = ",".join("?" for _ in handles)
    params = list(handles)

    if max_recent:
        rows = conn.execute(
            f"""SELECT v.video_url, v.username, v.posted_at, v.caption, v.hashtags,
                v.like_count, v.share_count, v.save_count, v.comment_count,
                v.duration_seconds, COALESCE(v.voice_to_text, '') as voice_to_text,
                COALESCE(t.transcript_text, '') as transcript,
                v.news, v.politics
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY username ORDER BY create_time DESC) as rn
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
            f"""SELECT v.video_url, v.username, v.posted_at, v.caption, v.hashtags,
                v.like_count, v.share_count, v.save_count, v.comment_count,
                v.duration_seconds, COALESCE(v.voice_to_text, '') as voice_to_text,
                COALESCE(t.transcript_text, '') as transcript,
                v.news, v.politics
            FROM videos v
            LEFT JOIN transcripts t ON v.video_id = t.video_id
            WHERE v.username IN ({placeholders})
            {date_filter}
            ORDER BY v.username, v.create_time DESC""",
            params,
        ).fetchall()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(VIDEO_COLUMNS)
        for r in rows:
            writer.writerow([
                r["video_url"], f"@{r['username']}", r["posted_at"], r["caption"],
                r["hashtags"], r["like_count"], r["share_count"], r["save_count"],
                r["comment_count"], r["duration_seconds"], r["voice_to_text"],
                r["transcript"], r["news"], r["politics"],
            ])

    logger.info(f"Exported {len(rows)} videos to {output_path}")


def export_users(conn, handles, output_path):
    """Export users to CSV with video count aggregates."""
    counts = get_video_counts_by_user(conn)

    placeholders = ",".join("?" for _ in handles)
    rows = conn.execute(
        f"SELECT * FROM users WHERE username IN ({placeholders}) ORDER BY username",
        handles,
    ).fetchall()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(USER_COLUMNS)
        for r in rows:
            c = counts.get(r["username"], {})
            writer.writerow([
                f"@{r['username']}", r["display_name"], r["follower_count"],
                r["following_count"], r["likes_count"], r["video_count"],
                r["is_verified"], r["bio"],
                c.get("videos_pulled", 0),
                r["api_failed"],
            ])

    logger.info(f"Exported {len(rows)} users to {output_path}")


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
        export_videos(conn, handles, os.path.join(exports_dir, f"{prefix}_videos_by_handle.csv"),
                      start_date=args.start_date, end_date=args.end_date, max_recent=args.max_recent)

    if args.users or export_both:
        export_users(conn, handles, os.path.join(exports_dir, f"{prefix}_handle_info.csv"))

    conn.close()


if __name__ == "__main__":
    main()
