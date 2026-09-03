"""Pull all videos posted by a list of handles within a date range."""

import time
import csv
from datetime import datetime, timezone, timedelta
from auth import BASE_URL, auth_headers
import requests

HANDLES = [
    "nickdiramio",
    "kahlilgreene",
    "eyeinspired",
    "zohran_k_mamdani",
    "whoiskingtrivv",
    "realdonaldtrump",
    "underthedesknews",
    "cfh.unfiltered",
    "teamtrump",
    "goodtrouble_",
    "plantbased.baby",
    "priyaee_",
    "teatime.with.chris",
    "off_jawaggon",
    "violettewitch5",
    "pearlmania500",
    "cassiewillson",
    "prettypolitics101",
    "westbrouck",
    "keibenet",
    "emilysavesamerica",
    "mattwalsh_",
    "davidgyiham",
    "nikitadumptruck",
    "damonimani",
    "therealathenak",
    "iamuniquedaily",
    "neurodivergent_nate",
    "duncanyounot",
    "mitchellsyndrome",
    "hardwork544",
    "femaleintern_",
    "philipdefranco",
    "cohen.489",
    "seanlinden1",
    "harryjsisson",
    "xaviaer",
    "kristenzisek",
    "rachel_mle",
    "chasingoz",
    "newsnationnow",
    "slaythegop",
    "najwazebian",
    "expatriarch",
    "tazzyphe",
    "themakershub",
    "iamccsuarez",
    "tuckercarlson",
    "aaronparnas1",
    "lisaremillard",
    "c.a.i.t.l.y.n",
    "slothgirl__",
    "toureshow",
    "sabrina.zohar",
    "mikesippel21",
    "fr.sam48",
    "lovetohatepod",
    "ashleyblairxoxo",
]

START_DATE = "20260101"
END_DATE = datetime.now(timezone.utc).strftime("%Y%m%d")

FIELDS = [
    "id",
    "username",
    "create_time",
    "video_description",
    "hashtag_names",
    "like_count",
    "share_count",
    "comment_count",
    "favorites_count",
    "video_duration",
    "voice_to_text",
]

CSV_COLUMNS = [
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
    "transcript",
]


def date_chunks(start_str, end_str, max_days=30):
    """Split a date range into chunks of at most max_days."""
    start = datetime.strptime(start_str, "%Y%m%d")
    end = datetime.strptime(end_str, "%Y%m%d")
    chunks = []
    while start < end:
        chunk_end = min(start + timedelta(days=max_days), end)
        chunks.append((start.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        start = chunk_end
    return chunks


def query_videos_for_user(username):
    """Fetch all videos for a single user, splitting into 30-day windows."""
    query = {
        "and": [
            {
                "operation": "EQ",
                "field_name": "username",
                "field_values": [username],
            }
        ]
    }

    all_videos = []

    for chunk_start, chunk_end in date_chunks(START_DATE, END_DATE):
        cursor = 0
        search_id = None

        while True:
            body = {
                "query": query,
                "max_count": 100,
                "start_date": chunk_start,
                "end_date": chunk_end,
            }
            if search_id:
                body["cursor"] = cursor
                body["search_id"] = search_id

            resp = requests.post(
                f"{BASE_URL}/research/video/query/",
                headers={**auth_headers(), "Content-Type": "application/json"},
                json=body,
                params={"fields": ",".join(FIELDS)},
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 10))
                print(f"  Rate limited — waiting {retry_after}s")
                time.sleep(retry_after)
                continue

            if not resp.ok:
                print(f"  Error {resp.status_code} for @{username}: {resp.text}")
                break

            data = resp.json().get("data", {})
            batch = data.get("videos", [])
            all_videos.extend(batch)

            if not data.get("has_more", False):
                break

            cursor = data.get("cursor", 0)
            search_id = data.get("search_id", search_id)

    return all_videos


def format_row(video):
    """Transform a raw API video dict into a clean CSV row."""
    username = video.get("username", "")
    video_id = video.get("id", "")
    create_time = video.get("create_time", 0)

    if create_time:
        posted_at = datetime.fromtimestamp(create_time, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    else:
        posted_at = ""

    hashtags = video.get("hashtag_names", [])
    if isinstance(hashtags, list):
        hashtags = ", ".join(hashtags)

    return {
        "video_url": f"https://www.tiktok.com/@{username}/video/{video_id}",
        "handle": f"@{username}",
        "posted_at": posted_at,
        "caption": video.get("video_description", ""),
        "hashtags": hashtags,
        "like_count": video.get("like_count", 0),
        "share_count": video.get("share_count", 0),
        "save_count": video.get("favorites_count", 0),
        "comment_count": video.get("comment_count", 0),
        "duration_seconds": video.get("video_duration", 0),
        "transcript": video.get("voice_to_text", ""),
    }


def main():
    output_file = "sample_videos_by_handle.csv"
    total = 0

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for i, handle in enumerate(HANDLES, 1):
            print(f"[{i}/{len(HANDLES)}] Pulling videos for @{handle}...")
            videos = query_videos_for_user(handle)
            rows = [format_row(v) for v in videos]
            writer.writerows(rows)
            f.flush()
            total += len(rows)
            print(f"  → {len(rows)} videos")

    print(f"\nDone. {total} total videos written to {output_file}")


if __name__ == "__main__":
    main()
