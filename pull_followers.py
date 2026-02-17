"""Pull follower counts for each handle."""

import time
import csv
import requests
from auth import BASE_URL, auth_headers
from pull_videos import HANDLES

FIELDS = [
    "display_name",
    "follower_count",
    "following_count",
    "likes_count",
    "video_count",
    "is_verified",
    "bio_description",
]

CSV_COLUMNS = [
    "handle",
    "display_name",
    "follower_count",
    "following_count",
    "likes_count",
    "video_count",
    "is_verified",
    "bio",
]


def get_user_info(username):
    """Fetch profile data for a single user."""
    resp = requests.post(
        f"{BASE_URL}/research/user/info/",
        headers={**auth_headers(), "Content-Type": "application/json"},
        json={"username": username},
        params={"fields": ",".join(FIELDS)},
    )

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 10))
        print(f"  Rate limited — waiting {retry_after}s")
        time.sleep(retry_after)
        return get_user_info(username)

    if not resp.ok:
        print(f"  Error {resp.status_code} for @{username}: {resp.text}")
        return None

    return resp.json().get("data", {})


def format_row(username, data):
    return {
        "handle": f"@{username}",
        "display_name": data.get("display_name", ""),
        "follower_count": data.get("follower_count", 0),
        "following_count": data.get("following_count", 0),
        "likes_count": data.get("likes_count", 0),
        "video_count": data.get("video_count", 0),
        "is_verified": data.get("is_verified", False),
        "bio": data.get("bio_description", ""),
    }


def main():
    output_file = "sample_handle_info.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for i, handle in enumerate(HANDLES, 1):
            print(f"[{i}/{len(HANDLES)}] Fetching info for @{handle}...")
            data = get_user_info(handle)
            if data:
                writer.writerow(format_row(handle, data))
                f.flush()
                print(f"  → {data.get('follower_count', '?')} followers")
            else:
                print(f"  → skipped (no data)")

    print(f"\nDone. Results written to {output_file}")


if __name__ == "__main__":
    main()
