import time
import requests
from auth import BASE_URL, auth_headers

DEFAULT_FIELDS = [
    "display_name",
    "bio_description",
    "avatar_url",
    "is_verified",
    "follower_count",
    "following_count",
    "likes_count",
    "video_count",
]


def get_user_info(username, fields=None):
    """Fetch account data for a single TikTok user.

    Args:
        username: TikTok username (without @).
        fields: List of fields to request.

    Returns:
        Dict of user data.
    """
    fields = fields or DEFAULT_FIELDS

    resp = requests.post(
        f"{BASE_URL}/research/user/info/",
        headers={**auth_headers(), "Content-Type": "application/json"},
        json={"username": username},
        params={"fields": ",".join(fields)},
    )

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 10))
        print(f"Rate limited — waiting {retry_after}s")
        time.sleep(retry_after)
        return get_user_info(username, fields)

    resp.raise_for_status()
    return resp.json().get("data", {})


def get_user_liked_videos(username, max_results=100, fields=None):
    """Fetch liked videos for a user with pagination.

    Args:
        username: TikTok username.
        max_results: Maximum number of videos to return.
        fields: List of video fields to request.

    Returns:
        List of video dicts.
    """
    fields = fields or ["id", "video_description", "create_time", "like_count", "view_count"]
    videos = []
    cursor = 0

    while len(videos) < max_results:
        resp = requests.post(
            f"{BASE_URL}/research/user/liked_videos/",
            headers={**auth_headers(), "Content-Type": "application/json"},
            json={"username": username, "max_count": min(100, max_results - len(videos)), "cursor": cursor},
            params={"fields": ",".join(fields)},
        )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 10))
            print(f"Rate limited — waiting {retry_after}s")
            time.sleep(retry_after)
            continue

        resp.raise_for_status()
        data = resp.json().get("data", {})

        batch = data.get("user_liked_videos", [])
        videos.extend(batch)

        if not data.get("has_more", False):
            break
        cursor = data.get("cursor", 0)

    return videos[:max_results]
