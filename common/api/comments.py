"""TikTok Research API — comment list with pagination."""

import logging
from datetime import datetime, timezone
from typing import List

from api.client import TikTokClient

logger = logging.getLogger(__name__)

COMMENT_FIELDS = [
    "id",
    "video_id",
    "text",
    "like_count",
    "create_time",
    "parent_comment_id",
]


def format_comment(comment: dict, video_url: str, video_username: str) -> dict:
    """Normalize a raw API comment dict into a DB row."""
    create_time = comment.get("create_time", 0)
    if create_time:
        posted_at = datetime.fromtimestamp(create_time, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    else:
        posted_at = ""

    return {
        "comment_id": str(comment.get("id", "")),
        "video_id": str(comment.get("video_id", "")),
        "video_url": video_url,
        "video_username": video_username,
        "commenter_handle": comment.get("username", ""),
        "text": comment.get("text", ""),
        "like_count": comment.get("like_count", 0),
        "create_time": create_time,
        "posted_at": posted_at,
        "parent_comment_id": str(comment.get("parent_comment_id", "")) or None,
        "reply_count": 0,
    }


def get_comments_for_video(client: TikTokClient, video_id: str,
                            video_url: str, video_username: str,
                            max_comments: int = 100) -> List[dict]:
    """Fetch up to max_comments comments for a single video (with pagination)."""
    all_comments = []
    cursor = 0

    while len(all_comments) < max_comments:
        fetch_count = min(100, max_comments - len(all_comments))
        body = {
            "video_id": int(video_id),
            "max_count": fetch_count,
            "cursor": cursor,
        }

        response = client.post(
            endpoint="research/video/comment/list/",
            body=body,
            params={"fields": ",".join(COMMENT_FIELDS)},
            handle=video_username,
        )

        if response is None:
            break

        data = response.get("data", {})
        batch = data.get("comments", [])
        all_comments.extend(batch)

        if not data.get("has_more", False):
            break

        cursor = data.get("cursor", 0)

    return [format_comment(c, video_url, video_username) for c in all_comments]
