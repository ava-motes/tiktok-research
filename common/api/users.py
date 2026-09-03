"""TikTok Research API — unified user info fetching."""

import logging
from api.client import TikTokClient

logger = logging.getLogger(__name__)

USER_FIELDS = [
    "display_name",
    "bio_description",
    "is_verified",
    "follower_count",
    "following_count",
    "likes_count",
    "video_count",
]


def get_user_info(client: TikTokClient, username: str) -> dict:
    """Fetch profile data for a single user.

    Returns a normalized dict ready for db.insert_user(), or a
    dict with api_failed=1 on error.
    """
    response = client.post(
        endpoint="research/user/info/",
        body={"username": username},
        params={"fields": ",".join(USER_FIELDS)},
        handle=username,
    )

    if response is None:
        return {
            "username": username,
            "display_name": "",
            "bio": "",
            "is_verified": False,
            "follower_count": 0,
            "following_count": 0,
            "likes_count": 0,
            "video_count": 0,
            "api_failed": 1,
        }

    data = response.get("data", {})
    return {
        "username": username,
        "display_name": data.get("display_name", ""),
        "bio": data.get("bio_description", ""),
        "is_verified": data.get("is_verified", False),
        "follower_count": data.get("follower_count", 0),
        "following_count": data.get("following_count", 0),
        "likes_count": data.get("likes_count", 0),
        "video_count": data.get("video_count", 0),
        "api_failed": 0,
    }
