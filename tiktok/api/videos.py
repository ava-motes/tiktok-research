"""TikTok Research API — video query with date chunking and pagination."""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional

from tiktok.api.client import TikTokClient

logger = logging.getLogger(__name__)

VIDEO_FIELDS = [
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
    "sticker_info_list",
]

# Requested only by Pipeline 1 (not by v5.0 pull_videos.py)
PIPELINE_EXTRA_VIDEO_FIELDS = [
    "view_count",
    "region_code",
    "video_mention_list",
    "video_label",
    "effect_ids",
    "music_id",
]


def video_fields_param(*, extra: bool = False) -> str:
    names = list(VIDEO_FIELDS)
    if extra:
        for f in PIPELINE_EXTRA_VIDEO_FIELDS:
            if f not in names:
                names.append(f)
    return ",".join(names)


def flatten_sticker_overlay_text(sticker_info_list: Any) -> str:
    """Join non-empty ``sticker_name`` values from Research API ``sticker_info_list``."""
    if not sticker_info_list or not isinstance(sticker_info_list, list):
        return ""
    parts: List[str] = []
    for item in sticker_info_list:
        if not isinstance(item, dict):
            continue
        name = (item.get("sticker_name") or "").strip()
        if name:
            parts.append(name)
    return "\n---\n".join(parts)


def date_chunks(start_str: str, end_str: str, max_days: int = 30) -> List[tuple]:
    """Split a date range into chunks of at most max_days."""
    start = datetime.strptime(start_str, "%Y%m%d")
    end = datetime.strptime(end_str, "%Y%m%d")
    chunks = []
    while start < end:
        chunk_end = min(start + timedelta(days=max_days), end)
        chunks.append((start.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        start = chunk_end
    return chunks


def format_video(video: dict) -> dict:
    """Transform a raw API video dict into a normalized row for SQLite."""
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

    raw_stickers = video.get("sticker_info_list")
    if raw_stickers is None:
        raw_stickers = []
    sticker_overlay_text = flatten_sticker_overlay_text(raw_stickers)
    sticker_info_list_json = (
        json.dumps(raw_stickers, ensure_ascii=False) if raw_stickers else ""
    )

    return {
        "video_id": video_id,
        "username": username,
        "video_url": f"https://www.tiktok.com/@{username}/video/{video_id}",
        "create_time": create_time,
        "posted_at": posted_at,
        "caption": video.get("video_description", ""),
        "hashtags": hashtags,
        "like_count": video.get("like_count", 0),
        "share_count": video.get("share_count", 0),
        "comment_count": video.get("comment_count", 0),
        "save_count": video.get("favorites_count", 0),
        "duration_seconds": video.get("video_duration", 0),
        "voice_to_text": video.get("voice_to_text", ""),
        "sticker_overlay_text": sticker_overlay_text,
        "sticker_info_list": sticker_info_list_json,
        "view_count": video.get("view_count"),
        "region_code": video.get("region_code") or "",
        "video_mention_list": _json_field(video.get("video_mention_list")),
        "video_label": _json_field(video.get("video_label")),
        "effect_ids": _json_field(video.get("effect_ids")),
        "music_id": video.get("music_id") or "",
    }


def _json_field(value: Any) -> str:
    """Serialize API list/dict fields; leave blank when the API omitted them."""
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)


def query_videos_for_chunk(client: TikTokClient, username: str,
                            chunk_start: str, chunk_end: str,
                            max_videos: Optional[int] = None,
                            extra_fields: bool = False) -> List[dict]:
    """Fetch all videos for a user within a single date chunk (with pagination).

    If ``max_videos`` is set, stop after collecting that many videos (across pages).
    Pass ``None`` for existing multi-account behavior (no cap).
    """
    if max_videos is not None and max_videos <= 0:
        return []

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

        response = client.post(
            endpoint="research/video/query/",
            body=body,
            params={"fields": video_fields_param(extra=extra_fields)},
            handle=username,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
        )

        if response is None:
            break

        data = response.get("data", {})
        batch = data.get("videos", [])
        if max_videos is not None:
            space = max_videos - len(all_videos)
            if space <= 0:
                break
            if len(batch) > space:
                batch = batch[:space]
        all_videos.extend(batch)

        if max_videos is not None and len(all_videos) >= max_videos:
            break

        if not data.get("has_more", False):
            break

        cursor = data.get("cursor", 0)
        search_id = data.get("search_id", search_id)

    return [format_video(v) for v in all_videos]


def query_videos_by_keyword(
    client: TikTokClient,
    keyword: str,
    chunk_start: str,
    chunk_end: str,
    max_videos: Optional[int] = None,
) -> List[dict]:
    """Fetch videos matching a keyword within a date chunk (paginated).

    Uses Research API ``keyword`` IN filter (same pattern as legacy/videos.py).
    """
    if max_videos is not None and max_videos <= 0:
        return []

    query = {
        "and": [
            {
                "operation": "IN",
                "field_name": "keyword",
                "field_values": [keyword],
            }
        ]
    }

    all_videos: List[dict] = []
    cursor = 0
    search_id = None
    # Use keyword as handle label for raw JSONL archival
    handle_label = f"kw:{keyword}"[:80]

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

        response = client.post(
            endpoint="research/video/query/",
            body=body,
            params={"fields": ",".join(VIDEO_FIELDS)},
            handle=handle_label,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            keyword=keyword,
        )

        if response is None:
            break

        data = response.get("data", {})
        batch = data.get("videos", [])
        if max_videos is not None:
            space = max_videos - len(all_videos)
            if space <= 0:
                break
            if len(batch) > space:
                batch = batch[:space]
        all_videos.extend(batch)

        if max_videos is not None and len(all_videos) >= max_videos:
            break

        if not data.get("has_more", False):
            break

        cursor = data.get("cursor", 0)
        search_id = data.get("search_id", search_id)

    return [format_video(v) for v in all_videos]


def _find_video_in_chunk(
    client: TikTokClient,
    username: str,
    video_id: str,
    chunk_start: str,
    chunk_end: str,
) -> Optional[dict]:
    """Paginate a date chunk until ``video_id`` is found or pages are exhausted."""
    query = {
        "and": [
            {
                "operation": "EQ",
                "field_name": "username",
                "field_values": [username],
            }
        ]
    }
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

        response = client.post(
            endpoint="research/video/query/",
            body=body,
            params={"fields": ",".join(VIDEO_FIELDS)},
            handle=username,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
        )
        if response is None:
            return None

        data = response.get("data", {})
        for raw in data.get("videos", []):
            if str(raw.get("id")) == str(video_id):
                return format_video(raw)

        if not data.get("has_more", False):
            return None

        cursor = data.get("cursor", 0)
        search_id = data.get("search_id", search_id)


def fetch_video_by_id(
    client: TikTokClient,
    username: str,
    video_id: str,
    *,
    lookback_days: int = 120,
) -> Optional[dict]:
    """Look up one video via Research API across recent 30-day date chunks."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days)
    chunks = date_chunks(
        start.strftime("%Y%m%d"),
        end.strftime("%Y%m%d"),
        max_days=30,
    )
    for chunk_start, chunk_end in reversed(chunks):
        found = _find_video_in_chunk(
            client, username, video_id, chunk_start, chunk_end
        )
        if found:
            return found
    return None
