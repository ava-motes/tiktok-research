import time
import requests
from auth import BASE_URL, auth_headers

DEFAULT_FIELDS = [
    "id",
    "video_description",
    "create_time",
    "region_code",
    "share_count",
    "view_count",
    "like_count",
    "comment_count",
    "music_id",
    "hashtag_names",
    "username",
    "effect_ids",
]


def query_videos(keyword=None, hashtag=None, start_date=None, end_date=None,
                 max_results=100, fields=None):
    """Query videos by keyword or hashtag with automatic pagination.

    Args:
        keyword: Search term to match in video descriptions.
        hashtag: Hashtag name (without #).
        start_date: Start date string YYYYMMDD.
        end_date: End date string YYYYMMDD.
        max_results: Maximum number of videos to return.
        fields: List of fields to request (uses DEFAULT_FIELDS if None).

    Returns:
        List of video dicts.
    """
    if not keyword and not hashtag:
        raise ValueError("Provide at least a keyword or hashtag")

    conditions = []
    if keyword:
        conditions.append({
            "operation": "IN",
            "field_name": "keyword",
            "field_values": [keyword],
        })
    if hashtag:
        conditions.append({
            "operation": "IN",
            "field_name": "hashtag_name",
            "field_values": [hashtag],
        })

    query = {"and": conditions}
    fields = fields or DEFAULT_FIELDS
    videos = []
    cursor = 0
    search_id = None
    page_size = min(max_results, 100)

    while len(videos) < max_results:
        body = {
            "query": query,
            "max_count": page_size,
            "start_date": start_date,
            "end_date": end_date,
        }
        if search_id:
            body["cursor"] = cursor
            body["search_id"] = search_id

        resp = requests.post(
            f"{BASE_URL}/research/video/query/",
            headers={**auth_headers(), "Content-Type": "application/json"},
            json=body,
            params={"fields": ",".join(fields)},
        )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 10))
            print(f"Rate limited — waiting {retry_after}s")
            time.sleep(retry_after)
            continue

        if not resp.ok:
            print(f"Error {resp.status_code}: {resp.text}")
            resp.raise_for_status()
        data = resp.json().get("data", {})

        batch = data.get("videos", [])
        videos.extend(batch)

        if not data.get("has_more", False):
            break

        cursor = data.get("cursor", 0)
        search_id = data.get("search_id", search_id)

    return videos[:max_results]
