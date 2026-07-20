"""Debug: inspect raw TikTok Research API video payload for OCR-related fields.

Does not modify the production pipeline. Fetches one video, saves the full API
response, and recursively searches keys/values for text/OCR-related terms.

Usage (from project root):
    source venv/bin/activate
    python scripts/debug_api_video_payload.py
    python scripts/debug_api_video_payload.py --url "https://www.tiktok.com/t/ZP8gL1VxH/"
    python scripts/debug_api_video_payload.py --video-id 7625948114075012382 --username harryjsisson
    python scripts/debug_api_video_payload.py --compare-web
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok import auth
from tiktok.api.client import TikTokClient
from tiktok.api.download import extract_video_metadata
from tiktok.api.videos import date_chunks
from tiktok.config import load_config
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)

# All fields documented for research/video/query/
ALL_DOCUMENTED_VIDEO_FIELDS = [
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
    "playlist_id",
    "voice_to_text",
    "is_stem_verified",
    "favorites_count",
    "video_duration",
    "hashtag_info_list",
    "sticker_info_list",
    "effect_info_list",
    "video_mention_list",
    "video_label",
    "video_tag",
]

SEARCH_KEYWORDS = [
    "ocr",
    "text",
    "onscreen",
    "on_screen",
    "on-screen",
    "subtitle",
    "caption",
    "voice",
    "transcript",
    "extracted",
    "recognition",
    "sticker",
    "visual",
    "overlay",
    "cla",
]

DEFAULT_URL = "https://www.tiktok.com/t/ZP8gL1VxH/"
DEFAULT_USERNAME = "harryjsisson"

OUTPUT_API_JSON = os.path.join("data", "debug_api_payload.json")
OUTPUT_SEARCH_TXT = os.path.join("data", "debug_api_field_search.txt")
OUTPUT_WEB_JSON = os.path.join("data", "debug_web_payload.json")


def resolve_video(
    url: Optional[str],
    video_id: Optional[str],
    username: Optional[str],
) -> Tuple[str, str, str]:
    """Return (video_id, username, canonical_url)."""
    if url:
        info = extract_video_metadata(url)
        if not info:
            raise RuntimeError(f"yt-dlp could not resolve URL: {url}")
        vid = str(info.get("id") or video_id or "")
        user = (
            username
            or info.get("uploader")
            or info.get("channel")
            or DEFAULT_USERNAME
        )
        canonical = info.get("webpage_url") or info.get("url") or url
        return vid, str(user), canonical

    if not video_id or not username:
        raise ValueError("Provide --url or both --video-id and --username")
    canonical = f"https://www.tiktok.com/@{username}/video/{video_id}"
    return video_id, username, canonical


def fetch_raw_api_video(
    client: TikTokClient,
    video_id: str,
    username: str,
    *,
    lookback_days: int = 120,
) -> Dict[str, Any]:
    """Query Research API and return envelope with request + first matching raw video."""
    end = datetime.now(timezone.utc)
    start = end - __import__("datetime").timedelta(days=lookback_days)
    chunks = date_chunks(
        start.strftime("%Y%m%d"),
        end.strftime("%Y%m%d"),
        max_days=30,
    )
    fields_param = ",".join(ALL_DOCUMENTED_VIDEO_FIELDS)

    for chunk_start, chunk_end in reversed(chunks):
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
        pages = 0

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
                params={"fields": fields_param},
                handle=username,
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                debug_lookup_video_id=video_id,
            )
            pages += 1

            if response is None:
                break

            videos = response.get("data", {}).get("videos", [])
            for raw in videos:
                if str(raw.get("id")) == str(video_id):
                    return {
                        "matched": True,
                        "video_id": video_id,
                        "username": username,
                        "chunk_start": chunk_start,
                        "chunk_end": chunk_end,
                        "pages_searched_in_chunk": pages,
                        "requested_fields": ALL_DOCUMENTED_VIDEO_FIELDS,
                        "request_body": body,
                        "fields_param": fields_param,
                        "full_api_response": response,
                        "matched_video_raw": raw,
                    }

            data = response.get("data", {})
            if not data.get("has_more", False):
                break
            cursor = data.get("cursor", 0)
            search_id = data.get("search_id", search_id)

    return {
        "matched": False,
        "video_id": video_id,
        "username": username,
        "requested_fields": ALL_DOCUMENTED_VIDEO_FIELDS,
        "error": "video_not_found_in_research_api",
    }


def fetch_web_item_struct(url: str) -> Dict[str, Any]:
    from tiktok.web.metadata import fetch_hydration_html, parse_item_struct

    html = fetch_hydration_html(url)
    item = parse_item_struct(html)
    if not item:
        return {"matched": False, "url": url, "error": "hydration_item_struct_missing"}
    return {
        "matched": True,
        "url": url,
        "video_id": str(item.get("id") or ""),
        "username": (item.get("author") or {}).get("uniqueId"),
        "item_struct": item,
    }


def _preview(value: Any, max_len: int = 200) -> str:
    if value is None:
        return "<null>"
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False)
    else:
        text = str(value)
    text = text.replace("\n", "\\n")
    if len(text) > max_len:
        return text[:max_len] + "..."
    return text


def search_fields(
    obj: Any,
    *,
    path: str = "",
    keywords: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Recursively find keys or string values matching keywords."""
    return _search_fields_impl(obj, path, keywords or SEARCH_KEYWORDS)


def _search_fields_impl(
    obj: Any,
    path: str,
    keywords: List[str],
) -> List[Dict[str, Any]]:
    hits: List[Dict[str, Any]] = []

    def key_matches(key: str) -> bool:
        kl = key.lower()
        return any(kw in kl for kw in keywords)

    def value_matches(val: str) -> bool:
        vl = val.lower()
        return any(kw in vl for kw in keywords)

    if isinstance(obj, dict):
        for key, val in obj.items():
            p = f"{path}.{key}" if path else key
            if key_matches(key):
                hits.append(
                    {"path": p, "match_type": "key", "preview": _preview(val)}
                )
            if isinstance(val, str) and len(val) > 2 and value_matches(val):
                hits.append(
                    {"path": p, "match_type": "value", "preview": _preview(val)}
                )
            hits.extend(_search_fields_impl(val, p, keywords))
    elif isinstance(obj, list):
        for i, val in enumerate(obj):
            p = f"{path}[{i}]"
            hits.extend(_search_fields_impl(val, p, keywords))

    return hits


def format_search_report(
    label: str,
    payload: Any,
    hits: List[Dict[str, Any]],
) -> str:
    lines = [
        f"=== {label} ===",
        f"Total matches: {len(hits)}",
        "",
    ]
    seen = set()
    for h in hits:
        key = (h["path"], h["match_type"])
        if key in seen:
            continue
        seen.add(key)
        lines.append(f"{h['path']}  [{h['match_type']}]")
        lines.append(f"  {_preview(h.get('preview', ''), 300)}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect raw Research API video payload for OCR-related fields"
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--url", default=DEFAULT_URL, help="TikTok URL (short or canonical)")
    parser.add_argument("--video-id", default=None)
    parser.add_argument("--username", default=None)
    parser.add_argument(
        "--compare-web",
        action="store_true",
        help="Also fetch browser hydration JSON for side-by-side comparison",
    )
    parser.add_argument(
        "--output-api",
        default=OUTPUT_API_JSON,
        help="Path for full API debug JSON",
    )
    parser.add_argument(
        "--output-search",
        default=OUTPUT_SEARCH_TXT,
        help="Path for field search report",
    )
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    os.makedirs("data", exist_ok=True)

    if args.video_id:
        username = args.username or DEFAULT_USERNAME
        video_id = args.video_id
        canonical = f"https://www.tiktok.com/@{username}/video/{video_id}"
    else:
        video_id, username, canonical = resolve_video(
            args.url, None, args.username
        )

    logger.info("Resolved video_id=%s username=@%s", video_id, username)

    cfg = load_config(args.config)
    auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"])

    api_bundle = {
        "inspected_at": datetime.now(timezone.utc).isoformat(),
        "canonical_url": canonical,
        "video_id": video_id,
        "username": username,
        "search_keywords": SEARCH_KEYWORDS,
        **fetch_raw_api_video(client, video_id, username),
    }

    api_hits = []
    if api_bundle.get("matched_video_raw"):
        api_hits = search_fields(api_bundle["matched_video_raw"])
    api_bundle["field_search_hits"] = api_hits
    api_bundle["top_level_video_keys"] = sorted(
        (api_bundle.get("matched_video_raw") or {}).keys()
    )

    with open(args.output_api, "w", encoding="utf-8") as f:
        json.dump(api_bundle, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", args.output_api)

    report_parts = [
        f"Inspected at: {api_bundle['inspected_at']}",
        f"Video: {canonical}",
        f"Username: @{username}",
        f"Video ID: {video_id}",
        "",
        "DOCUMENTED API VIDEO FIELDS REQUESTED:",
        ", ".join(ALL_DOCUMENTED_VIDEO_FIELDS),
        "",
        format_search_report(
            "Research API — matched_video_raw",
            api_bundle.get("matched_video_raw"),
            api_hits,
        ),
    ]

    if args.compare_web:
        logger.info("Fetching browser hydration payload for comparison")
        web_bundle = {
            "inspected_at": datetime.now(timezone.utc).isoformat(),
            "canonical_url": canonical,
            **fetch_web_item_struct(canonical),
        }
        web_hits = []
        if web_bundle.get("item_struct"):
            web_hits = search_fields(web_bundle["item_struct"])
        web_bundle["field_search_hits"] = web_hits
        web_bundle["top_level_item_keys"] = sorted(
            (web_bundle.get("item_struct") or {}).keys()
        )

        with open(OUTPUT_WEB_JSON, "w", encoding="utf-8") as f:
            json.dump(web_bundle, f, indent=2, ensure_ascii=False)
        logger.info("Wrote %s", OUTPUT_WEB_JSON)

        report_parts.extend(
            [
                "",
                format_search_report(
                    "Browser itemStruct",
                    web_bundle.get("item_struct"),
                    web_hits,
                ),
            ]
        )

        # Summary comparison
        api_paths = {h["path"] for h in api_hits}
        web_paths = {h["path"] for h in web_hits}
        web_only = sorted(web_paths - api_paths)[:40]
        api_only = sorted(api_paths - web_paths)[:40]
        report_parts.extend(
            [
                "=== Comparison summary ===",
                f"API keyword hits: {len(api_hits)}",
                f"Web keyword hits: {len(web_hits)}",
                "",
                "Sample paths only in WEB payload (first 40):",
                *[f"  {p}" for p in web_only],
                "",
                "Sample paths only in API payload (first 40):",
                *[f"  {p}" for p in api_only],
                "",
            ]
        )

    # Conclusion block
    raw = api_bundle.get("matched_video_raw") or {}
    has_vtt = bool((raw.get("voice_to_text") or "").strip())
    sticker_names = [
        (s.get("sticker_name") or "").strip()
        for s in (raw.get("sticker_info_list") or [])
        if (s.get("sticker_name") or "").strip()
    ]
    report_parts.extend(
        [
            "=== Conclusion (API) ===",
            f"voice_to_text present: {has_vtt} ({len(raw.get('voice_to_text') or '')} chars)",
            f"sticker_info_list present: {bool(raw.get('sticker_info_list'))}",
            f"sticker_info_list non-empty names: {sticker_names}",
            f"video_description present: {bool(raw.get('video_description'))}",
            "",
            "No field named 'ocr' in Research API or web itemStruct.",
            "On-screen overlay text (creator stickers, NOT frame OCR):",
            "  API: sticker_info_list[].sticker_name",
            "  Web: stickersOnItem[].stickerText",
            "Speech/closed-caption text:",
            "  API: voice_to_text",
            "  Web: video.claInfo.captionInfos (WebVTT URLs)",
            "Frame-level OCR for screenshots/burned-in text: NOT in API or web JSON.",
            "",
        ]
    )

    if args.compare_web:
        item = web_bundle.get("item_struct") or {}
        stickers = item.get("stickersOnItem") or []
        cla = (item.get("video") or {}).get("claInfo") or {}
        report_parts.extend(
            [
                "=== Conclusion (Web) ===",
                f"stickersOnItem count: {len(stickers)}",
                f"stickerText blocks: {sum(len(s.get('stickerText') or []) for s in stickers)}",
                f"claInfo.captionInfos count: {len(cla.get('captionInfos') or [])}",
                "",
                "Web-only on-screen overlay text: stickersOnItem[].stickerText",
                "Web closed captions: video.claInfo.captionInfos (WebVTT URLs)",
                "",
            ]
        )

    report = "\n".join(report_parts)
    with open(args.output_search, "w", encoding="utf-8") as f:
        f.write(report)

    print(report)
    logger.info("Wrote %s", args.output_search)

    if not api_bundle.get("matched"):
        logger.error("Video not found in Research API")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
