"""Extract video metadata from TikTok web hydration (browser landing page).

TikTok stores burned-in / visually detected on-screen text in the landing-page
``itemStruct`` payload under ``stickersOnItem[].stickerText`` (often
``stickerType`` 4). That text is produced by TikTok's internal OCR/parsing, not
by client-side frame OCR. The Research API exposes the same content via
``sticker_info_list`` / ``sticker_overlay_text`` when available.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

_HYDRATION_RE = re.compile(
    r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([^<]+)</script>',
    re.DOTALL,
)
_DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def fetch_hydration_html(url: str, timeout: int = 30) -> str:
    resp = requests.get(
        url,
        headers={"User-Agent": _DEFAULT_UA},
        timeout=timeout,
        allow_redirects=True,
    )
    resp.raise_for_status()
    return resp.text


def parse_item_struct(html: str) -> Optional[Dict[str, Any]]:
    m = _HYDRATION_RE.search(html)
    if not m:
        return None
    data = json.loads(m.group(1))
    return (
        data.get("__DEFAULT_SCOPE__", {})
        .get("webapp.video-detail", {})
        .get("itemInfo", {})
        .get("itemStruct")
    )


def extract_sticker_texts(item: Dict[str, Any]) -> List[str]:
    """On-screen text lines from ``stickersOnItem`` (TikTok OCR / overlay store)."""
    out: List[str] = []
    for sticker in item.get("stickersOnItem") or []:
        for line in sticker.get("stickerText") or []:
            t = (line or "").strip()
            if t:
                out.append(t)
    return out


def join_sticker_texts(texts: List[str]) -> str:
    if not texts:
        return ""
    return "\n---\n".join(texts)


def extract_onscreen_text_from_item(item: Dict[str, Any]) -> str:
    """Normalized on-screen text from hydration ``itemStruct``."""
    return join_sticker_texts(extract_sticker_texts(item))


def fetch_web_onscreen_text(url: str, timeout: int = 30) -> Dict[str, Any]:
    """Fetch landing page and return TikTok-native on-screen text only.

    Returns dict with ``text``, ``video_id``, ``username``, ``error`` (if any).
    """
    try:
        html = fetch_hydration_html(url, timeout=timeout)
        item = parse_item_struct(html)
        if not item:
            return {"url": url, "text": "", "error": "hydration_item_struct_missing"}
        author = item.get("author") or {}
        return {
            "url": url,
            "video_id": str(item.get("id") or ""),
            "username": author.get("uniqueId") or "",
            "text": extract_onscreen_text_from_item(item),
            "sticker_texts": extract_sticker_texts(item),
        }
    except Exception as e:
        logger.warning("Web on-screen text fetch failed for %s: %s", url, e)
        return {"url": url, "text": "", "error": str(e)}


def caption_info_urls(item: Dict[str, Any]) -> List[Dict[str, str]]:
    cla = (item.get("video") or {}).get("claInfo") or {}
    infos = []
    for cap in cla.get("captionInfos") or []:
        url = cap.get("url") or cap.get("Url")
        if url:
            infos.append(
                {
                    "language": cap.get("language") or cap.get("Language") or "",
                    "format": cap.get("captionFormat") or cap.get("format") or "",
                    "url": url,
                }
            )
    for sub in (item.get("video") or {}).get("subtitleInfos") or []:
        url = sub.get("Url") or sub.get("url")
        if url and not any(i["url"] == url for i in infos):
            infos.append(
                {
                    "language": sub.get("LanguageCodeName") or "",
                    "format": "subtitleInfos",
                    "url": url,
                }
            )
    return infos


def webvtt_to_plain_text(vtt: str) -> str:
    """Strip WEBVTT timing lines; return cue text joined by newlines."""
    lines: List[str] = []
    for raw in vtt.splitlines():
        line = raw.strip()
        if not line or line == "WEBVTT":
            continue
        if line.startswith("NOTE"):
            continue
        if "-->" in line:
            continue
        if re.match(r"^\d+$", line):
            continue
        if re.match(r"^[\d:.]+\s+--> ", line):
            continue
        lines.append(line)
    return "\n".join(lines)


def fetch_url_text(url: str, timeout: int = 30) -> str:
    resp = requests.get(url, headers={"User-Agent": _DEFAULT_UA}, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def fetch_closed_captions(item: Dict[str, Any]) -> Dict[str, Any]:
    """Download WebVTT (or similar) caption tracks referenced in hydration."""
    tracks: List[Dict[str, Any]] = []
    combined: List[str] = []
    for info in caption_info_urls(item):
        try:
            body = fetch_url_text(info["url"])
            plain = webvtt_to_plain_text(body)
            tracks.append({**info, "char_count": len(plain), "text": plain})
            if plain:
                combined.append(plain)
        except Exception as e:
            logger.warning("Caption fetch failed (%s): %s", info.get("language"), e)
            tracks.append({**info, "error": str(e), "text": ""})
    return {
        "tracks": tracks,
        "text": "\n\n".join(combined),
    }


def collect_web_video_metadata(url: str) -> Dict[str, Any]:
    """Fetch landing page and return normalized web metadata for one video."""
    html = fetch_hydration_html(url)
    item = parse_item_struct(html)
    if not item:
        return {"url": url, "error": "hydration_item_struct_missing"}

    video = item.get("video") or {}
    author = item.get("author") or {}
    stickers = extract_sticker_texts(item)
    captions = fetch_closed_captions(item)

    return {
        "url": url,
        "video_id": str(item.get("id") or ""),
        "username": author.get("uniqueId") or "",
        "display_name": author.get("nickname") or "",
        "caption": item.get("desc") or "",
        "web_sticker_texts": stickers,
        "web_sticker_text": join_sticker_texts(stickers),
        "web_onscreen_text": join_sticker_texts(stickers),
        "web_closed_captions": captions["text"],
        "web_closed_caption_tracks": captions["tracks"],
        "cla_info": video.get("claInfo"),
        "has_stickers": bool(stickers),
        "has_web_captions": bool(captions["text"]),
        "canonical_url": (
            f"https://www.tiktok.com/@{author.get('uniqueId', '')}/video/{item.get('id', '')}"
            if author.get("uniqueId") and item.get("id")
            else url
        ),
    }
