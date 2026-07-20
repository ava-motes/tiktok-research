"""TikTok web landing-page metadata (hydration JSON, not Research API)."""

from tiktok.web.metadata import (
    collect_web_video_metadata,
    extract_onscreen_text_from_item,
    fetch_web_onscreen_text,
)

__all__ = [
    "collect_web_video_metadata",
    "extract_onscreen_text_from_item",
    "fetch_web_onscreen_text",
]
