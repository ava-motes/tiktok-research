"""Classify OCR text into research-relevant content sources.

Engine (google_vision) is separate from *content* source labels such as
``video_overlay``, ``twitter_screenshot``, ``green_screen``, etc.
"""

from __future__ import annotations

import json
import re
from typing import Iterable, List, Sequence

# Content-source labels (stored in ocr_sources JSON)
VIDEO_OVERLAY = "video_overlay"
TWITTER_SCREENSHOT = "twitter_screenshot"
TRUTH_SOCIAL_SCREENSHOT = "truth_social_screenshot"
GREEN_SCREEN = "green_screen"
NEWS_SCREENSHOT = "news_screenshot"
STITCH_CONTENT = "stitch_content"
GENERIC_SCREENSHOT = "screenshot"
UNKNOWN = "unknown"

_TWITTER = re.compile(
    r"(?i)\b(twitter|repost(?:ed)?|retweet|quote\s*tweet|followers?|following)\b"
    r"|\bx\.com\b|@\w{2,}|\b\d+\s*[mhd]\b.*@"
)
_TRUTH = re.compile(
    r"(?i)\b(truth\s*social|retruth|truth\s*details|@realdonaldtrump)\b|^TRUTH\b"
)
_NEWS = re.compile(
    r"(?i)\b(source:\s*|al\s*jazeera|reuters|associated\s*press|\bap\b|"
    r"times of israel|cnn|bbc|minutes?\s*read|file:\s*)"
)
_STITCH = re.compile(r"(?i)\b(stitch|duet with|original sound)\b")
_OVERLAY = re.compile(
    r"(?i)^[A-Z0-9\s\.\,\!\?'\-]{3,80}$|"
    r"\b(breaking|watch|listen|follow|subscribe)\b"
)


def classify_ocr_text(text: str) -> List[str]:
    """Return zero or more content-source labels for one OCR blob."""
    t = (text or "").strip()
    if not t:
        return []
    labels: List[str] = []
    if _TRUTH.search(t):
        labels.append(TRUTH_SOCIAL_SCREENSHOT)
        # Truth Social often appears via green-screen in creator videos
        if len(t) > 80:
            labels.append(GREEN_SCREEN)
    if _TWITTER.search(t):
        labels.append(TWITTER_SCREENSHOT)
    if _NEWS.search(t):
        labels.append(NEWS_SCREENSHOT)
    if _STITCH.search(t):
        labels.append(STITCH_CONTENT)
    # Dense multi-line blocks look like screenshots even without brand markers
    lines = [ln for ln in t.splitlines() if ln.strip()]
    if len(lines) >= 4 and len(t) > 120 and GENERIC_SCREENSHOT not in labels:
        if not any(
            x in labels
            for x in (TWITTER_SCREENSHOT, TRUTH_SOCIAL_SCREENSHOT, NEWS_SCREENSHOT)
        ):
            labels.append(GENERIC_SCREENSHOT)
    # Short / caption-like text → overlay
    if len(t) <= 120 or (len(lines) <= 3 and _OVERLAY.search(t)):
        labels.append(VIDEO_OVERLAY)
    elif VIDEO_OVERLAY not in labels and not labels:
        labels.append(VIDEO_OVERLAY)
    # Deduplicate preserving order
    seen = []
    for lab in labels:
        if lab not in seen:
            seen.append(lab)
    return seen or [UNKNOWN]


def classify_ocr_rows(rows: Sequence[dict]) -> List[str]:
    """Union of content-source labels across OCR frame rows."""
    seen: List[str] = []
    for r in rows:
        for lab in classify_ocr_text(r.get("ocr_text") or ""):
            if lab not in seen:
                seen.append(lab)
    return seen


def ocr_sources_json(rows: Sequence[dict]) -> str:
    """JSON array string for BigQuery ``ocr_sources`` field."""
    return json.dumps(classify_ocr_rows(rows), ensure_ascii=False)


def merge_source_lists(*lists: Iterable[str]) -> List[str]:
    out: List[str] = []
    for lst in lists:
        for item in lst:
            if item and item not in out:
                out.append(item)
    return out
