"""Normalize and merge multimodal visual text layers (API, hydration, EasyOCR)."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple

# Minimum combined visual text length before EasyOCR fallback is considered worthwhile
DEFAULT_VISUAL_TEXT_THRESHOLD = 10

# Broad emoji / pictograph capture (includes ZWJ sequences when adjacent)
_EMOJI_RE = re.compile(
    "["
    "\U0001F1E0-\U0001F1FF"
    "\U0001F300-\U0001FAFF"
    "\U00002600-\U000027BF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "\U0000FE0F"
    "\U0000200D"
    "]+",
    flags=re.UNICODE,
)

_SOURCE_API = "api_sticker"
_SOURCE_BROWSER = "browser_hydration"
_SOURCE_EASYOCR = "easyocr"
_PRIORITY_ORDER = (_SOURCE_API, _SOURCE_BROWSER, _SOURCE_EASYOCR)


def normalize_visual_text(text: Optional[str]) -> str:
    """Normalize text for comparison and light cleanup.

    - collapse whitespace and duplicate ``---`` separators
    - trim repeated blank lines
    - lowercase (for dedup keys; combined output keeps source casing)
    """
    if not text:
        return ""
    s = text.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\n-{2,}\n", "\n---\n", s)
    s = re.sub(r"(\n---\n)+", "\n---\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    lines = [ln.strip() for ln in s.split("\n")]
    out_lines: List[str] = []
    prev_blank = False
    for ln in lines:
        if not ln:
            if not prev_blank:
                out_lines.append("")
            prev_blank = True
            continue
        prev_blank = False
        out_lines.append(ln)
    while out_lines and not out_lines[0]:
        out_lines.pop(0)
    while out_lines and not out_lines[-1]:
        out_lines.pop()
    return "\n".join(out_lines).lower()


def extract_emojis(*texts: Optional[str]) -> str:
    """Return unique emojis from one or more text blobs, in order of first appearance."""
    seen: Set[str] = set()
    ordered: List[str] = []
    for text in texts:
        if not text:
            continue
        for match in _EMOJI_RE.finditer(str(text)):
            glyph = match.group(0)
            if glyph not in seen:
                seen.add(glyph)
                ordered.append(glyph)
    return "".join(ordered)


def extract_emojis_from_sticker_info(sticker_info_list: Optional[str]) -> str:
    """Pull emojis from raw Research API ``sticker_info_list`` JSON."""
    if not sticker_info_list or not str(sticker_info_list).strip():
        return ""
    try:
        payload = json.loads(sticker_info_list)
    except (json.JSONDecodeError, TypeError):
        return extract_emojis(sticker_info_list)
    if not isinstance(payload, list):
        return extract_emojis(sticker_info_list)
    parts: List[str] = []
    for item in payload:
        if isinstance(item, dict):
            parts.append(str(item.get("sticker_name") or ""))
        else:
            parts.append(str(item))
    return extract_emojis(*parts)


def _line_key(line: str) -> str:
    return normalize_visual_text(line)


def _split_into_lines(text: Optional[str]) -> List[str]:
    if not text or not str(text).strip():
        return []
    raw = str(text).replace("\r\n", "\n").replace("\r", "\n")
    parts: List[str] = []
    for block in re.split(r"\n-{2,}\n", raw):
        for ln in block.split("\n"):
            ln = ln.strip()
            if ln:
                parts.append(ln)
    return parts


def has_sufficient_visual_text(
    text: Optional[str],
    threshold: int = DEFAULT_VISUAL_TEXT_THRESHOLD,
) -> bool:
    return len((text or "").strip()) >= threshold


def needs_easyocr_fallback(
    visual_text_combined: Optional[str],
    threshold: int = DEFAULT_VISUAL_TEXT_THRESHOLD,
) -> bool:
    """True when merged visual text is still too sparse for research use."""
    return not has_sufficient_visual_text(visual_text_combined, threshold)


def merge_visual_text_sources(
    sticker_overlay_text: Optional[str] = None,
    browser_ocr_text: Optional[str] = None,
    onscreen_text: Optional[str] = None,
) -> Dict[str, Any]:
    """Merge visual layers with dedup and source priority (API → hydration → EasyOCR).

    Returns:
        visual_text_combined: newline-joined unique lines (original casing preserved)
        visual_text_sources: provenance metadata for each layer
    """
    layers: List[Tuple[str, Optional[str]]] = [
        (_SOURCE_API, sticker_overlay_text),
        (_SOURCE_BROWSER, browser_ocr_text),
        (_SOURCE_EASYOCR, onscreen_text),
    ]

    seen: Set[str] = set()
    combined_lines: List[str] = []
    source_meta: Dict[str, Dict[str, Any]] = {}

    for source_id, raw in layers:
        lines = _split_into_lines(raw)
        lines_used = 0
        for ln in lines:
            key = _line_key(ln)
            if not key or key in seen:
                continue
            seen.add(key)
            combined_lines.append(ln)
            lines_used += 1
        source_meta[source_id] = {
            "present": bool((raw or "").strip()),
            "char_count": len((raw or "").strip()),
            "line_count": len(lines),
            "lines_used": lines_used,
        }

    combined = "\n".join(combined_lines)
    active = [s for s in _PRIORITY_ORDER if source_meta[s]["lines_used"] > 0]

    return {
        "visual_text_combined": combined,
        "visual_text_sources": {
            "priority_order": list(_PRIORITY_ORDER),
            "active_sources": active,
            "primary_source": active[0] if active else None,
            "combined_line_count": len(combined_lines),
            "combined_char_count": len(combined),
            "sources": source_meta,
        },
    }


def line_overlap_ratio(a: Optional[str], b: Optional[str]) -> float:
    """Jaccard similarity of normalized lines between two visual text blobs."""
    la = {_line_key(x) for x in _split_into_lines(a) if _line_key(x)}
    lb = {_line_key(x) for x in _split_into_lines(b) if _line_key(x)}
    if not la and not lb:
        return 1.0
    if not la or not lb:
        return 0.0
    return len(la & lb) / len(la | lb)
