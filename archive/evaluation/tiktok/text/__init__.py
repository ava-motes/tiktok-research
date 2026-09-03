"""Multimodal text normalization and visual-layer merging."""

from tiktok.text.normalize import (
    DEFAULT_VISUAL_TEXT_THRESHOLD,
    has_sufficient_visual_text,
    line_overlap_ratio,
    merge_visual_text_sources,
    needs_easyocr_fallback,
    normalize_visual_text,
)

__all__ = [
    "DEFAULT_VISUAL_TEXT_THRESHOLD",
    "has_sufficient_visual_text",
    "line_overlap_ratio",
    "merge_visual_text_sources",
    "needs_easyocr_fallback",
    "normalize_visual_text",
]
