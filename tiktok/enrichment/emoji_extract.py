"""Emoji extraction with Unicode / CLDR names, counts, and source tracking.

Preserves emoji→description mapping for coded-language analysis (e.g. 🧊 ice cube).
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from typing import Any, Dict, List, Sequence, Tuple

logger = logging.getLogger(__name__)

# Exclude trademark / punctuation / UI chrome that are not research emojis
_BLOCKED_CODEPOINTS = {
    0x00AE,  # ®
    0x00A9,  # ©
    0x2122,  # ™
    0x2022,  # •
    0x00B0,  # °
    0x2611,  # ☑ ballot box with check
    0x2610,  # ☐
    0x2713,  # ✓
    0x2714,  # ✔
    0x26AA,  # ⚪
    0x26AB,  # ⚫
    0x25CF,  # ●
    0x25CB,  # ○
    0x25A0,  # ■
    0x25A1,  # □
    0x200D,  # ZWJ alone
    0xFE0F,  # variation selector alone
}

# Research-critical CLDR fallbacks when ``emoji`` package is unavailable
_CLDR_FALLBACK = {
    "🧊": "ice cube",
    "🔫": "water pistol",
    "🍉": "watermelon",
    "🔥": "fire",
    "❤️": "red heart",
    "❤": "red heart",
    "😂": "face with tears of joy",
    "😭": "loudly crying face",
    "🚨": "police car light",
    "🇺🇸": "flag United States",
}


def _is_blocked(glyph: str) -> bool:
    if not glyph:
        return True
    cps = {ord(c) for c in glyph}
    if cps & _BLOCKED_CODEPOINTS:
        return True
    # Pure ASCII / digits are never emoji for our purposes
    if all(ord(c) < 128 for c in glyph):
        return True
    return False


def _iter_emoji_glyphs(text: str) -> List[str]:
    """Return individual emoji glyphs (prefer ``emoji`` package segmentation)."""
    if not text:
        return []
    glyphs: List[str] = []
    try:
        import emoji as emoji_lib

        found = emoji_lib.emoji_list(str(text))
        glyphs = [item["emoji"] for item in found if item.get("emoji")]
    except Exception:
        pattern = re.compile(
            "(?:"
            "[\U0001F1E0-\U0001F1FF]{2}"
            "|"
            "[\U0001F300-\U0001FAFF]"
            "[\U0000FE0F\U0000200D\U0001F3FB-\U0001F3FF]*"
            "|"
            "[\U00002600-\U000027BF]\U0000FE0F?"
            "|"
            "[\U0001F600-\U0001F64F]"
            "|"
            "[\U0001F680-\U0001F6FF]"
            "|"
            "[\U0001F900-\U0001F9FF]"  # includes 🧊 ice cube
            ")",
            flags=re.UNICODE,
        )
        glyphs = pattern.findall(str(text))
    return [g for g in glyphs if not _is_blocked(g)]


def _demojize_name(glyph: str) -> str:
    if glyph in _CLDR_FALLBACK:
        # Prefer package name when available; fallback map is authoritative for codes
        pass
    try:
        import emoji as emoji_lib

        raw = emoji_lib.demojize(glyph, language="en")
        name = raw.strip(":").replace("_", " ").strip()
        if name and name != glyph:
            # Normalize common renames for research clarity
            if name == "pistol":
                return "water pistol"
            if name in ("ice", "ice_cube"):
                return "ice cube"
            if glyph in _CLDR_FALLBACK and name.replace(" ", "_") in (
                "ice",
                "pistol",
            ):
                return _CLDR_FALLBACK[glyph]
            # Prefer explicit research fallbacks when package is overly short
            if glyph == "🧊":
                return "ice cube"
            return name
    except Exception:
        pass
    if glyph in _CLDR_FALLBACK:
        return _CLDR_FALLBACK[glyph]
    try:
        cps = " ".join(f"U+{ord(c):04X}" for c in glyph if ord(c) != 0xFE0F)
        return cps or "unknown_emoji"
    except Exception:
        return "unknown_emoji"


def emoji_codepoint(glyph: str) -> str:
    """Unicode codepoint string, e.g. U+1F9CA or U+1F1FA U+1F1F8 for flags."""
    parts = []
    for c in glyph:
        if ord(c) in (0xFE0F, 0x200D):  # variation selector / ZWJ — keep for fidelity
            parts.append(f"U+{ord(c):04X}")
            continue
        parts.append(f"U+{ord(c):04X}")
    return " ".join(parts)


def emoji_kind(glyph: str) -> str:
    """High-level kind: flag | emoji | symbol | modifier."""
    cps = [ord(c) for c in glyph]
    if any(0x1F1E6 <= c <= 0x1F1FF for c in cps) and len([c for c in cps if 0x1F1E6 <= c <= 0x1F1FF]) >= 2:
        return "flag"
    if any(0x1F3FB <= c <= 0x1F3FF for c in cps) and len(cps) == 1:
        return "modifier"
    # Dingbats / enclosed marks often OCR noise or UI chrome
    if any(c in (0x2611, 0x2713, 0x2714, 0x26AA, 0x26AB, 0x25CF, 0x25CB) for c in cps):
        return "symbol"
    if any(0x2600 <= c <= 0x26FF for c in cps) and not any(0x1F300 <= c <= 0x1FAFF for c in cps):
        # misc symbols — keep as symbol unless clearly emoji-presentational
        if any(c in (0x2764, 0x2665) for c in cps):  # hearts
            return "emoji"
        return "symbol"
    return "emoji"


def _category_for(glyph: str, name: str) -> str:
    """Unicode-ish semantic category for research (object/food/flag/emotion/...)."""
    n = name.lower()
    kind = emoji_kind(glyph)
    if kind == "flag" or "flag" in n:
        return "flag"
    if kind == "modifier":
        return "modifier"
    if kind == "symbol":
        return "symbol"
    cps = {ord(c) for c in glyph}
    if 0x1F9CA in cps or "ice cube" in n:
        return "object"
    if "watermelon" in n or any(x in n for x in ("food", "fruit", "pizza", "coffee", "beer", "drink")):
        return "food"
    if any(x in n for x in ("face", "smile", "cry", "angry", "kiss", "heart-eyes", "tears")):
        return "emotion"
    if "heart" in n:
        return "emotion"
    if any(x in n for x in ("fire", "sparkles", "collision", "boom", "100")):
        return "emphasis"
    if any(x in n for x in ("hand", "thumb", "clap", "wave", "point", "fist", "pray", "shrug")):
        return "gesture"
    if any(x in n for x in ("person", "man", "woman", "baby", "family")):
        return "people"
    if any(x in n for x in ("animal", "dog", "cat", "bird", "fish", "duck")):
        return "animal"
    if any(x in n for x in ("pistol", "gun", "knife", "bomb")):
        return "object"
    if any(x in n for x in ("police", "car", "light", "ambulance")):
        return "object"
    if any(x in n for x in ("music", "microphone", "note", "camera", "movie")):
        return "media"
    return "object"


def annotate_emoji(glyph: str) -> Tuple[str, str]:
    name = _demojize_name(glyph)
    return name, _category_for(glyph, name)


def extract_emoji_rows_from_text(text: str, text_source: str) -> List[Dict]:
    glyphs = _iter_emoji_glyphs(text or "")
    if not glyphs:
        return []
    counts = Counter(glyphs)
    rows = []
    seen = set()
    for glyph in glyphs:
        if glyph in seen:
            continue
        seen.add(glyph)
        kind = emoji_kind(glyph)
        # Drop non-semantic UI symbols; keep emoji / flag / modifier
        if kind == "symbol":
            continue
        name, category = annotate_emoji(glyph)
        rows.append(
            {
                "text_source": text_source,
                "emoji": glyph,
                "emoji_name": name,
                "emoji_category": category,
                "emoji_codepoint": emoji_codepoint(glyph),
                "emoji_kind": kind,
                "count": int(counts[glyph]),
            }
        )
    return rows


def extract_emoji_rows_for_video(row: Dict) -> List[Dict]:
    """Pull emojis from all available text layers on a video row."""
    sources = [
        ("caption", row.get("caption") or row.get("description")),
        ("hashtags", row.get("hashtags")),
        ("transcript", row.get("transcript") or row.get("voice_to_text")),
        ("ocr", row.get("ocr_text") or row.get("visual_text_combined") or row.get("onscreen_text")),
        ("browser_ocr", row.get("browser_ocr_text")),
        ("sticker", row.get("sticker_overlay_text")),
    ]
    # Merge rows for same emoji across sources → keep per-source rows (for location)
    out: List[Dict] = []
    for source, text in sources:
        out.extend(extract_emoji_rows_from_text(text or "", source))
    return out


def aggregate_emoji_fields(rows: Sequence[dict]) -> Dict[str, Any]:
    """Build final-table emoji fields from staging rows.

    Returns:
      emoji_characters, emoji_descriptions, emoji_count, emoji_sources (JSON),
      plus legacy emojis / emoji_names / emoji_categories.
    """
    if not rows:
        return {
            "emoji_characters": "",
            "emoji_descriptions": "",
            "emoji_category": "",
            "emoji_source": "",
            "emoji_count": 0,
            "emoji_sources": "[]",
            "emoji_codepoints": "",
            "emoji_kinds": "",
            "emojis": "",
            "emoji_names": "",
            "emoji_categories": "",
        }

    # glyph -> {name, category, total_count, sources:set}
    agg: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        g = (r.get("emoji") or "").strip()
        if not g or _is_blocked(g):
            continue
        name = r.get("emoji_name") or annotate_emoji(g)[0]
        cat = r.get("emoji_category") or annotate_emoji(g)[1]
        cnt = int(r.get("count") or 1)
        src = (r.get("text_source") or "unknown").strip()
        kind = r.get("emoji_kind") or emoji_kind(g)
        cp = r.get("emoji_codepoint") or emoji_codepoint(g)
        if g not in agg:
            agg[g] = {
                "emoji": g,
                "description": name,
                "category": cat,
                "kind": kind,
                "codepoint": cp,
                "count": 0,
                "sources": [],
            }
        agg[g]["count"] += cnt
        if src and src not in agg[g]["sources"]:
            agg[g]["sources"].append(src)

    ordered = sorted(agg.values(), key=lambda x: (-x["count"], x["emoji"]))
    chars = [x["emoji"] for x in ordered]
    descs = [x["description"] for x in ordered]
    cats = []
    for x in ordered:
        if x["category"] not in cats:
            cats.append(x["category"])
    total = sum(x["count"] for x in ordered)
    sources_payload = [
        {
            "emoji": x["emoji"],
            "description": x["description"],
            "codepoint": x["codepoint"],
            "category": x["category"],
            "kind": x["kind"],
            "count": x["count"],
            "sources": x["sources"],
        }
        for x in ordered
    ]
    # Flatten unique text sources across all glyphs (caption|ocr|transcript|...)
    flat_sources: List[str] = []
    for x in ordered:
        for s in x["sources"]:
            if s and s not in flat_sources:
                flat_sources.append(s)
    return {
        "emoji_characters": " | ".join(chars),
        "emoji_descriptions": " | ".join(descs),
        "emoji_category": " | ".join(cats),
        "emoji_source": " | ".join(flat_sources),
        "emoji_count": total,
        "emoji_sources": json.dumps(sources_payload, ensure_ascii=False),
        "emoji_codepoints": " | ".join(x["codepoint"] for x in ordered),
        "emoji_kinds": " | ".join(list(dict.fromkeys(x["kind"] for x in ordered))),
        "emojis": " | ".join(chars),
        "emoji_names": " | ".join(descs),
        "emoji_categories": " | ".join(cats),
    }


def emoji_context_words(text: str, glyph: str, window: int = 4) -> List[str]:
    """Return nearby words around an emoji occurrence (for co-occurrence analysis)."""
    if not text or not glyph:
        return []
    words: List[str] = []
    for m in re.finditer(re.escape(glyph), text):
        start = max(0, m.start() - 80)
        end = min(len(text), m.end() + 80)
        chunk = text[start:end]
        toks = re.findall(r"[A-Za-z]{3,}", chunk)
        # drop the emoji neighborhood noise
        words.extend(toks[:window] + toks[-window:])
    # unique preserve order
    seen = []
    for w in words:
        wl = w.lower()
        if wl not in seen:
            seen.append(wl)
    return seen[:20]
