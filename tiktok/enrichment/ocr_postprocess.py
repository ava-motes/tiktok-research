"""OCR post-processing: clean fragmented Vision text and build segments.

Google Vision remains the OCR engine. This module only reshapes outputs for
research use (reading order, dedupe, source labels, quality scoring).
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from tiktok.enrichment.ocr_sources import classify_ocr_text

# Prefer more specific content labels when emitting a single source_type
_SOURCE_PRIORITY = [
    "truth_social_screenshot",
    "twitter_screenshot",
    "news_screenshot",
    "green_screen",
    "stitch_content",
    "screenshot",
    "video_overlay",
    "unknown",
]

# UI / player chrome that is almost never research content
_UI_ARTIFACT_RE = re.compile(
    r"(?i)\b("
    r"like|comment|share|follow|following|subscribers?|views?|"
    r"for\s*you|fyp|live|sponsored|shop\s*now|learn\s*more|"
    r"tiktok|capcut|duet|stitch\s*with"
    r")\b"
)
_SYMBOLS_ONLY_RE = re.compile(r"^[\W_\d\s]+$", re.UNICODE)
_GARBAGE_EXACT = {
    "*",
    ".",
    "-",
    "—",
    "•",
    "·",
    "o",
    "O",
    "G",
    "0",
    "00",
    "000",
    "0000",
    "100",
    "SEC",
    "DOL",
    "e",
    "B",
}

# UI / player chrome tokens mixed into OCR (e.g. "B 1000", "8 O", "12")
# Do NOT match Roman numerals (II) or mid-phrase numbers (AMERICA 250).
_UI_TOKEN_RE = re.compile(
    r"(?ix)^("
    r"[A-Z]\s*\d{1,5}"          # B 1000, B 8
    r"|\d{1,3}\s*[A-Z]"         # 8 O
    r")$"
)
_BARE_UI_NUMBER_RE = re.compile(r"^\d{1,4}$")


def primary_source_type(text: str) -> str:
    labels = classify_ocr_text(text)
    if not labels:
        return "unknown"
    for pref in _SOURCE_PRIORITY:
        if pref in labels:
            return pref
    return labels[0]


def alpha_ratio(text: str) -> float:
    if not text:
        return 0.0
    letters = sum(1 for c in text if c.isalpha())
    return letters / max(len(text), 1)


def is_garbage_ocr_text(text: str, *, min_confidence: Optional[float] = None, confidence: Optional[float] = None) -> bool:
    """True when a block should be dropped from cleaned OCR (keep in raw)."""
    t = (text or "").strip()
    if not t:
        return True
    if confidence is not None and min_confidence is not None and confidence < min_confidence:
        return True
    compact = re.sub(r"\s+", " ", t).strip()
    if compact in _GARBAGE_EXACT:
        return True
    if _UI_TOKEN_RE.match(compact):
        return True
    if len(compact) <= 2 and alpha_ratio(compact) < 0.5:
        return True
    # Pure symbols / digits / whitespace (e.g. "0000 100", "0 6 G")
    letters = re.sub(r"[^A-Za-z]", "", compact)
    if len(compact) <= 24 and len(letters) <= 2:
        return True
    if _SYMBOLS_ONLY_RE.match(compact) and len(letters) < 3:
        return True
    # Mostly digits/punctuation with almost no words
    words = re.findall(r"[A-Za-z]{3,}", compact)
    if len(compact) <= 40 and not words and alpha_ratio(compact) < 0.35:
        return True
    # Lone UI chrome fragments
    if len(compact) <= 28 and _UI_ARTIFACT_RE.fullmatch(compact.replace("!", "").strip()):
        return True
    # After stripping UI tokens, nothing meaningful remains
    if not strip_ui_tokens(compact) and len(compact) <= 40:
        if not re.search(r"[A-Za-z]{4,}", compact):
            return True
    return False


def strip_ui_tokens(text: str) -> str:
    """Remove short UI chrome tokens like 'B 1000', '8 O', leading frame numbers.

    Preserves mid-phrase numbers (e.g. AMERICA 250) and Roman numerals (II).
    """
    if not text:
        return ""
    tokens = re.findall(r"\S+", text)
    cleaned: List[str] = []
    skip_next = False
    for idx, tok in enumerate(tokens):
        if skip_next:
            skip_next = False
            continue
        if tok in _GARBAGE_EXACT:
            continue
        nxt = tokens[idx + 1] if idx + 1 < len(tokens) else ""
        prev = cleaned[-1] if cleaned else ""
        pair = f"{tok} {nxt}".strip()
        if nxt and _UI_TOKEN_RE.match(pair):
            skip_next = True
            continue
        if _UI_TOKEN_RE.match(tok):
            continue
        # Bare numbers: keep when attached to a word (AMERICA 250 / top 10).
        # Drop only leading/orphan counters (e.g. line starting with "12").
        if _BARE_UI_NUMBER_RE.match(tok):
            prev_alpha = bool(re.search(r"[A-Za-z]", prev))
            next_alpha = bool(re.search(r"[A-Za-z]", nxt))
            if prev_alpha or next_alpha:
                cleaned.append(tok)
                continue
            continue
        cleaned.append(tok)
    return re.sub(r"\s+", " ", " ".join(cleaned)).strip()


def find_persistent_phrases(
    frame_texts: Sequence[str],
    *,
    min_frame_ratio: float = 0.4,
    min_frames: int = 3,
) -> List[str]:
    """Phrases that appear across many frames (flags, watermarks, fixed overlays)."""
    frames = [re.sub(r"\s+", " ", (t or "").strip()) for t in frame_texts if (t or "").strip()]
    if len(frames) < min_frames:
        return []
    # Candidate 2–4 word phrases, prefer ALLCAPS / short brand-like overlays
    counts: Dict[str, int] = {}
    for fr in frames:
        seen_in_frame = set()
        words = fr.split()
        for n in (2, 3, 4):
            for i in range(0, max(0, len(words) - n + 1)):
                phrase = " ".join(words[i : i + n])
                letters = re.sub(r"[^A-Za-z]", "", phrase)
                if len(letters) < 4:
                    continue
                # Prefer overlay-like phrases (mostly caps or short)
                caps = sum(1 for c in letters if c.isupper())
                if caps / max(len(letters), 1) < 0.6 and n > 2:
                    continue
                key = phrase.upper()
                if key in seen_in_frame:
                    continue
                seen_in_frame.add(key)
                counts[key] = counts.get(key, 0) + 1
    threshold = max(min_frames, int(round(len(frames) * min_frame_ratio)))
    persistent = [p for p, n in counts.items() if n >= threshold]
    # Prefer longer phrases; drop subsumed shorter ones
    persistent.sort(key=lambda p: (-len(p), p))
    kept: List[str] = []
    for p in persistent:
        if any(p != k and p in k for k in kept):
            continue
        kept.append(p)
    return kept


def strip_phrases(text: str, phrases: Sequence[str]) -> str:
    if not text:
        return ""
    out = text
    for phrase in sorted(phrases, key=len, reverse=True):
        if not phrase:
            continue
        out = re.sub(re.escape(phrase), " ", out, flags=re.IGNORECASE)
    out = strip_ui_tokens(out)
    out = re.sub(r"\s+([,.])", r"\1", out)
    out = re.sub(r"\s+", " ", out).strip(" ,.-")
    return out.strip()


def collapse_persistent_overlays(frame_texts: Sequence[str]) -> str:
    """Keep persistent overlays once; keep unique residual text (headlines).

    Fixes cases like a flag reading AMERICA 250 on every frame while burned-in
    captions change underneath — without repeating the flag on every line.
    """
    cleaned_frames: List[str] = []
    for t in frame_texts:
        c = clean_ocr_text(t or "")
        c = strip_ui_tokens(c)
        if c and not is_garbage_ocr_text(c):
            cleaned_frames.append(c)
    if not cleaned_frames:
        return ""

    persistent = find_persistent_phrases(cleaned_frames)
    residuals: List[str] = []
    seen_norm: List[str] = []
    for fr in cleaned_frames:
        residual = strip_phrases(fr, persistent)
        if not residual or is_garbage_ocr_text(residual):
            continue
        # Drop residuals that are mostly burned-in caption fragments once we
        # already captured a strong headline earlier (ALLCAPS long line).
        norm = _normalize_for_dedupe(residual)
        if not norm:
            continue
        dup = False
        for prev in seen_norm:
            if norm == prev or (len(norm) > 30 and (norm in prev or prev in norm)):
                dup = True
                break
            if _jaccard(_token_set(norm), _token_set(prev)) >= 0.8:
                dup = True
                break
        if dup:
            continue
        seen_norm.append(norm)
        residuals.append(residual)

    # Prefer a clear headline residual (longest ALLCAPS-ish line). Keep only one
    # residual so burned-in caption fragments do not accumulate beside overlays.
    residuals.sort(
        key=lambda s: (
            -sum(1 for c in s if c.isupper()),
            -len(s),
        )
    )
    residuals = residuals[:1]

    parts: List[str] = []
    parts.extend(residuals)
    for p in persistent:
        # Avoid adding overlays already present (incl. shorter core like AMERICA 250)
        blob = " ".join(parts).upper()
        core = " ".join(p.split()[:2])
        if p in blob or (core and core in blob):
            continue
        parts.append(p)

    # If we only found persistent overlays, still return them once
    if not parts and persistent:
        parts = persistent[:3]
    return "\n".join(parts).strip()


def refine_joined_ocr_text(text: str) -> str:
    """Clean already-joined OCR that repeats overlays across lines."""
    if not text:
        return ""
    blocks = [b.strip() for b in re.split(r"\n\s*\n|(?<=\n)", text) if b.strip()]
    # Also split single-line-ish blocks that are newline separated
    if len(blocks) <= 1:
        blocks = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return collapse_persistent_overlays(blocks)


def filter_ocr_blocks(
    texts: Sequence[str],
    *,
    confidences: Optional[Sequence[Optional[float]]] = None,
    min_confidence: float = 0.35,
) -> List[str]:
    """Drop garbage / low-confidence blocks; preserve meaningful overlays."""
    out: List[str] = []
    for i, text in enumerate(texts):
        conf = None
        if confidences is not None and i < len(confidences):
            conf = confidences[i]
        if is_garbage_ocr_text(text, min_confidence=min_confidence, confidence=conf):
            continue
        cleaned = clean_ocr_text(text).strip()
        if not cleaned or is_garbage_ocr_text(cleaned):
            continue
        out.append(cleaned)
    return out


def ocr_unique_text_ratio(parts: Sequence[str]) -> float:
    """Unique normalized blocks / total blocks (1.0 = no repetition)."""
    norms = [_normalize_for_dedupe(p) for p in parts if (p or "").strip()]
    norms = [n for n in norms if n]
    if not norms:
        return 0.0
    return round(len(set(norms)) / len(norms), 4)


def score_ocr_quality(
    *,
    raw_text: str,
    cleaned_text: str,
    unique_ratio: float,
    source_count: int,
    frames_in: int,
    frames_kept: int,
) -> int:
    """0–100 OCR quality score; penalize repetition, garbage, symbol-only text."""
    cleaned = (cleaned_text or "").strip()
    raw = (raw_text or "").strip()
    if not raw and not cleaned:
        return 0
    if not cleaned:
        return 10

    score = 55
    n = len(cleaned)
    if n >= 20:
        score += 15
    if n >= 60:
        score += 10
    if n >= 160:
        score += 5

    ar = alpha_ratio(cleaned)
    if ar >= 0.55:
        score += 15
    elif ar >= 0.35:
        score += 8
    else:
        score -= 20

    if unique_ratio >= 0.85:
        score += 10
    elif unique_ratio >= 0.6:
        score += 4
    elif unique_ratio < 0.4:
        score -= 25
    elif unique_ratio < 0.6:
        score -= 12

    if source_count >= 2:
        score += 5
    if frames_in > 0 and frames_kept > 0:
        keep_ratio = frames_kept / frames_in
        if keep_ratio < 0.25 and frames_in >= 4:
            score -= 10

    # Meaningful headline / news-like cues
    if re.search(r"(?i)\b(breaking|news|trump|congress|tweet|@\w+)\b", cleaned):
        score += 5

    # Hard penalties for leftover garbage
    if is_garbage_ocr_text(cleaned):
        score = min(score, 15)
    if n < 8:
        score = min(score, 25)

    return int(max(0, min(100, score)))


def clean_ocr_text(raw: str) -> str:
    """Join fragmented Vision lines into readable sentences/paragraphs.

    Examples:
      "TRU\\nMP CALLED" → "TRUMP CALLED"
      "THE\\nCREW" → "THE CREW" (when not a mid-word break)
    """
    if not raw:
        return ""
    text = raw.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.strip() for ln in text.split("\n")]
    lines = [ln for ln in lines if ln]

    if not lines:
        return ""

    merged: List[str] = [lines[0]]
    for nxt in lines[1:]:
        prev = merged[-1]
        # Mid-word break: previous ends lowercase/letter, next continues lowercase
        # or previous is short ALLCAPS fragment + next continues ALLCAPS
        if _looks_midword_break(prev, nxt):
            merged[-1] = prev + nxt
            continue
        # Soft wrap: previous doesn't end sentence punct, next starts lowercase
        if (
            prev
            and prev[-1] not in ".!?:;"
            and nxt[:1].islower()
            and not prev.endswith("-")
        ):
            merged[-1] = f"{prev} {nxt}"
            continue
        # Hyphenated wrap: "govern-" + "ment"
        if prev.endswith("-") and nxt[:1].islower():
            merged[-1] = prev[:-1] + nxt
            continue
        # Short ALLCAPS caption fragments on consecutive lines → same phrase
        if _short_caps_fragment(prev) and _short_caps_fragment(nxt):
            merged[-1] = f"{prev} {nxt}"
            continue
        merged.append(nxt)

    # Collapse whitespace inside each paragraph line
    cleaned_lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in merged]
    # Join short consecutive caption-like lines with space; keep blank between dense blocks
    out: List[str] = []
    buf: List[str] = []
    for ln in cleaned_lines:
        dense = len(ln) > 80 or ln.count(" ") >= 8
        if dense and buf:
            out.append(" ".join(buf))
            buf = []
            out.append(ln)
        elif dense:
            out.append(ln)
        else:
            buf.append(ln)
    if buf:
        out.append(" ".join(buf))
    return "\n".join(out).strip()


def _short_caps_fragment(s: str) -> bool:
    letters = re.sub(r"[^A-Za-z]", "", s)
    if not letters or len(s) > 24:
        return False
    return letters.isupper() and len(letters) <= 18


def _looks_midword_break(prev: str, nxt: str) -> bool:
    if not prev or not nxt:
        return False
    # "TRU" + "MP" / "CAL" + "LED"
    if re.search(r"[A-Za-z]$", prev) and re.match(r"^[a-z]", nxt):
        # only if prev token is short (likely split)
        last = prev.split()[-1] if prev.split() else prev
        return 1 <= len(last) <= 3
    if re.search(r"[A-Z]$", prev) and re.match(r"^[A-Z]{1,6}\b", nxt):
        last = re.findall(r"[A-Za-z]+$", prev)
        if last and 1 <= len(last[0]) <= 3:
            return True
    return False


def _normalize_for_dedupe(text: str) -> str:
    t = clean_ocr_text(text).lower()
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def _token_set(text: str) -> set:
    return set(re.findall(r"[a-z0-9]{2,}", text.lower()))


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def dedupe_ocr_frames(rows: Sequence[dict], *, similarity_threshold: float = 0.88) -> List[dict]:
    """Drop duplicate / near-duplicate OCR across adjacent frames (screenshots).

    Keeps the first (earliest) occurrence when cleaned text is identical or
    highly overlapping (repeated green-screen / tweet screenshots).
    """
    out: List[dict] = []
    kept_norms: List[str] = []
    kept_tokens: List[set] = []
    for r in rows:
        key = _normalize_for_dedupe(r.get("ocr_text") or r.get("ocr_text_raw") or "")
        if not key:
            continue
        toks = _token_set(key)
        dup = False
        for prev, prev_toks in zip(kept_norms, kept_tokens):
            if key == prev or (len(key) > 40 and key in prev) or (len(prev) > 40 and prev in key):
                dup = True
                break
            if _jaccard(toks, prev_toks) >= similarity_threshold:
                dup = True
                break
        if dup:
            continue
        kept_norms.append(key)
        kept_tokens.append(toks)
        out.append(dict(r))
    return out


def build_ocr_segments(
    rows: Sequence[dict],
    *,
    duration_seconds: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Build structured OCR segments with source_type + frame_position."""
    segments: List[Dict[str, Any]] = []
    for r in dedupe_ocr_frames(rows):
        raw = (r.get("ocr_text_raw") or r.get("ocr_text") or "").strip()
        if not raw:
            continue
        cleaned = (r.get("ocr_text_clean") or clean_ocr_text(raw)).strip()
        ts = r.get("frame_timestamp")
        try:
            ts_f = float(ts) if ts is not None else None
        except (TypeError, ValueError):
            ts_f = None
        pct = r.get("frame_pct")
        if pct is None and ts_f is not None and duration_seconds and duration_seconds > 0:
            pct = round(min(100.0, max(0.0, 100.0 * ts_f / duration_seconds)), 2)
        conf = r.get("confidence")
        try:
            conf_f = float(conf) if conf is not None else None
        except (TypeError, ValueError):
            conf_f = None
        source = r.get("source_type") or primary_source_type(cleaned or raw)
        # Map research labels to user-facing overlay/screenshot/unknown buckets
        if source in ("video_overlay",):
            source_type = "overlay"
        elif source in (
            "twitter_screenshot",
            "truth_social_screenshot",
            "news_screenshot",
            "screenshot",
            "green_screen",
        ):
            source_type = "screenshot" if source != "green_screen" else "green_screen"
        elif source == "stitch_content":
            source_type = "stitch"
        else:
            source_type = source or "unknown"

        segments.append(
            {
                "text": cleaned or raw,
                "raw_text": raw,
                "frame_number": r.get("frame_number"),
                "frame_timestamp": ts_f,
                "frame_position": pct,
                "source_type": source_type,
                "source_detail": source,
                "confidence": conf_f,
            }
        )
    return segments


def aggregate_ocr_outputs(
    rows: Sequence[dict],
    *,
    duration_seconds: Optional[float] = None,
    min_confidence: float = 0.35,
) -> Dict[str, Any]:
    """Produce raw/cleaned text, segments JSON, source labels, quality metrics.

    ``raw_ocr_text`` keeps all non-empty Vision text (including noisy frames).
    ``cleaned_ocr_text`` / ``ocr_text`` drop duplicates, low-confidence, and
    garbage UI/symbol blocks while preserving screenshots, tweets, overlays.
    """
    enriched_rows: List[dict] = []
    raw_all: List[str] = []
    for r in rows:
        raw = (r.get("ocr_text_raw") or r.get("ocr_text") or "").strip()
        if not raw:
            continue
        raw_all.append(raw)
        conf = r.get("confidence")
        try:
            conf_f = float(conf) if conf is not None else None
        except (TypeError, ValueError):
            conf_f = None
        cleaned = clean_ocr_text(raw)
        if is_garbage_ocr_text(cleaned or raw, min_confidence=min_confidence, confidence=conf_f):
            # Keep raw history but exclude from cleaned segments
            continue
        rr = dict(r)
        rr["ocr_text_raw"] = raw
        rr["ocr_text_clean"] = cleaned
        rr["ocr_text"] = cleaned  # default working text
        rr["source_type"] = primary_source_type(cleaned or raw)
        enriched_rows.append(rr)

    segments = build_ocr_segments(enriched_rows, duration_seconds=duration_seconds)
    # Extra pass: drop residual garbage from segments
    segments = [
        s
        for s in segments
        if not is_garbage_ocr_text(
            s.get("text") or "",
            min_confidence=min_confidence,
            confidence=s.get("confidence"),
        )
    ]
    raw_parts = list(raw_all) if raw_all else [s["raw_text"] for s in segments if s.get("raw_text")]
    # Collapse persistent overlays (flags/watermarks) that repeat every frame and
    # strip UI chrome / burned-in caption bleed mixed into those overlays.
    frame_texts_for_collapse = [s.get("text") or "" for s in segments]
    collapsed = collapse_persistent_overlays(frame_texts_for_collapse)
    if not collapsed:
        collapsed = refine_joined_ocr_text("\n".join(frame_texts_for_collapse))
    clean_parts = [collapsed] if collapsed else []
    confs = [s["confidence"] for s in segments if s.get("confidence") is not None]
    sources = []
    for s in segments:
        for key in (s.get("source_detail"), s.get("source_type")):
            if key and key not in sources:
                sources.append(key)
    if find_persistent_phrases(frame_texts_for_collapse):
        if "video_overlay" not in sources:
            sources.append("video_overlay")

    n_raw_frames = len(raw_all)
    n_with_text = len(segments)
    avg_chars = (
        round(sum(len(s.get("text") or "") for s in segments) / n_with_text, 2)
        if n_with_text
        else 0.0
    )
    raw_ocr_text = "\n\n".join(raw_parts)
    cleaned_ocr_text = collapsed
    joined_len = sum(len(t) for t in frame_texts_for_collapse) or 1
    unique_ratio = ocr_unique_text_ratio(frame_texts_for_collapse or raw_parts)
    # Collapsing repeats into a short cleaned string implies high uniqueness.
    if cleaned_ocr_text and len(cleaned_ocr_text) < 0.55 * joined_len:
        unique_ratio = max(unique_ratio, 0.9)
    quality = score_ocr_quality(
        raw_text=raw_ocr_text,
        cleaned_text=cleaned_ocr_text,
        unique_ratio=unique_ratio,
        source_count=len(sources),
        frames_in=n_raw_frames,
        frames_kept=max(1, len(clean_parts)) if cleaned_ocr_text else 0,
    )
    return {
        "raw_ocr_text": raw_ocr_text,
        "cleaned_ocr_text": cleaned_ocr_text,
        "ocr_text": cleaned_ocr_text,  # backward-compatible primary = cleaned
        "ocr_text_segments": json.dumps(segments, ensure_ascii=False),
        "ocr_frames_processed": n_raw_frames,
        "frames_with_text": n_with_text,
        "number_of_frames_processed": n_raw_frames,
        "average_text_per_frame": avg_chars,
        "ocr_confidence_avg": (sum(confs) / len(confs)) if confs else None,
        "ocr_sources": json.dumps(sources, ensure_ascii=False),
        "ocr_source_count": len(sources),
        "ocr_character_count": len(cleaned_ocr_text),
        "ocr_unique_text_ratio": unique_ratio,
        "ocr_quality_score": quality,
        "ocr_language": "",  # Vision DOCUMENT_TEXT_DETECTION does not always return lang
        "segments": segments,
        "frame_rows": enriched_rows,
    }
