"""Lightweight OCR incremental-signal evaluation over ocr_eval_batch.py CSV output.

Answers: does on-screen OCR add information beyond caption + voice_to_text + Whisper?

Usage (from project root):
    python scripts/ocr_signal_evaluation.py
    python scripts/ocr_signal_evaluation.py --input data/ocr_eval_batch_20260419_120000.csv

Writes: data/ocr_signal_eval_summary.csv
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys
from collections import Counter
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- text overlap helpers -------------------------------------------------

_STOPWORDS = frozenset(
    """a an the and or but if in on at to for of as is was are were be been being
    it its this that these those with from by not no yes so than then too very
    i you he she we they me my your his her our their what which who whom
    about into through over after before between again further once here there
    when where why how all each both few more most other some such only same
    can could should would will just also than into out up down off out
    """.split()
)


def normalize_text(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def word_tokens(s: str) -> List[str]:
    return [w for w in normalize_text(s).split() if w and w not in _STOPWORDS]


def word_set(s: str) -> Set[str]:
    return set(word_tokens(s))


def jaccard_words(a: str, b: str) -> float:
    sa, sb = word_set(a), word_set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def overlap_vs_sources(
    ocr: str,
    caption: str,
    voice_to_text: str,
    whisper: str,
) -> Dict[str, Any]:
    """Similarity / novelty metrics for OCR vs each text source and combined ref."""
    o = word_set(ocr)
    c, v, w = word_set(caption), word_set(voice_to_text), word_set(whisper)
    ref = c | v | w

    novel = o - ref
    novel_ratio = len(novel) / len(o) if o else 0.0
    jac_ref = (
        len(o & ref) / len(o | ref)
        if (o | ref)
        else (1.0 if not o else 0.0)
    )

    return {
        "ocr_word_count": len(o),
        "novel_word_count": len(novel),
        "novel_ratio": round(novel_ratio, 4),
        "jaccard_ocr_vs_caption": round(jaccard_words(ocr, caption), 4),
        "jaccard_ocr_vs_vtt": round(jaccard_words(ocr, voice_to_text), 4),
        "jaccard_ocr_vs_whisper": round(jaccard_words(ocr, whisper), 4),
        "jaccard_ocr_vs_combined_ref": round(jac_ref, 4),
    }


# --- heuristics -----------------------------------------------------------

ContentType = str
Redundancy = str
Quality = str
Usefulness = str


def infer_content_type(
    ocr: str,
    caption: str,
    metrics: Dict[str, Any],
) -> ContentType:
    """Rough bucket for manual review alignment (not a classifier)."""
    o_lower = (ocr or "").lower()
    words = word_tokens(ocr)
    n_words = len(words)
    lines = [ln.strip() for ln in (ocr or "").splitlines() if ln.strip()]

    news_keys = (
        "breaking",
        "live",
        "update",
        "cnn",
        "msnbc",
        "fox",
        "reuters",
        "white house",
        "congress",
    )
    url_like = "http" in o_lower or "www." in o_lower
    tweet_like = "twitter" in o_lower or "x.com" in o_lower or re.search(
        r"@\w+", ocr or ""
    )
    if any(k in o_lower for k in news_keys) or url_like or tweet_like:
        return "screenshot / news overlay"

    if n_words < 8 and len((ocr or "").strip()) < 50:
        return "low_text / music-only"

    if len(lines) >= 6 and sum(len(x) for x in lines) / max(len(lines), 1) < 30:
        return "meme / stylized text"

    jv = metrics.get("jaccard_ocr_vs_vtt", 0.0)
    jw = metrics.get("jaccard_ocr_vs_whisper", 0.0)
    if n_words >= 10 and max(jv, jw) > 0.42:
        return "standard talking video"

    return "standard talking video"


def infer_redundancy(metrics: Dict[str, Any], ocr_empty: bool) -> Redundancy:
    if ocr_empty:
        return "redundant"
    nr = metrics["novel_ratio"]
    jac = metrics["jaccard_ocr_vs_combined_ref"]
    if nr < 0.12 or (jac > 0.65 and metrics["ocr_word_count"] > 0):
        return "redundant"
    if nr > 0.38 or (nr > 0.22 and metrics["novel_word_count"] >= 6):
        return "unique"
    return "partially_redundant"


def infer_ocr_quality(ocr: str, metrics: Dict[str, Any]) -> Quality:
    wc = metrics["ocr_word_count"]
    if wc < 4 and len((ocr or "").strip()) < 30:
        return "low"
    if wc >= 18 or len((ocr or "").strip()) >= 200:
        return "high"
    return "medium"


def infer_ocr_adds_new_info(metrics: Dict[str, Any], redundancy: Redundancy) -> bool:
    if metrics["ocr_word_count"] == 0:
        return False
    nr = metrics["novel_ratio"]
    nv = metrics["novel_word_count"]
    if redundancy == "unique":
        return True
    if redundancy == "partially_redundant" and (nr >= 0.18 or nv >= 4):
        return True
    if nr >= 0.25 and metrics["ocr_word_count"] >= 6:
        return True
    return False


def usefulness_bucket(
    adds: bool,
    redundancy: Redundancy,
    quality: Quality,
    ocr_empty: bool,
) -> Usefulness:
    """USEFUL / CONDITIONAL / NOT USEFUL — for summary stats only."""
    if ocr_empty or quality == "low" and not adds:
        return "NOT USEFUL"
    if adds and redundancy != "redundant" and quality in ("high", "medium"):
        return "USEFUL"
    if adds and redundancy == "partially_redundant":
        return "CONDITIONAL"
    if not adds and redundancy == "redundant":
        return "NOT USEFUL"
    return "CONDITIONAL"


def build_notes(
    metrics: Dict[str, Any],
    redundancy: Redundancy,
    bucket: Usefulness,
) -> str:
    return (
        f"usefulness={bucket}; novel_ratio={metrics['novel_ratio']}; "
        f"jaccard_vs_ref={metrics['jaccard_ocr_vs_combined_ref']}; "
        f"j_vtt={metrics['jaccard_ocr_vs_vtt']}; j_whisper={metrics['jaccard_ocr_vs_whisper']}"
    )


def find_latest_batch_csv(data_dir: str) -> Optional[str]:
    paths = sorted(
        glob.glob(os.path.join(data_dir, "ocr_eval_batch_*.csv")),
        key=os.path.getmtime,
        reverse=True,
    )
    return paths[0] if paths else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate incremental OCR signal vs caption/VTT/Whisper",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Path to ocr_eval_batch_*.csv (default: newest in data/)",
    )
    parser.add_argument(
        "--output",
        default=os.path.join("data", "ocr_signal_eval_summary.csv"),
        help="Output summary CSV path",
    )
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    in_path = args.input or find_latest_batch_csv("data")
    if not in_path or not os.path.isfile(in_path):
        print("No input CSV found. Run scripts/ocr_eval_batch.py first.", file=sys.stderr)
        return 1

    rows_out: List[Dict[str, Any]] = []
    usefulness_counts: Counter[str] = Counter()
    usefulness_by_type: Dict[str, Counter[str]] = {}

    with open(in_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vid = (row.get("video_id") or "").strip()
            url = (row.get("url") or "").strip()
            ocr = row.get("onscreen_text") or ""
            cap = row.get("caption") or ""
            vtt = row.get("voice_to_text") or ""
            wh = row.get("whisper_transcript") or ""

            ocr_empty = not ocr.strip()
            m = overlap_vs_sources(ocr, cap, vtt, wh)
            redundancy = infer_redundancy(m, ocr_empty)
            quality = infer_ocr_quality(ocr, m)
            ctype = infer_content_type(ocr, cap, m)
            adds = infer_ocr_adds_new_info(m, redundancy)
            bucket = usefulness_bucket(adds, redundancy, quality, ocr_empty)

            usefulness_counts[bucket] += 1
            usefulness_by_type.setdefault(ctype, Counter())
            usefulness_by_type[ctype][bucket] += 1

            rows_out.append(
                {
                    "video_id": vid,
                    "url": url,
                    "content_type": ctype,
                    "ocr_adds_new_info": adds,
                    "redundancy_level": redundancy,
                    "ocr_quality": quality,
                    "notes": build_notes(m, redundancy, bucket),
                }
            )

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    fieldnames = [
        "video_id",
        "url",
        "content_type",
        "ocr_adds_new_info",
        "redundancy_level",
        "ocr_quality",
        "notes",
    ]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows_out)

    n = sum(usefulness_counts.values()) or 1
    print(f"Read: {in_path}")
    print(f"Wrote: {args.output} ({len(rows_out)} rows)\n")
    print("--- Overall usefulness (heuristic) ---")
    for label in ("USEFUL", "CONDITIONAL", "NOT USEFUL"):
        c = usefulness_counts.get(label, 0)
        print(f"  {label}: {c} ({100.0 * c / n:.1f}%)")

    print("\n--- By content_type ---")
    for ctype in sorted(usefulness_by_type.keys()):
        sub = usefulness_by_type[ctype]
        tot = sum(sub.values()) or 1
        print(f"  [{ctype}] n={tot}")
        for label in ("USEFUL", "CONDITIONAL", "NOT USEFUL"):
            c = sub.get(label, 0)
            print(f"    {label}: {c} ({100.0 * c / tot:.1f}%)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
