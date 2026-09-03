"""MediaCloud US news-outlet TikTok handle discovery (review CSV only).

Uses Research API ``research/user/info`` with Pipeline 1 credentials so
Pipeline 2 quota can finish Paperboy journalists. Does not collect videos,
write SQLite/BigQuery, or edit production handle lists.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from tiktok.discovery.paperboy_journalists import (
    DISCOVERY_OUT_DIR,
    DiscoveryCheckpoint,
    DiscoveryStop,
    HIGH_CONFIDENCE_SCORE,
    P1_HANDLE_FILE,
    P2_HANDLE_FILE,
    REVIEW_STATUS,
    empty_profile_fields,
    known_source,
    load_known_handles,
    lookup_user_info,
    normalize_username,
    profile_output_fields,
)

logger = logging.getLogger(__name__)

DEFAULT_CSV = os.path.join("config", "discovery", "mediacloud_us_news_outlets.csv")
SAMPLE_OUTLET_COUNT = 10
MAX_CANDIDATES_PER_OUTLET = 5
MIN_USERNAME_LEN = 3
MAX_USERNAME_LEN = 24

CHECKPOINT_FULL = os.path.join(
    "data", "checkpoints", "mediacloud_outlet_discovery.json"
)
CHECKPOINT_SAMPLE = os.path.join(
    "data", "checkpoints", "mediacloud_outlet_discovery_sample.json"
)
OUTPUT_CSV_FULL = os.path.join(
    DISCOVERY_OUT_DIR, "mediacloud_outlet_tiktok_candidates.csv"
)
OUTPUT_CSV_SAMPLE = os.path.join(
    DISCOVERY_OUT_DIR, "mediacloud_outlet_tiktok_candidates_sample.csv"
)
OUTPUT_CSV_HIGH = os.path.join(
    DISCOVERY_OUT_DIR, "mediacloud_outlet_tiktok_candidates_high_confidence.csv"
)
OUTPUT_CSV_HIGH_SAMPLE = os.path.join(
    DISCOVERY_OUT_DIR,
    "mediacloud_outlet_tiktok_candidates_high_confidence_sample.csv",
)
RUN_JSON_FULL = os.path.join(
    DISCOVERY_OUT_DIR, "mediacloud_outlet_tiktok_candidates_run.json"
)
RUN_JSON_SAMPLE = os.path.join(
    DISCOVERY_OUT_DIR, "mediacloud_outlet_tiktok_candidates_sample_run.json"
)

NAME_STOP = frozenset({"the", "and", "of", "a", "an", "for"})
SKIP_SUBDOMAINS = frozenset(
    {
        "www",
        "amp",
        "m",
        "mobile",
        "cdn",
        "s",
        "files",
        "live",
        "shop",
        "buy",
        "openx",
        "feeds",
        "x",
    }
)
# Only when the outlet display name would not produce the real handle via slug.
# These are well-known US mastheads, several already on P2 (API is skipped).
WELL_KNOWN_USERNAMES = {
    "new york times": ("nytimes",),
    "usa today": ("usatoday",),
    "boston globe": ("bostonglobe", "bostondotcom"),
    "mother jones": ("motherjones", "motherjonesmag"),
    "dallas morning news": ("dallasmorningnews",),
    "baltimore sun": ("baltimoresun",),
    "columbus dispatch": ("columbusdispatch",),
    "ms.now (msnbc)": ("msnbc",),
    "nbc breaking news": ("nbcnews",),
}

OUTPUT_COLUMNS = [
    "outlet_name",
    "outlet_platform",
    "outlet_media_type",
    "outlet_profile_type",
    "outlet_usable",
    "outlet_notes",
    "outlet_csv_tiktok_handle",
    "candidate_username",
    "candidate_source",
    "candidate_rank",
    "match_score",
    "match_reason",
    "match_reasons",
    "tiktok_username",
    "tiktok_display_name",
    "tiktok_bio_description",
    "tiktok_avatar_url",
    "tiktok_is_verified",
    "tiktok_follower_count",
    "tiktok_following_count",
    "tiktok_likes_count",
    "tiktok_video_count",
    "tiktok_bio_url",
    "api_status",
    "http_status",
    "already_known",
    "already_known_in",
    "review_status",
]

EVIDENCE_POINTS = (
    ("exact_csv_tiktok_handle", 40),
    ("exact_domain_slug", 40),
    ("exact_name_slug", 35),
    ("well_known_alias", 35),
    ("display_name_match", 30),
    ("outlet_in_bio", 20),
    ("display_name_contains", 15),
    ("verified", 5),
    ("weak_username_similarity", 5),
)


@dataclass
class Outlet:
    name: str
    csv_tiktok_handle: str
    platform: str
    media_type: str
    usable: str
    profile_type: str
    notes: str
    source_row: int


@dataclass
class Candidate:
    username: str
    source: str
    rank: int


def _norm_header(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\n", " ")).strip().lower()


def _cell(row: Dict[str, Any], *needles: str) -> str:
    lowered = {_norm_header(k): (v if v is not None else "") for k, v in row.items()}
    for needle in needles:
        n = _norm_header(needle)
        if n in lowered:
            return str(lowered[n]).strip()
    return ""


def name_tokens(name: str) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9]+", (name or "").lower()) if t not in NAME_STOP]


def concat_name(name: str) -> str:
    return "".join(name_tokens(name))


def underscore_name(name: str) -> str:
    return "_".join(name_tokens(name))


def looks_like_host(value: str) -> bool:
    raw = (value or "").strip().lower()
    if not raw or " " in raw:
        return False
    if raw.startswith("http://") or raw.startswith("https://"):
        return True
    return "." in raw and "@" not in raw


def domain_slugs(outlet: str) -> List[str]:
    """Second-level domain slugs from a host/URL. Empty if not a domain."""
    raw = (outlet or "").strip()
    if not looks_like_host(raw):
        return []
    if "://" not in raw:
        raw = "http://" + raw
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    if not host:
        return []
    parts = [p for p in host.split(".") if p]
    while len(parts) > 2 and (
        parts[0] in SKIP_SUBDOMAINS or re.fullmatch(r"cdn\d+", parts[0])
    ):
        parts = parts[1:]
    if len(parts) >= 3 and parts[-2] == "go" and parts[-1] == "com":
        sld = parts[-3]
    elif len(parts) >= 2:
        sld = parts[-2]
    else:
        sld = parts[0]
    sld = re.sub(r"[^a-z0-9_-]", "", sld)
    if not sld:
        return []
    compact = sld.replace("-", "").replace("_", "")
    under = sld.replace("-", "_")
    out: List[str] = []
    for item in (compact, under):
        if item and item not in out:
            out.append(item)
    return out


def parse_outlets(csv_path: str) -> List[Outlet]:
    """Load MediaCloud outlets CSV; order-preserving, skip blank names."""
    outlets: List[Outlet] = []
    seen = set()
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            name = _cell(row, "outlet")
            if not name:
                continue
            key = name.strip().lower()
            if key in seen:
                continue
            seen.add(key)
            outlets.append(
                Outlet(
                    name=name,
                    csv_tiktok_handle=normalize_username(_cell(row, "tiktok handle")),
                    platform=_cell(row, "platform"),
                    media_type=_cell(row, "media_type"),
                    usable=_cell(row, "usable (human review)", "usable"),
                    profile_type=_cell(row, "profile_type"),
                    notes=_cell(row, "notes"),
                    source_row=i,
                )
            )
    return outlets


def generate_candidates(outlet: Outlet) -> List[Candidate]:
    """Conservative masthead username variants (outlets, not journalists)."""
    items: List[Tuple[str, str]] = []
    seen = set()

    def add(username: str, source: str) -> None:
        n = normalize_username(username)
        if not n or len(n) < MIN_USERNAME_LEN or len(n) > MAX_USERNAME_LEN:
            return
        if n in seen:
            return
        if len(items) >= MAX_CANDIDATES_PER_OUTLET:
            return
        seen.add(n)
        items.append((n, source))

    add(outlet.csv_tiktok_handle, "csv_tiktok_handle")
    if looks_like_host(outlet.name):
        for slug in domain_slugs(outlet.name):
            add(slug, "domain_slug")
    else:
        add(concat_name(outlet.name), "name_concat")
        add(underscore_name(outlet.name), "name_underscore")
        for alias in WELL_KNOWN_USERNAMES.get(outlet.name.strip().lower(), ()):
            add(alias, "well_known_alias")
        base = concat_name(outlet.name)
        if base and len(base) >= 5 and not base.endswith("news"):
            add(base + "news", "name_suffix_news")

    return [
        Candidate(username=u, source=s, rank=i)
        for i, (u, s) in enumerate(items, start=1)
    ]


def checkpoint_key(outlet_name: str, username: str) -> str:
    return f"{(outlet_name or '').strip().lower()}|{normalize_username(username)}"


def _fold_text(value: str) -> str:
    text = (value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def score_match(
    outlet: Outlet,
    candidate: Candidate,
    profile: Optional[Dict[str, Any]] = None,
) -> Tuple[int, str, str]:
    reasons: List[str] = []
    display = _fold_text((profile or {}).get("display_name") or "")
    bio = _fold_text((profile or {}).get("bio") or "")
    folded_name = _fold_text(outlet.name)
    username = candidate.username
    concat = concat_name(outlet.name)
    domains = set(domain_slugs(outlet.name))

    if outlet.csv_tiktok_handle and username == outlet.csv_tiktok_handle:
        reasons.append("exact_csv_tiktok_handle")
    if username in domains:
        reasons.append("exact_domain_slug")
    if concat and username == concat:
        reasons.append("exact_name_slug")
    if candidate.source == "well_known_alias":
        reasons.append("well_known_alias")
    if profile:
        if display and folded_name and display == folded_name:
            reasons.append("display_name_match")
        elif display and folded_name:
            name_toks = set(folded_name.split())
            disp_toks = set(display.split())
            if name_toks and name_toks.issubset(disp_toks):
                reasons.append("display_name_contains")
        if folded_name and len(folded_name) >= 4 and folded_name in bio:
            reasons.append("outlet_in_bio")
        elif concat and len(concat) >= 5 and concat in bio.replace(" ", ""):
            reasons.append("outlet_in_bio")
        if bool((profile or {}).get("is_verified")):
            reasons.append("verified")
    strong = {
        "exact_csv_tiktok_handle",
        "exact_domain_slug",
        "exact_name_slug",
        "well_known_alias",
        "display_name_match",
        "display_name_contains",
        "outlet_in_bio",
    }
    if concat and len(concat) >= 6 and concat not in username:
        if concat[:6] in username and not (strong & set(reasons)):
            reasons.append("weak_username_similarity")

    points = dict(EVIDENCE_POINTS)
    score = sum(points[r] for r in reasons if r in points)
    ordered = [r for r, _ in EVIDENCE_POINTS if r in reasons]
    primary = ordered[0] if ordered else ""
    return score, primary, "|".join(ordered)


def is_high_confidence(row: Dict[str, str]) -> bool:
    if (row.get("already_known") or "") == "true":
        return False
    if (row.get("api_status") or "") != "found":
        return False
    try:
        score = int(row.get("match_score") or 0)
    except ValueError:
        score = 0
    if score < HIGH_CONFIDENCE_SCORE:
        return False
    reasons = (row.get("match_reasons") or "") + "|" + (row.get("match_reason") or "")
    return any(
        token in reasons
        for token in (
            "exact_csv_tiktok_handle",
            "exact_domain_slug",
            "exact_name_slug",
            "well_known_alias",
            "display_name_match",
        )
    )


def build_row(
    outlet: Outlet,
    candidate: Candidate,
    *,
    api_status: str,
    http_status: str,
    already_known: bool,
    already_known_in: str,
    profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    score, reason, reasons = score_match(outlet, candidate, profile)
    row = {
        "outlet_name": outlet.name,
        "outlet_platform": outlet.platform,
        "outlet_media_type": outlet.media_type,
        "outlet_profile_type": outlet.profile_type,
        "outlet_usable": outlet.usable,
        "outlet_notes": outlet.notes,
        "outlet_csv_tiktok_handle": outlet.csv_tiktok_handle,
        "candidate_username": candidate.username,
        "candidate_source": candidate.source,
        "candidate_rank": str(candidate.rank),
        "match_score": str(score),
        "match_reason": reason,
        "match_reasons": reasons,
        "api_status": api_status,
        "http_status": http_status,
        "already_known": "true" if already_known else "false",
        "already_known_in": already_known_in,
        "review_status": REVIEW_STATUS,
    }
    row.update(profile_output_fields(profile) if profile else empty_profile_fields())
    return row


def _ordered_rows(
    outlets: List[Outlet],
    rows_by_key: Dict[str, Dict[str, str]],
) -> List[Dict[str, str]]:
    order = {o.name.strip().lower(): i for i, o in enumerate(outlets)}
    items = list(rows_by_key.values())

    def sort_key(row: Dict[str, str]):
        name = (row.get("outlet_name") or "").strip().lower()
        try:
            rank = int(row.get("candidate_rank") or 0)
        except ValueError:
            rank = 0
        return (order.get(name, 10_000), rank, row.get("candidate_username") or "")

    return sorted(items, key=sort_key)


def write_output_csv(path: str, rows: Iterable[Dict[str, str]]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in OUTPUT_COLUMNS})


def write_run_json(path: str, payload: Dict[str, Any]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def discovery_paths(*, sample: bool) -> Dict[str, str]:
    if sample:
        return {
            "checkpoint": CHECKPOINT_SAMPLE,
            "output_csv": OUTPUT_CSV_SAMPLE,
            "output_csv_high": OUTPUT_CSV_HIGH_SAMPLE,
            "run_json": RUN_JSON_SAMPLE,
        }
    return {
        "checkpoint": CHECKPOINT_FULL,
        "output_csv": OUTPUT_CSV_FULL,
        "output_csv_high": OUTPUT_CSV_HIGH,
        "run_json": RUN_JSON_FULL,
    }


def run_discovery(
    *,
    csv_path: str,
    client: Any,
    known: Dict[str, str],
    sample: bool = False,
    reset_checkpoints: bool = False,
    retry_failed: bool = False,
    checkpoint_path: Optional[str] = None,
    output_csv: Optional[str] = None,
    output_csv_high: Optional[str] = None,
    run_json: Optional[str] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    """Generate outlet candidates, skip known handles, probe user/info."""
    paths = discovery_paths(sample=sample)
    checkpoint_path = checkpoint_path or paths["checkpoint"]
    output_csv = output_csv or paths["output_csv"]
    output_csv_high = output_csv_high or paths["output_csv_high"]
    run_json = run_json or paths["run_json"]

    outlets = parse_outlets(csv_path)
    if sample:
        outlets = outlets[:SAMPLE_OUTLET_COUNT]

    ckpt = DiscoveryCheckpoint(checkpoint_path)
    if reset_checkpoints:
        ckpt.reset()
    elif retry_failed:
        ckpt.clear_failed()

    stats: Dict[str, Any] = {
        "workflow": "mediacloud_outlet_discovery",
        "sample": bool(sample),
        "csv_path": csv_path,
        "outlets": len(outlets),
        "candidates_generated": 0,
        "candidates_already_known": 0,
        "api_calls_attempted": 0,
        "successful_user_info_responses": 0,
        "accounts_not_found": 0,
        "api_errors": 0,
        "rate_limit_or_quota_errors": 0,
        "high_confidence_candidates": 0,
        "output_csv": output_csv,
        "output_csv_high": output_csv_high,
        "checkpoint": checkpoint_path,
        "run_json": run_json,
        "stopped_reason": "",
        "finished_at": "",
    }

    stopped_reason = ""
    try:
        for outlet in outlets:
            for candidate in generate_candidates(outlet):
                stats["candidates_generated"] += 1
                key = checkpoint_key(outlet.name, candidate.username)
                source = known_source(candidate.username, known)
                already = bool(source)

                if ckpt.is_completed_key(key):
                    continue
                if ckpt.is_failed_key(key) and not retry_failed:
                    continue

                if already:
                    stats["candidates_already_known"] += 1
                    row = build_row(
                        outlet,
                        candidate,
                        api_status="skipped_already_known",
                        http_status="",
                        already_known=True,
                        already_known_in=source,
                    )
                    ckpt.mark_completed_row(key, row)
                    continue

                stats["api_calls_attempted"] += 1
                try:
                    api_status, http_status, profile = lookup_user_info(
                        client, candidate.username, sleep=sleep
                    )
                except DiscoveryStop as exc:
                    stopped_reason = str(exc)
                    stats["rate_limit_or_quota_errors"] += 1
                    stats["stopped_reason"] = stopped_reason
                    logger.error(
                        "MediaCloud outlet discovery stopped: %s", stopped_reason
                    )
                    raise

                if api_status == "found":
                    stats["successful_user_info_responses"] += 1
                elif api_status == "not_found":
                    stats["accounts_not_found"] += 1
                else:
                    stats["api_errors"] += 1

                row = build_row(
                    outlet,
                    candidate,
                    api_status=api_status,
                    http_status="" if http_status is None else str(http_status),
                    already_known=False,
                    already_known_in="",
                    profile=profile,
                )
                if api_status == "failed":
                    ckpt.mark_failed_row(key, row)
                else:
                    ckpt.mark_completed_row(key, row)
    except DiscoveryStop:
        pass

    rows = _ordered_rows(outlets, ckpt._rows)
    write_output_csv(output_csv, rows)
    high_rows = [r for r in rows if is_high_confidence(r)]
    write_output_csv(output_csv_high, high_rows)
    stats["candidates_already_known"] = sum(
        1 for r in rows if r.get("already_known") == "true"
    )
    stats["successful_user_info_responses"] = sum(
        1 for r in rows if r.get("api_status") == "found"
    )
    stats["accounts_not_found"] = sum(
        1 for r in rows if r.get("api_status") == "not_found"
    )
    stats["api_errors"] = sum(1 for r in rows if r.get("api_status") == "failed")
    stats["high_confidence_candidates"] = len(high_rows)
    stats["output_rows"] = len(rows)
    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_run_json(run_json, stats)
    return stats
