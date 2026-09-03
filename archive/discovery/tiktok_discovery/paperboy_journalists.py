"""Paperboy journalist TikTok handle discovery (review CSV only).

Uses Research API ``research/user/info`` with NEWS_API credentials.
Does not collect videos, write SQLite/BigQuery, or edit production lists.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from tiktok.checkpoint import CheckpointStore
from tiktok.pipelines import load_handle_file, normalize_handle

logger = logging.getLogger(__name__)

SECTION_HEADER_PREFIX = "also tracked"
SAMPLE_JOURNALIST_COUNT = 10
MAX_CANDIDATES_PER_JOURNALIST = 6
MIN_USERNAME_LEN = 3
SUFFIX_MIN_BASE_LEN = 5
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_SLEEP_SECONDS = 2.0
REVIEW_STATUS = "needs_review"
HIGH_CONFIDENCE_SCORE = 50

USER_INFO_FIELDS = (
    "display_name,bio_description,avatar_url,is_verified,"
    "follower_count,following_count,likes_count,video_count,bio_url"
)

DEFAULT_CSV = os.path.join("config", "discovery", "paperboy_journalist_list.csv")
P1_HANDLE_FILE = os.path.join("config", "handles", "newsfluencer_combined.txt")
P2_HANDLE_FILE = os.path.join("config", "handles", "news_accounts.txt")

DISCOVERY_RAW_DIR = os.path.join("data", "discovery", "raw")
DISCOVERY_OUT_DIR = os.path.join("data", "discovery")
CHECKPOINT_FULL = os.path.join(
    "data", "checkpoints", "paperboy_journalist_discovery.json"
)
CHECKPOINT_SAMPLE = os.path.join(
    "data", "checkpoints", "paperboy_journalist_discovery_sample.json"
)
OUTPUT_CSV_FULL = os.path.join(
    DISCOVERY_OUT_DIR, "paperboy_journalist_tiktok_candidates.csv"
)
OUTPUT_CSV_SAMPLE = os.path.join(
    DISCOVERY_OUT_DIR, "paperboy_journalist_tiktok_candidates_sample.csv"
)
RUN_JSON_FULL = os.path.join(
    DISCOVERY_OUT_DIR, "paperboy_journalist_tiktok_candidates_run.json"
)
RUN_JSON_SAMPLE = os.path.join(
    DISCOVERY_OUT_DIR, "paperboy_journalist_tiktok_candidates_sample_run.json"
)

GENERATIONAL_TOKENS = frozenset({"jr", "sr", "ii", "iii", "iv", "v"})
USERNAME_RE = re.compile(r"^[a-z0-9._]+$")

OUTPUT_COLUMNS = [
    "paperboy_name",
    "paperboy_x_handle",
    "paperboy_outlet",
    "paperboy_outlet_primary",
    "paperboy_city",
    "paperboy_state",
    "paperboy_notes",
    "paperboy_usable",
    "paperboy_masthead_handles",
    "paperboy_csv_tiktok_handle",
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
    ("exact_x_handle", 40),
    ("exact_csv_tiktok_handle", 35),
    ("display_name_match", 25),
    ("exact_name_username", 20),
    ("outlet_in_bio", 15),
    ("display_name_contains", 10),
    ("name_in_bio", 10),
    ("verified", 5),
    ("weak_username_similarity", 5),
)


class DiscoveryStop(RuntimeError):
    """Abort the run without marking the current candidate completed."""


@dataclass
class Journalist:
    name: str
    outlet: str
    outlet_primary: str
    usable: str
    csv_tiktok_handle: str
    url: str
    followers: str
    notes: str
    masthead_handles: List[str]
    x_handle: str
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
        for key, val in lowered.items():
            if key.startswith(n):
                return str(val).strip()
    return ""


def normalize_username(raw: str) -> str:
    """Lowercase, strip @, trim, extract handle from a TikTok profile URL."""
    value = (raw or "").strip()
    if not value:
        return ""
    if "tiktok.com" in value.lower():
        path = urlparse(value).path if "://" in value else value
        path = path.split("?")[0].strip()
        m = re.search(r"@([A-Za-z0-9._]+)", path)
        if m:
            value = m.group(1)
        else:
            tail = path.rstrip("/").split("/")[-1]
            value = tail.lstrip("@")
    value = value.strip().lstrip("@").strip().lower()
    value = value.split("/")[0].split("?")[0].strip()
    if value and not USERNAME_RE.fullmatch(value):
        return ""
    return value


def name_tokens(name: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", (name or "").lower())


def slug_tokens(name: str) -> List[str]:
    tokens = name_tokens(name)
    while tokens and tokens[-1] in GENERATIONAL_TOKENS:
        tokens = tokens[:-1]
    return tokens


def concat_name(name: str) -> str:
    return "".join(slug_tokens(name))


def underscore_name(name: str) -> str:
    return "_".join(slug_tokens(name))


def first_last_tokens(name: str) -> Optional[Tuple[str, str]]:
    tokens = slug_tokens(name)
    if len(tokens) < 3:
        return None
    first, last = tokens[0], tokens[-1]
    middle = tokens[1:-1]
    if not any(len(t) == 1 for t in middle):
        return None
    if len(first) < 2 or len(last) < 2:
        return None
    return first, last


def primary_outlet(outlet: str) -> str:
    raw = (outlet or "").strip()
    if not raw:
        return ""
    return raw.split("+")[0].strip()


def parse_masthead_handles(raw: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for part in re.split(r"[,;/]", raw or ""):
        n = normalize_username(part)
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def is_section_header(name: str) -> bool:
    return (name or "").strip().lower().startswith(SECTION_HEADER_PREFIX)


def parse_journalists(csv_path: str) -> List[Journalist]:
    """Load Paperboy CSV; skip the 'Also tracked' header; order-preserving dedupe."""
    journalists: List[Journalist] = []
    seen_names = set()
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            name = _cell(row, "name")
            if not name or is_section_header(name):
                continue
            key = name.strip().lower()
            if key in seen_names:
                continue
            seen_names.add(key)
            outlet = _cell(row, "outlet")
            journalists.append(
                Journalist(
                    name=name,
                    outlet=outlet,
                    outlet_primary=primary_outlet(outlet),
                    usable=_cell(row, "usable (as individual)", "usable"),
                    csv_tiktok_handle=normalize_username(
                        _cell(row, "tiktok handle")
                    ),
                    url=_cell(row, "url"),
                    followers=_cell(row, "followers"),
                    notes=_cell(row, "notes"),
                    masthead_handles=parse_masthead_handles(
                        _cell(row, "journalist as creator info")
                    ),
                    x_handle=normalize_username(_cell(row, "x handle")),
                    source_row=i,
                )
            )
    return journalists


def generate_candidates(journalist: Journalist) -> List[Candidate]:
    """Conservative username variants; masthead handles are never added."""
    items: List[Tuple[str, str]] = []
    seen = set()
    masthead = set(journalist.masthead_handles)

    def add(username: str, source: str) -> None:
        n = normalize_username(username)
        if not n or len(n) < MIN_USERNAME_LEN:
            return
        if n in seen or n in masthead:
            return
        if len(items) >= MAX_CANDIDATES_PER_JOURNALIST:
            return
        seen.add(n)
        items.append((n, source))

    add(journalist.x_handle, "x_handle")
    add(journalist.csv_tiktok_handle, "csv_tiktok_handle")
    add(concat_name(journalist.name), "name_concat")
    add(underscore_name(journalist.name), "name_underscore")
    fl = first_last_tokens(journalist.name)
    if fl:
        add("".join(fl), "name_first_last")
        add("_".join(fl), "name_first_last_underscore")
    base = concat_name(journalist.name)
    if base and len(base) >= SUFFIX_MIN_BASE_LEN:
        add(base + "news", "name_suffix_news")
        add(base + "tv", "name_suffix_tv")
        add(base + "reporter", "name_suffix_reporter")

    return [
        Candidate(username=u, source=s, rank=i)
        for i, (u, s) in enumerate(items, start=1)
    ]


def load_known_handles(
    p1_path: str = P1_HANDLE_FILE,
    p2_path: str = P2_HANDLE_FILE,
) -> Dict[str, str]:
    """Map normalized handle -> news_accounts | newsfluencer_combined | both."""
    p1 = {normalize_handle(h) for h in load_handle_file(p1_path)}
    p2 = {normalize_handle(h) for h in load_handle_file(p2_path)}
    out: Dict[str, str] = {}
    for h in p1 | p2:
        in_p1 = h in p1
        in_p2 = h in p2
        if in_p1 and in_p2:
            out[h] = "both"
        elif in_p2:
            out[h] = "news_accounts"
        else:
            out[h] = "newsfluencer_combined"
    return out


def known_source(username: str, known: Dict[str, str]) -> str:
    return known.get(normalize_handle(username) or username, "")


def checkpoint_key(journalist_name: str, username: str) -> str:
    return f"{(journalist_name or '').strip().lower()}|{normalize_username(username)}"


def _fold_text(value: str) -> str:
    text = (value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _person_first_last(name: str) -> Optional[Tuple[str, str]]:
    tokens = [t for t in slug_tokens(name) if len(t) >= 2]
    if len(tokens) < 2:
        return None
    return tokens[0], tokens[-1]


def score_match(
    journalist: Journalist,
    candidate: Candidate,
    profile: Optional[Dict[str, Any]] = None,
) -> Tuple[int, str, str]:
    """Return (score, primary_reason, pipe-separated reasons). Always explainable."""
    reasons: List[str] = []
    display = _fold_text((profile or {}).get("display_name") or "")
    bio = _fold_text((profile or {}).get("bio") or "")
    person = _fold_text(journalist.name)
    outlet = _fold_text(journalist.outlet_primary)
    username = candidate.username
    concat = concat_name(journalist.name)
    fl = _person_first_last(journalist.name)

    if journalist.x_handle and username == journalist.x_handle:
        reasons.append("exact_x_handle")
    if journalist.csv_tiktok_handle and username == journalist.csv_tiktok_handle:
        reasons.append("exact_csv_tiktok_handle")
    if concat and username == concat:
        reasons.append("exact_name_username")
    if profile:
        if display and person and display == person:
            reasons.append("display_name_match")
        elif fl and display:
            first, last = fl
            if first in display.split() and last in display.split():
                reasons.append("display_name_contains")
            elif first in display and last in display:
                reasons.append("display_name_contains")
        if person and len(person) >= 5 and person in bio:
            reasons.append("name_in_bio")
        elif fl:
            first, last = fl
            if first in bio and last in bio:
                reasons.append("name_in_bio")
        outlet_hit = False
        if outlet and len(outlet) >= 4 and outlet in bio:
            outlet_hit = True
        for handle in journalist.masthead_handles:
            if handle and handle in bio.replace(" ", ""):
                outlet_hit = True
            if handle and f"@{handle}" in ((profile or {}).get("bio") or "").lower():
                outlet_hit = True
        if outlet_hit:
            reasons.append("outlet_in_bio")
        if bool((profile or {}).get("is_verified")):
            reasons.append("verified")

    strong = {
        "exact_x_handle",
        "exact_csv_tiktok_handle",
        "exact_name_username",
        "display_name_match",
        "display_name_contains",
        "name_in_bio",
        "outlet_in_bio",
    }
    if fl and not (strong & set(reasons)):
        first, last = fl
        if len(first) >= 3 and len(last) >= 3:
            if first in username and last in username:
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
    reason = row.get("match_reason") or ""
    reasons = row.get("match_reasons") or ""
    if score < HIGH_CONFIDENCE_SCORE:
        return False
    return reason in ("exact_x_handle", "display_name_match") or (
        "exact_x_handle" in reasons or "display_name_match" in reasons
    )


class DiscoveryCheckpoint(CheckpointStore):
    """CheckpointStore plus saved output rows for resume/rebuild."""

    def __init__(self, filepath: str):
        self._rows: Dict[str, Dict[str, str]] = {}
        super().__init__(filepath)

    def _load(self):
        self._completed = set()
        self._failed = set()
        self._rows = {}
        if not os.path.exists(self.filepath):
            return
        with open(self.filepath, encoding="utf-8") as f:
            data = json.load(f)
        self._completed = set(data.get("completed") or [])
        self._failed = set(data.get("failed") or [])
        rows = data.get("rows") or {}
        if isinstance(rows, dict):
            self._rows = {str(k): dict(v) for k, v in rows.items()}

    def _save(self):
        parent = os.path.dirname(self.filepath)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "completed": sorted(self._completed),
                    "failed": sorted(self._failed),
                    "rows": self._rows,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

    def reset(self):
        self._completed.clear()
        self._failed.clear()
        self._rows = {}
        self._save()
        logger.info("Discovery checkpoints cleared: %s", self.filepath)

    def is_completed_key(self, key: str) -> bool:
        return key in self._completed

    def is_failed_key(self, key: str) -> bool:
        return key in self._failed

    def mark_completed_row(self, key: str, row: Dict[str, str]) -> None:
        self._completed.add(key)
        self._failed.discard(key)
        self._rows[key] = dict(row)
        self._save()

    def mark_failed_row(self, key: str, row: Dict[str, str]) -> None:
        self._failed.add(key)
        self._completed.discard(key)
        self._rows[key] = dict(row)
        self._save()
        logger.warning("Discovery checkpointed as failed: %s", key)


def empty_profile_fields() -> Dict[str, str]:
    return {
        "tiktok_username": "",
        "tiktok_display_name": "",
        "tiktok_bio_description": "",
        "tiktok_avatar_url": "",
        "tiktok_is_verified": "",
        "tiktok_follower_count": "",
        "tiktok_following_count": "",
        "tiktok_likes_count": "",
        "tiktok_video_count": "",
        "tiktok_bio_url": "",
    }


def profile_from_api_data(data: Dict[str, Any], queried_username: str) -> Dict[str, Any]:
    return {
        "username": data.get("username") or queried_username,
        "display_name": data.get("display_name") or "",
        "bio": data.get("bio_description") or "",
        "avatar_url": data.get("avatar_url") or "",
        "is_verified": bool(data.get("is_verified")),
        "follower_count": data.get("follower_count"),
        "following_count": data.get("following_count"),
        "likes_count": data.get("likes_count"),
        "video_count": data.get("video_count"),
        "bio_url": data.get("bio_url") or data.get("bio_URL") or "",
    }


def profile_output_fields(profile: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not profile:
        return empty_profile_fields()

    def num(key: str) -> str:
        val = profile.get(key)
        if val is None or val == "":
            return ""
        return str(val)

    return {
        "tiktok_username": str(profile.get("username") or ""),
        "tiktok_display_name": str(profile.get("display_name") or ""),
        "tiktok_bio_description": str(profile.get("bio") or ""),
        "tiktok_avatar_url": str(profile.get("avatar_url") or ""),
        "tiktok_is_verified": (
            "true" if profile.get("is_verified") else "false"
        ),
        "tiktok_follower_count": num("follower_count"),
        "tiktok_following_count": num("following_count"),
        "tiktok_likes_count": num("likes_count"),
        "tiktok_video_count": num("video_count"),
        "tiktok_bio_url": str(profile.get("bio_url") or ""),
    }


def build_row(
    journalist: Journalist,
    candidate: Candidate,
    *,
    api_status: str,
    http_status: str,
    already_known: bool,
    already_known_in: str,
    profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    score, reason, reasons = score_match(journalist, candidate, profile)
    row = {
        "paperboy_name": journalist.name,
        "paperboy_x_handle": journalist.x_handle,
        "paperboy_outlet": journalist.outlet,
        "paperboy_outlet_primary": journalist.outlet_primary,
        "paperboy_city": "",
        "paperboy_state": "",
        "paperboy_notes": journalist.notes,
        "paperboy_usable": journalist.usable,
        "paperboy_masthead_handles": ", ".join(journalist.masthead_handles),
        "paperboy_csv_tiktok_handle": journalist.csv_tiktok_handle,
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
    row.update(profile_output_fields(profile))
    return row


def lookup_user_info(
    client: Any,
    username: str,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> Tuple[str, Optional[int], Optional[Dict[str, Any]]]:
    """Probe ``research/user/info``. Returns (status, http_status, profile).

    status is found | not_found | failed. 429/401/403 raise DiscoveryStop.
    """
    last_status: Optional[int] = None
    for attempt in range(HTTP_RETRY_ATTEMPTS):
        try:
            result = client.post_with_status(
                endpoint="research/user/info/",
                body={"username": username},
                params={"fields": USER_INFO_FIELDS},
                handle=username,
                raise_on_rate_limit=True,
            )
        except RuntimeError as exc:
            msg = str(exc)
            if (
                "daily_quota_limit_exceeded" in msg
                or "authentication_failure" in msg
                or "rate_limited" in msg
                or "HTTP 429" in msg
            ):
                raise DiscoveryStop(msg) from exc
            raise

        http_status = result.get("http_status")
        last_status = http_status if http_status is not None else last_status
        if result.get("ok"):
            body = result.get("body") or {}
            data = body.get("data") if isinstance(body, dict) else {}
            if not isinstance(data, dict):
                data = {}
            return "found", http_status, profile_from_api_data(data, username)

        if http_status in (400, 404):
            return "not_found", http_status, None
        if http_status is None or (isinstance(http_status, int) and http_status >= 500):
            if attempt + 1 < HTTP_RETRY_ATTEMPTS:
                sleep(HTTP_RETRY_SLEEP_SECONDS)
                continue
            return "failed", http_status, None
        return "not_found", http_status, None
    return "failed", last_status, None


def _ordered_rows(
    journalists: List[Journalist],
    rows_by_key: Dict[str, Dict[str, str]],
) -> List[Dict[str, str]]:
    order = {j.name.strip().lower(): i for i, j in enumerate(journalists)}
    items = list(rows_by_key.values())

    def sort_key(row: Dict[str, str]):
        name = (row.get("paperboy_name") or "").strip().lower()
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
            "run_json": RUN_JSON_SAMPLE,
        }
    return {
        "checkpoint": CHECKPOINT_FULL,
        "output_csv": OUTPUT_CSV_FULL,
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
    run_json: Optional[str] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    """Generate candidates, skip known handles, probe user/info, write review CSV."""
    paths = discovery_paths(sample=sample)
    checkpoint_path = checkpoint_path or paths["checkpoint"]
    output_csv = output_csv or paths["output_csv"]
    run_json = run_json or paths["run_json"]

    journalists = parse_journalists(csv_path)
    if sample:
        journalists = journalists[:SAMPLE_JOURNALIST_COUNT]

    ckpt = DiscoveryCheckpoint(checkpoint_path)
    if reset_checkpoints:
        ckpt.reset()
    elif retry_failed:
        ckpt.clear_failed()

    stats = {
        "workflow": "paperboy_news_account_discovery",
        "sample": bool(sample),
        "csv_path": csv_path,
        "journalists": len(journalists),
        "candidates_generated": 0,
        "candidates_already_known": 0,
        "api_calls_attempted": 0,
        "successful_user_info_responses": 0,
        "accounts_not_found": 0,
        "api_errors": 0,
        "rate_limit_or_quota_errors": 0,
        "high_confidence_candidates": 0,
        "output_csv": output_csv,
        "checkpoint": checkpoint_path,
        "run_json": run_json,
        "stopped_reason": "",
        "finished_at": "",
    }

    stopped_reason = ""
    try:
        for journalist in journalists:
            for candidate in generate_candidates(journalist):
                stats["candidates_generated"] += 1
                key = checkpoint_key(journalist.name, candidate.username)
                source = known_source(candidate.username, known)
                already = bool(source)

                if ckpt.is_completed_key(key):
                    continue
                if ckpt.is_failed_key(key) and not retry_failed:
                    continue

                if already:
                    stats["candidates_already_known"] += 1
                    row = build_row(
                        journalist,
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
                    logger.error("Discovery stopped: %s", stopped_reason)
                    raise

                if api_status == "found":
                    stats["successful_user_info_responses"] += 1
                elif api_status == "not_found":
                    stats["accounts_not_found"] += 1
                else:
                    stats["api_errors"] += 1

                row = build_row(
                    journalist,
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

    rows = _ordered_rows(journalists, ckpt._rows)
    write_output_csv(output_csv, rows)
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
    stats["high_confidence_candidates"] = sum(1 for r in rows if is_high_confidence(r))
    stats["output_rows"] = len(rows)
    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_run_json(run_json, stats)
    return stats
