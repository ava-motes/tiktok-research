"""P1 then P2 production daily job — command construction only.

Does not collect, call TikTok, or write BigQuery. Used by
``common/scripts/run_daily_p1_p2.py`` and static checks.

P3 is intentionally absent. Whisper is skipped (OCR + emoji only).
GCS upload is left to each runner after a completed run
(API handle failures allowed for P1/P2).
"""

from __future__ import annotations

import os
from typing import List, Optional

from tiktok.collection.date_window import (
    DEFAULT_RESEARCH_TIMEZONE,
    lagged_research_date,
    parse_research_date,
)

DEFAULT_LAG_DAYS = 2
P1_SCRIPT = "p1_content_creators/scripts/run_content_creators.py"
P2_SCRIPT = "p2_news/scripts/run_news.py"
P1_PIPELINE = "content_creators"
P2_PIPELINE = "news"
P1_BQ_TABLE = "cfme-mediaengagment-prod.tiktok_research.content_creators"
P2_BQ_TABLE = "cfme-mediaengagment-prod.tiktok_research.news"
P1_GCS_PREFIX = "gs://tiktok_research_3/p1_content_creators"
P2_GCS_PREFIX = "gs://tiktok_research_3/p2_news"
FORBIDDEN_AUTO_FLAGS = (
    "--reset-checkpoints",
    "--sample",
    "--skip-gcs",
    "--skip-enrich",
    "--skip-bigquery",
)


def resolve_lag_days(lag_days: Optional[int] = None) -> int:
    if lag_days is not None:
        days = int(lag_days)
    else:
        raw = (os.environ.get("RESEARCH_LAG_DAYS") or str(DEFAULT_LAG_DAYS)).strip()
        days = int(raw)
    if days < 0:
        raise ValueError(f"lag_days must be >= 0, got {days!r}")
    return days


def resolve_research_date(
    explicit: Optional[str] = None,
    *,
    lag_days: Optional[int] = None,
    timezone_name: str = DEFAULT_RESEARCH_TIMEZONE,
) -> str:
    """Use ``DATE`` / ``explicit`` if set, otherwise Chicago today minus lag."""
    raw = (explicit or os.environ.get("DATE") or "").strip()
    if raw:
        return parse_research_date(raw)
    return lagged_research_date(
        lag_days=resolve_lag_days(lag_days),
        timezone_name=timezone_name,
    )


def p1_argv(research_date: str) -> List[str]:
    date = parse_research_date(research_date)
    return [
        P1_SCRIPT,
        "--date",
        date,
        "--utc-day",
        "--skip-whisper",
        "--continue-on-failures",
        "--skip-user-info",
    ]


def p2_argv(research_date: str) -> List[str]:
    date = parse_research_date(research_date)
    return [
        P2_SCRIPT,
        "--date",
        date,
        "--utc-day",
        "--skip-whisper",
    ]


def gcs_uri(pipeline: str, research_date: str) -> str:
    date = parse_research_date(research_date)
    if pipeline == P1_PIPELINE:
        return f"{P1_GCS_PREFIX}/{date}.csv"
    if pipeline == P2_PIPELINE:
        return f"{P2_GCS_PREFIX}/{date}.csv"
    raise ValueError(f"automation does not include pipeline {pipeline!r}")


def assert_safe_argv(argv: List[str]) -> None:
    joined = " ".join(argv)
    for flag in FORBIDDEN_AUTO_FLAGS:
        if flag in argv:
            raise ValueError(f"daily automation must not pass {flag}: {joined}")
    if "run_keyword.py" in joined or "--pipeline keyword" in joined:
        raise ValueError("daily automation must not run P3")
    if "--skip-whisper" not in argv:
        raise ValueError("daily automation must skip Whisper")
    if "--utc-day" not in argv:
        raise ValueError("daily automation must use --utc-day")
