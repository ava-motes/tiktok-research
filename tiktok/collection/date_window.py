"""Research-date windows for daily collection pipelines.

Storage timestamps are UTC. The research calendar day uses a configurable
timezone (default America/Chicago).

For ``--date YYYY-MM-DD`` the collection window is the half-open interval
``[start_of_day, start_of_next_day)`` in that timezone, converted to UTC.

TikTok Research API ``start_date`` / ``end_date`` are YYYYMMDD strings. This
module emits a **half-open** API range compatible with existing
``date_chunks(start, end)`` (which requires ``start < end`` and otherwise
returns no chunks). Videos are then filtered by ``create_time`` so adjacent
UTC calendar days are not kept.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

DEFAULT_RESEARCH_TIMEZONE = "America/Chicago"


@dataclass(frozen=True)
class ResearchWindow:
    """One research calendar day mapped to UTC + API query dates."""

    research_date: str  # YYYY-MM-DD
    timezone_name: str
    start_utc: datetime
    end_utc: datetime  # exclusive
    api_start_yyyymmdd: str  # inclusive API start (UTC calendar)
    api_end_yyyymmdd: str  # exclusive end for date_chunks()

    @property
    def collection_window_start(self) -> str:
        return self.start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def collection_window_end(self) -> str:
        return self.end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def start_unix(self) -> int:
        return int(self.start_utc.timestamp())

    @property
    def end_unix(self) -> int:
        return int(self.end_utc.timestamp())

    def contains_create_time(self, create_time: Optional[int]) -> bool:
        """True if unix create_time falls in [start, end)."""
        if create_time is None:
            return False
        try:
            ts = int(create_time)
        except (TypeError, ValueError):
            return False
        return self.start_unix <= ts < self.end_unix


def parse_research_date(value: str) -> str:
    """Normalize YYYY-MM-DD or YYYYMMDD to YYYY-MM-DD."""
    raw = (value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        raw = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    datetime.strptime(raw, "%Y-%m-%d")
    return raw


def research_window(
    research_date: str,
    *,
    timezone_name: str = DEFAULT_RESEARCH_TIMEZONE,
) -> ResearchWindow:
    """Build the UTC window and API date coverage for one research day."""
    day = parse_research_date(research_date)
    tz = ZoneInfo(timezone_name or DEFAULT_RESEARCH_TIMEZONE)
    start_local = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    api_start = start_utc.strftime("%Y%m%d")
    last_instant = end_utc - timedelta(microseconds=1)
    last_utc_date = datetime.strptime(last_instant.strftime("%Y%m%d"), "%Y%m%d")
    api_end_exclusive = (last_utc_date + timedelta(days=1)).strftime("%Y%m%d")
    if api_start >= api_end_exclusive:
        # Should not happen; keep a one-day exclusive end so date_chunks is non-empty.
        api_end_exclusive = (
            datetime.strptime(api_start, "%Y%m%d") + timedelta(days=1)
        ).strftime("%Y%m%d")

    return ResearchWindow(
        research_date=day,
        timezone_name=timezone_name or DEFAULT_RESEARCH_TIMEZONE,
        start_utc=start_utc,
        end_utc=end_utc,
        api_start_yyyymmdd=api_start,
        api_end_yyyymmdd=api_end_exclusive,
    )


def today_research_date(timezone_name: str = DEFAULT_RESEARCH_TIMEZONE) -> str:
    tz = ZoneInfo(timezone_name or DEFAULT_RESEARCH_TIMEZONE)
    return datetime.now(tz).date().isoformat()
