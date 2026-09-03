"""Research-date windows for daily collection pipelines.

Storage timestamps are UTC. The research calendar day uses a configurable
timezone (default America/Chicago).

For ``--date YYYY-MM-DD`` the collection window is the half-open interval
``[start_of_day, start_of_next_day)`` in that timezone, converted to UTC.

TikTok Research API ``start_date`` / ``end_date`` are inclusive UTC calendar
days (YYYYMMDD). The API has no hour/time filter. This module therefore
emits the **inclusive** UTC dates that overlap the research window (typically
two for America/Chicago, never a third unused day). ``create_time`` still
drops hours that fall outside the Chicago day.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from api.videos import date_chunks

DEFAULT_RESEARCH_TIMEZONE = "America/Chicago"


@dataclass(frozen=True)
class ResearchWindow:
    """One research calendar day mapped to UTC + API query dates."""

    research_date: str  # YYYY-MM-DD
    timezone_name: str
    start_utc: datetime
    end_utc: datetime  # exclusive
    api_start_yyyymmdd: str  # inclusive TikTok start_date (UTC calendar)
    api_end_yyyymmdd: str  # inclusive TikTok end_date (UTC calendar)

    def api_query_chunks(self, max_days: int = 30) -> List[Tuple[str, str]]:
        """Inclusive ``(start_date, end_date)`` pairs to send to TikTok.

        ``date_chunks`` yields nothing when start == end (one UTC day); still
        query that day once.
        """
        chunks = date_chunks(
            self.api_start_yyyymmdd, self.api_end_yyyymmdd, max_days=max_days
        )
        if not chunks:
            return [(self.api_start_yyyymmdd, self.api_end_yyyymmdd)]
        return chunks

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


def utc_calendar_window(research_date: str) -> ResearchWindow:
    """One inclusive UTC calendar day: TikTok ``start_date`` == ``end_date``.

    Keep every video the API returns for that YYYYMMDD. Do not expand to a
    second UTC day and do not apply a Chicago hour filter.
    """
    day = parse_research_date(research_date)
    start_utc = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_utc = start_utc + timedelta(days=1)
    yyyymmdd = start_utc.strftime("%Y%m%d")
    return ResearchWindow(
        research_date=day,
        timezone_name="UTC",
        start_utc=start_utc,
        end_utc=end_utc,
        api_start_yyyymmdd=yyyymmdd,
        api_end_yyyymmdd=yyyymmdd,
    )


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

    # Inclusive UTC dates that overlap [start_utc, end_utc). TikTok treats
    # both start_date and end_date as inclusive, so do not add a +1 day here.
    api_start = start_utc.strftime("%Y%m%d")
    last_instant = end_utc - timedelta(microseconds=1)
    api_end = last_instant.strftime("%Y%m%d")
    if api_end < api_start:
        api_end = api_start

    return ResearchWindow(
        research_date=day,
        timezone_name=timezone_name or DEFAULT_RESEARCH_TIMEZONE,
        start_utc=start_utc,
        end_utc=end_utc,
        api_start_yyyymmdd=api_start,
        api_end_yyyymmdd=api_end,
    )


def today_research_date(timezone_name: str = DEFAULT_RESEARCH_TIMEZONE) -> str:
    tz = ZoneInfo(timezone_name or DEFAULT_RESEARCH_TIMEZONE)
    return datetime.now(tz).date().isoformat()
