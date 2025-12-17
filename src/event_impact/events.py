from __future__ import annotations

import csv
import json
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


@dataclass
class Event:
    """Macro or micro event such as CPI, FOMC, or earnings."""

    name: str
    category: str
    timestamp: datetime
    tickers: list[str] = field(default_factory=list)
    metadata: dict | None = None

    def utc_timestamp(self) -> datetime:
        """Return event timestamp converted to UTC."""
        if self.timestamp.tzinfo is None:
            # Assume New York if not provided to keep consistent macro timing.
            return self.timestamp.replace(
                tzinfo=ZoneInfo("America/New_York")
            ).astimezone(ZoneInfo("UTC"))
        return self.timestamp.astimezone(ZoneInfo("UTC"))


@dataclass
class EventWindowConfig:
    """Window before/after an event."""

    pre: timedelta
    post: timedelta

    @classmethod
    def intraday_default(cls) -> "EventWindowConfig":
        return cls(pre=timedelta(hours=24), post=timedelta(hours=24))

    @classmethod
    def daily_default(cls) -> "EventWindowConfig":
        return cls(pre=timedelta(days=5), post=timedelta(days=5))


def _us_cpi_2024() -> list[Event]:
    nyc = ZoneInfo("America/New_York")
    # 8:30am ET CPI releases.
    dates = [
        datetime(2024, 1, 11, 8, 30, tzinfo=nyc),
        datetime(2024, 2, 13, 8, 30, tzinfo=nyc),
        datetime(2024, 3, 12, 8, 30, tzinfo=nyc),
        datetime(2024, 4, 10, 8, 30, tzinfo=nyc),
        datetime(2024, 5, 15, 8, 30, tzinfo=nyc),
        datetime(2024, 6, 12, 8, 30, tzinfo=nyc),
        datetime(2024, 7, 11, 8, 30, tzinfo=nyc),
        datetime(2024, 8, 14, 8, 30, tzinfo=nyc),
        datetime(2024, 9, 11, 8, 30, tzinfo=nyc),
        datetime(2024, 10, 10, 8, 30, tzinfo=nyc),
        datetime(2024, 11, 13, 8, 30, tzinfo=nyc),
        datetime(2024, 12, 11, 8, 30, tzinfo=nyc),
    ]
    return [Event(name=f"US CPI {d:%b %Y}", category="cpi", timestamp=d) for d in dates]


def _us_cpi_2025() -> list[Event]:
    nyc = ZoneInfo("America/New_York")
    dates = [
        datetime(2025, 1, 14, 8, 30, tzinfo=nyc),
        datetime(2025, 2, 12, 8, 30, tzinfo=nyc),
        datetime(2025, 3, 12, 8, 30, tzinfo=nyc),
        datetime(2025, 4, 9, 8, 30, tzinfo=nyc),
        datetime(2025, 5, 14, 8, 30, tzinfo=nyc),
        datetime(2025, 6, 11, 8, 30, tzinfo=nyc),
        datetime(2025, 7, 15, 8, 30, tzinfo=nyc),
        datetime(2025, 8, 13, 8, 30, tzinfo=nyc),
        datetime(2025, 9, 10, 8, 30, tzinfo=nyc),
        datetime(2025, 10, 15, 8, 30, tzinfo=nyc),
        datetime(2025, 11, 12, 8, 30, tzinfo=nyc),
        datetime(2025, 12, 10, 8, 30, tzinfo=nyc),
    ]
    return [Event(name=f"US CPI {d:%b %Y}", category="cpi", timestamp=d) for d in dates]


def _fomc_2024() -> list[Event]:
    nyc = ZoneInfo("America/New_York")
    # 2:00pm ET statement.
    dates = [
        datetime(2024, 1, 31, 14, 0, tzinfo=nyc),
        datetime(2024, 3, 20, 14, 0, tzinfo=nyc),
        datetime(2024, 5, 1, 14, 0, tzinfo=nyc),
        datetime(2024, 6, 12, 14, 0, tzinfo=nyc),
        datetime(2024, 7, 31, 14, 0, tzinfo=nyc),
        datetime(2024, 9, 18, 14, 0, tzinfo=nyc),
        datetime(2024, 11, 7, 14, 0, tzinfo=nyc),
        datetime(2024, 12, 18, 14, 0, tzinfo=nyc),
    ]
    return [Event(name=f"FOMC {d:%b %d}", category="fomc", timestamp=d) for d in dates]


def _fomc_2025() -> list[Event]:
    nyc = ZoneInfo("America/New_York")
    dates = [
        datetime(2025, 1, 29, 14, 0, tzinfo=nyc),
        datetime(2025, 3, 19, 14, 0, tzinfo=nyc),
        datetime(2025, 4, 30, 14, 0, tzinfo=nyc),
        datetime(2025, 6, 11, 14, 0, tzinfo=nyc),
        datetime(2025, 7, 30, 14, 0, tzinfo=nyc),
        datetime(2025, 9, 17, 14, 0, tzinfo=nyc),
        datetime(2025, 11, 5, 14, 0, tzinfo=nyc),
        datetime(2025, 12, 17, 14, 0, tzinfo=nyc),
    ]
    return [Event(name=f"FOMC {d:%b %d}", category="fomc", timestamp=d) for d in dates]


def _earnings_2024() -> list[Event]:
    nyc = ZoneInfo("America/New_York")
    # A small sample; users can extend by passing custom events.
    events = [
        Event(
            "AAPL FY23 Q4",
            "earnings",
            datetime(2024, 2, 1, 16, 0, tzinfo=nyc),
            tickers=["AAPL"],
        ),
        Event(
            "MSFT FY24 Q2",
            "earnings",
            datetime(2024, 1, 30, 16, 0, tzinfo=nyc),
            tickers=["MSFT"],
        ),
        Event(
            "NVDA FY24 Q4",
            "earnings",
            datetime(2024, 2, 21, 16, 20, tzinfo=nyc),
            tickers=["NVDA"],
        ),
        Event(
            "AMZN FY23 Q4",
            "earnings",
            datetime(2024, 2, 1, 16, 0, tzinfo=nyc),
            tickers=["AMZN"],
        ),
    ]
    return events


def _earnings_2025() -> list[Event]:
    nyc = ZoneInfo("America/New_York")
    events = [
        Event(
            "AAPL FY25 Q1",
            "earnings",
            datetime(2025, 1, 30, 16, 0, tzinfo=nyc),
            tickers=["AAPL"],
        ),
        Event(
            "MSFT FY25 Q2",
            "earnings",
            datetime(2025, 1, 29, 16, 0, tzinfo=nyc),
            tickers=["MSFT"],
        ),
        Event(
            "NVDA FY25 Q4",
            "earnings",
            datetime(2025, 2, 26, 16, 20, tzinfo=nyc),
            tickers=["NVDA"],
        ),
        Event(
            "AMZN FY24 Q4",
            "earnings",
            datetime(2025, 2, 6, 16, 0, tzinfo=nyc),
            tickers=["AMZN"],
        ),
    ]
    return events


STATIC_EVENTS = {
    2024: _us_cpi_2024() + _fomc_2024() + _earnings_2024(),
    2025: _us_cpi_2025() + _fomc_2025() + _earnings_2025(),
}


def sample_events(
    categories: Iterable[str] | None = None, year: int = 2025
) -> list[Event]:
    """Return a curated list of events for quick exploration (defaults to 2025)."""
    cat_set = {c.lower() for c in categories} if categories else None
    events = STATIC_EVENTS.get(year, [])
    if cat_set is None:
        return events
    return [ev for ev in events if ev.category.lower() in cat_set]


def load_events_from_json(path: str | Path) -> list[Event]:
    data = json.loads(Path(path).read_text())
    events: list[Event] = []
    for row in data:
        dt = datetime.fromisoformat(row["timestamp"])
        tickers = row.get("tickers") or []
        events.append(
            Event(
                name=row["name"],
                category=row.get("category", "custom"),
                timestamp=dt,
                tickers=tickers,
                metadata=row.get("metadata"),
            )
        )
    return events


def load_events_from_csv(path: str | Path) -> list[Event]:
    events: list[Event] = []
    with Path(path).open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = datetime.fromisoformat(row["timestamp"])
            tickers = [
                t.strip() for t in (row.get("tickers") or "").split(",") if t.strip()
            ]
            events.append(
                Event(
                    name=row["name"],
                    category=row.get("category", "custom"),
                    timestamp=dt,
                    tickers=tickers,
                    metadata={
                        k: v
                        for k, v in row.items()
                        if k not in {"name", "category", "timestamp", "tickers"}
                    },
                )
            )
    return events


def load_events_from_ics(url: str, category: str = "macro") -> list[Event]:
    """
    Lightweight ICS fetcher for free calendars (e.g., BLS/FED if provided).
    Expects DTSTART in UTC or with TZID; SUMMARY used as name.
    """
    import httpx

    # Allow local file paths to avoid network issues.
    path = Path(url)
    if path.exists():
        lines = path.read_text().splitlines()
    else:
        resp = httpx.get(
            url,
            headers={
                "User-Agent": "event-impact/0.1 (+https://github.com/)",
                "Accept": "text/calendar, text/plain; q=0.9, */*; q=0.8",
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        lines = resp.text.splitlines()
    events: list[Event] = []
    current: dict[str, str] = {}
    for line in lines:
        if line.startswith("BEGIN:VEVENT"):
            current = {}
        elif line.startswith("END:VEVENT"):
            if "DTSTART" in current and "SUMMARY" in current:
                dt_raw = current["DTSTART"]
                dt_match = re.search(r":(.+)$", dt_raw)
                dt_val = dt_match.group(1) if dt_match else dt_raw
                if "T" in dt_val:
                    if dt_val.endswith("Z"):
                        dt = datetime.strptime(dt_val, "%Y%m%dT%H%M%SZ").replace(
                            tzinfo=ZoneInfo("UTC")
                        )
                    else:
                        dt = datetime.strptime(dt_val, "%Y%m%dT%H%M%S")
                        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                else:
                    dt = datetime.strptime(dt_val, "%Y%m%d").replace(
                        tzinfo=ZoneInfo("UTC")
                    )
                events.append(
                    Event(name=current["SUMMARY"], category=category, timestamp=dt)
                )
        else:
            if ":" in line:
                key, val = line.split(":", 1)
                current[key] = val
    return events


def _parse_date_like(value: str | date | datetime) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()


def _fred_release_name(release_id: int, api_key: str) -> str:
    import httpx

    resp = httpx.get(
        "https://api.stlouisfed.org/fred/release",
        params={"file_type": "json", "api_key": api_key, "release_id": release_id},
        timeout=15.0,
    )
    resp.raise_for_status()
    data = resp.json().get("release", {})
    return data.get("name", f"Release {release_id}")


def load_events_from_fred(
    release_ids: dict[str, int],
    start: str | date | datetime,
    end: str | date | datetime,
    tz: str = "America/New_York",
    release_time: str = "08:30",
) -> list[Event]:
    """
    Build events from FRED release dates.
    release_ids: mapping category -> release_id (see FRED docs).
    start/end: date or ISO string bounds.
    release_time: HH:MM (local tz) to stamp the event.
    """
    import httpx

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise RuntimeError("FRED_API_KEY not set in environment")

    start_d = _parse_date_like(start)
    end_d = _parse_date_like(end)
    hour, minute = [int(x) for x in release_time.split(":")]
    tzinfo = ZoneInfo(tz)

    events: list[Event] = []
    for cat, rid in release_ids.items():
        # fetch release name once
        try:
            release_name = _fred_release_name(rid, api_key)
        except Exception:
            release_name = f"FRED {cat}"
        resp = httpx.get(
            "https://api.stlouisfed.org/fred/release/dates",
            params={
                "file_type": "json",
                "api_key": api_key,
                "release_id": rid,
                "include_release_dates_with_no_data": False,
                "limit": 1000,
                "offset": 0,
                "start_date": start_d.isoformat(),
                "end_date": end_d.isoformat(),
            },
            timeout=15.0,
        )
        resp.raise_for_status()
        data = resp.json()
        for row in data.get("release_dates", []):
            d = datetime.fromisoformat(row["date"]).date()
            if d < start_d or d > end_d:
                continue
            ts = datetime(d.year, d.month, d.day, hour, minute, tzinfo=tzinfo)
            events.append(
                Event(
                    name=f"{release_name} {d.isoformat()}",
                    category=cat,
                    timestamp=ts,
                    metadata={"source": "fred", "release_id": rid},
                )
            )
    return events
