from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import cast
from zoneinfo import ZoneInfo

import polars as pl
import yfinance_pl as yfp


def _ensure_dt(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=ZoneInfo("UTC"))
    dt = datetime.fromisoformat(str(value))
    return dt if dt.tzinfo else dt.replace(tzinfo=ZoneInfo("UTC"))


def fetch_history(
    ticker: str,
    start: datetime | str,
    end: datetime | str,
    interval: str = "1h",
) -> pl.DataFrame:
    """
    Fetch OHLCV data with yfinance-pl and return a Polars DataFrame with float columns.
    """
    start_dt = _ensure_dt(start)
    end_dt = _ensure_dt(end)

    lookback_days = max(1, (datetime.now(tz=ZoneInfo("UTC")) - start_dt).days)
    intraday = interval.endswith("m") or interval.endswith("h")

    def choose_period(days: int) -> str:
        if intraday:
            if days <= 30:
                return "60d"
            if days <= 90:
                return "6mo"
            if days <= 365:
                return "1y"
            return "2y"  # Yahoo caps intraday history; keep under ~2 years
        # daily/longer bars
        if days <= 30:
            return "3mo"
        if days <= 90:
            return "6mo"
        if days <= 200:
            return "1y"
        if days <= 400:
            return "2y"
        if days <= 1200:
            return "5y"
        return "10y"

    candidate_periods = [choose_period(lookback_days)]
    if intraday:
        candidate_periods.extend(
            p for p in ["1y", "6mo", "3mo", "60d", "30d"] if p not in candidate_periods
        )
    else:
        candidate_periods.extend(
            p for p in ["2y", "1y", "6mo", "3mo"] if p not in candidate_periods
        )

    t = yfp.Ticker(ticker)
    df = pl.DataFrame()
    last_err: Exception | None = None
    for period in candidate_periods:
        try:
            df = t.history(
                period=cast(yfp.Period, period), interval=cast(yfp.Interval, interval)
            )
            if not df.is_empty():
                break
        except (
            Exception
        ) as exc:  # yahoo sometimes throws 4xx for long intraday lookbacks
            last_err = exc
            continue
    if df.is_empty():
        if last_err:
            raise last_err
        return pl.DataFrame()
    if df.is_empty():
        return pl.DataFrame()

    df = (
        df.rename(
            {
                "date": "timestamp",
                "open.amount": "open",
                "high.amount": "high",
                "low.amount": "low",
                "close.amount": "close",
                "volume": "volume",
            }
        )
        .select(
            [
                pl.col("timestamp"),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
            ]
        )
        .with_columns(
            pl.col("timestamp").dt.replace_time_zone("UTC").dt.cast_time_unit("us")
        )
        .filter((pl.col("timestamp") >= start_dt) & (pl.col("timestamp") <= end_dt))
        .sort("timestamp")
    )
    return df


def fetch_assets(
    tickers: Iterable[str],
    start: datetime | str,
    end: datetime | str,
    interval: str = "1h",
) -> dict[str, pl.DataFrame]:
    """Fetch multiple tickers."""
    out: dict[str, pl.DataFrame] = {}
    for t in tickers:
        out[t] = fetch_history(t, start=start, end=end, interval=interval)
    return out
