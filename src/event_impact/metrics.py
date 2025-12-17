from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import polars as pl

from .events import Event, EventWindowConfig


@dataclass
class EventImpact:
    """Container for event-level analytics for one asset."""

    asset: str
    event: Event
    pre_return: float
    post_return: float
    pre_vol: float
    post_vol: float
    vol_change: float
    max_drawdown_post: float
    reaction_minutes: float | None


def _window(df: pl.DataFrame, start: datetime, end: datetime) -> pl.DataFrame:
    return df.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))


def _returns_expr() -> pl.Expr:
    return pl.col("close").log().diff().alias("log_ret")


def _realized_vol(log_returns: pl.Series) -> float:
    if log_returns.is_null().all() or log_returns.len() == 0:
        return float("nan")
    clean = log_returns.drop_nulls().cast(pl.Float64, strict=False)
    if clean.is_empty():
        return float("nan")
    val = clean.to_numpy().std()
    return float(val)


def _max_drawdown_from(ref_price: float, prices: pl.Series) -> float:
    if prices.is_null().all() or ref_price <= 0:
        return float("nan")
    cumulative = (prices / ref_price) - 1
    peak = float("-inf")
    max_dd = 0.0
    for val in cumulative:
        if val is None:
            continue
        if val > peak:
            peak = val
        drawdown = val - peak
        if drawdown < max_dd:
            max_dd = drawdown
    return max_dd


def _reaction_time_minutes(
    event_ts: datetime, ref_price: float, timestamps: pl.Series, prices: pl.Series
) -> float | None:
    if len(prices) == 0 or ref_price == 0:
        return None
    cum = (prices / ref_price) - 1
    abs_cum = cum.abs()
    idx = abs_cum.arg_max()
    if idx is None:
        return None
    target_ts = timestamps[idx]
    if event_ts is None or target_ts is None:
        return None
    delta = target_ts - event_ts
    return delta.total_seconds() / 60.0


def analyze_event(
    asset: str,
    df: pl.DataFrame,
    event: Event,
    window: EventWindowConfig,
) -> EventImpact:
    """
    Compute returns, vol changes, drawdowns, and reaction time for one asset around one event.
    """
    event_ts = event.utc_timestamp()
    pre_start = event_ts - window.pre
    post_end = event_ts + window.post

    pre_df = _window(df, pre_start, event_ts)
    post_df = _window(df, event_ts, post_end)

    if pre_df.height == 0 or post_df.height == 0:
        return EventImpact(
            asset,
            event,
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            None,
        )

    ref_price = float(pre_df.select(pl.col("close").last()).item())
    first_price = float(pre_df.select(pl.col("close").first()).item())
    last_price = float(post_df.select(pl.col("close").last()).item())

    pre_ret = ref_price / first_price - 1 if first_price else float("nan")
    post_ret = last_price / ref_price - 1 if ref_price else float("nan")

    pre_vol = _realized_vol(pre_df.select(_returns_expr()).to_series())
    post_vol = _realized_vol(post_df.select(_returns_expr()).to_series())
    vol_change = post_vol - pre_vol

    max_drawdown_post = _max_drawdown_from(ref_price, post_df["close"])
    reaction_minutes = _reaction_time_minutes(
        event_ts, ref_price, post_df["timestamp"], post_df["close"]
    )

    return EventImpact(
        asset=asset,
        event=event,
        pre_return=pre_ret,
        post_return=post_ret,
        pre_vol=pre_vol,
        post_vol=post_vol,
        vol_change=vol_change,
        max_drawdown_post=max_drawdown_post,
        reaction_minutes=reaction_minutes,
    )


def summarize_impacts(impacts: list[EventImpact]) -> pl.DataFrame:
    """Convert a list of EventImpact objects into a Polars table for reporting."""
    rows = []
    for imp in impacts:
        rows.append(
            {
                "event": imp.event.name,
                "category": imp.event.category,
                "asset": imp.asset,
                "pre_return": imp.pre_return,
                "post_return": imp.post_return,
                "pre_vol": imp.pre_vol,
                "post_vol": imp.post_vol,
                "vol_change": imp.vol_change,
                "max_drawdown_post": imp.max_drawdown_post,
                "reaction_minutes": imp.reaction_minutes,
                "event_time_utc": imp.event.utc_timestamp(),
            }
        )
    return pl.DataFrame(rows)
