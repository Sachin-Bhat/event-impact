from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

from .data import fetch_assets
from .events import (
    EventWindowConfig,
    dedupe_events,
    load_events_from_csv,
    load_events_from_fred,
    load_events_from_ics,
    load_events_from_json,
    sample_events,
)
from .metrics import analyze_event, summarize_impacts


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure how markets react to macro events using free Yahoo Finance data."
    )
    parser.add_argument(
        "--assets",
        default="SPY,QQQ,GLD,TLT,EURUSD=X,CL=F",
        help="Comma separated tickers to study (default: SPY,QQQ,GLD,TLT,EURUSD=X,CL=F).",
    )
    parser.add_argument(
        "--interval",
        default="1h",
        help="Price interval passed to Yahoo Finance (1h or 1d are most reliable).",
    )
    parser.add_argument(
        "--categories",
        default="cpi,fomc,earnings",
        help="Which event categories to load from the built-in calendar.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Calendar year for built-in events (default: 2025).",
    )
    parser.add_argument(
        "--pre-hours",
        type=float,
        default=24.0,
        help="Hours before event to include in the window.",
    )
    parser.add_argument(
        "--post-hours",
        type=float,
        default=24.0,
        help="Hours after event to include in the window.",
    )
    parser.add_argument(
        "--events-file",
        help="Optional CSV/JSON with columns name,category,timestamp[,tickers]. Appends to built-ins.",
    )
    parser.add_argument(
        "--events-ics-url",
        help="Optional ICS URL for a free calendar (e.g., BLS/FED). Appends to built-ins.",
    )
    parser.add_argument(
        "--events-ics-category",
        default="macro",
        help="Category label to apply to ICS events (default: macro).",
    )
    parser.add_argument(
        "--events-fred-release-ids",
        help='Optional mapping of category to FRED release_id, e.g. "cpi=9,fomc=10,employment=50". Requires FRED_API_KEY in env.',
    )
    parser.add_argument(
        "--fred-start",
        help="Start date (YYYY-MM-DD) for FRED release fetch (defaults to Jan 1 of --year).",
    )
    parser.add_argument(
        "--fred-end",
        help="End date (YYYY-MM-DD) for FRED release fetch (defaults to Dec 31 of --year).",
    )
    parser.add_argument(
        "--fred-time-overrides",
        help='Override release time per category, e.g. "fomc=14:00,cpi=08:30".',
    )
    parser.add_argument(
        "--fred-rolling-days",
        type=int,
        help="If set, fetch FRED releases from today to today+N days (overrides fred-start/fred-end defaults).",
    )
    parser.add_argument(
        "--output-csv",
        help="Optional path to write the impact table as CSV.",
    )
    return parser.parse_args()


def _date_bounds(events, window: EventWindowConfig):
    earliest = min(ev.utc_timestamp() - window.pre for ev in events)
    latest = max(ev.utc_timestamp() + window.post for ev in events)
    return earliest, latest


def main() -> None:
    load_dotenv()
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    assets = [a.strip() for a in args.assets.split(",") if a.strip()]
    categories = [c.strip() for c in args.categories.split(",") if c.strip()]

    window = EventWindowConfig(
        pre=timedelta(hours=args.pre_hours),
        post=timedelta(hours=args.post_hours),
    )

    events = sample_events(categories, year=args.year)
    if args.events_file:
        path = args.events_file
        if path.lower().endswith(".json"):
            events.extend(load_events_from_json(path))
        else:
            events.extend(load_events_from_csv(path))
    if args.events_ics_url:
        try:
            events.extend(
                load_events_from_ics(
                    args.events_ics_url, category=args.events_ics_category
                )
            )
        except Exception as exc:  # pragma: no cover
            logging.warning("failed to load ICS events: %s", exc)
    if args.events_fred_release_ids:
        mapping: dict[str, int] = {}
        for pair in args.events_fred_release_ids.split(","):
            if not pair.strip():
                continue
            key, val = pair.split("=", 1)
            mapping[key.strip()] = int(val.strip())
        if args.fred_rolling_days:
            today = datetime.now(timezone.utc).date()
            fred_start = today.isoformat()
            fred_end = (today + timedelta(days=args.fred_rolling_days)).isoformat()
        else:
            fred_start = args.fred_start or f"{args.year}-01-01"
            fred_end = args.fred_end or f"{args.year}-12-31"
        time_overrides: dict[str, str] = {}
        if args.fred_time_overrides:
            for pair in args.fred_time_overrides.split(","):
                if not pair.strip():
                    continue
                k, v = pair.split("=", 1)
                time_overrides[k.strip()] = v.strip()
        try:
            events.extend(
                load_events_from_fred(
                    mapping,
                    fred_start,
                    fred_end,
                    time_overrides=time_overrides,
                )
            )
        except Exception as exc:
            logging.warning("failed to load FRED events: %s", exc)
    if not events:
        raise SystemExit("No events matched categories.")

    events = dedupe_events(events, prefer_fred=True)

    start, end = _date_bounds(events, window)
    logging.info(
        "Fetching %s from %s to %s at %s...",
        assets,
        start.date(),
        end.date(),
        args.interval,
    )
    price_map = fetch_assets(assets, start=start, end=end, interval=args.interval)

    impacts = []
    for asset, df in price_map.items():
        if df.is_empty():
            logging.warning("no data for %s", asset)
            continue
        # ensure timezone aware for comparisons
        df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))
        for ev in events:
            impacts.append(analyze_event(asset, df, ev, window))

    results = summarize_impacts(impacts)
    if results.is_empty():
        print("No impacts computed.")
        return

    # Rank by fastest reaction
    ranked = (
        results.with_columns(
            pl.col(
                [
                    "pre_return",
                    "post_return",
                    "vol_change",
                    "max_drawdown_post",
                    "reaction_minutes",
                ]
            ).cast(pl.Float64)
        )
        .sort(["event_time_utc", "reaction_minutes"])
        .with_columns(
            pl.col("pre_return").round(4),
            pl.col("post_return").round(4),
            pl.col("vol_change").round(6),
            pl.col("max_drawdown_post").round(4),
            pl.col("reaction_minutes").round(2),
        )
    )

    print("\n=== Event impact table (lower reaction_minutes = faster pricing) ===")
    display_cols = [
        "event",
        "category",
        "asset",
        "post_return",
        "vol_change",
        "max_drawdown_post",
        "reaction_minutes",
    ]
    table = ranked.select(display_cols)
    print(table)

    if args.output_csv:
        out_path = Path(args.output_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        table.write_csv(out_path)
        logging.info("wrote CSV to %s", out_path)


if __name__ == "__main__":
    main()
