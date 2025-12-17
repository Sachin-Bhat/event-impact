## Event Impact

Measure how markets digest scheduled events (CPI, FOMC, earnings) using free Yahoo Finance data.

### What this does
- Pulls prices with `yfinance-pl` (Polars-native).
- Defines pre/post event windows.
- Computes pre/post returns, realized vol changes, max drawdowns, and reaction speed (minutes to peak move).
- Compares reactions across assets to see which markets price news fastest.

### Quickstart
```bash
uv run event-impact --assets "SPY,QQQ,GLD,TLT,EURUSD=X,CL=F" --interval 1h --categories "cpi,fomc" --year 2025 --pre-hours 24 --post-hours 24
```

### Custom events
- Append local files: `--events-file my_events.csv` (columns: `name,category,timestamp[,tickers]`) or JSON array with the same keys.
- Append ICS feeds: `--events-ics-url https://.../calendar.ics [--events-ics-category macro]` (expects DTSTART/SUMMARY).
- Append FRED release dates (requires `FRED_API_KEY`): `--events-fred-release-ids "cpi=9,fomc=10,employment=50" [--fred-start YYYY-MM-DD --fred-end YYYY-MM-DD]`. Dates are stamped at 08:30 ET by default.
- Built-in calendars: CPI and FOMC schedules for 2024 and 2025 plus a small earnings sample. Default year is 2025; override with `--year 2024` if needed.

### Free sources to try
- BLS/BEA/other agencies often publish ICS calendars; pass the URL via `--events-ics-url`.
- FRED release dates via API using your key (release ids documented on FRED; e.g., CPI=9, FOMC Press Release=10, Employment Situation=50, GDP=53).
- For earnings, drop your own CSV/JSON from broker feeds/FTP exports.

Example with FRED release dates:
```bash
uv run event-impact \                                             ─╯
  --assets "SPY,QQQ,GLD,TLT,EURUSD=X,CL=F" \
  --interval 1h \
  --categories "cpi,fomc" \
  --year 2025 \
  --events-fred-release-ids "cpi=9,fomc=10" \
  --fred-start 2025-01-01 --fred-end 2025-12-31 \
  --pre-hours 24 --post-hours 24 \
  --output-csv data/impacts_2025.csv
```

### Notes
- Events are a built-in 2024/2025 calendar for CPI (8:30am ET), FOMC (2:00pm ET statements), and a small earnings sample. Extend by adding your own events via the flags above.
- For intraday studies, stick to `1h` interval to avoid API throttling; `1d` works for longer windows.
- Reaction minutes are relative to the event timestamp, using the first time the absolute cumulative return peaks after the announcement.

### Disclaimer
This project in any shape or form is purely educational and should not be treated as financial advice.
