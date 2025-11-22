"""
Unified OHLCV Data Fetcher for Jamcakes v26
Supports:
- Yahoo Finance (yfinance)
- Polygon.io (requires POLYGON_API_KEY)
Includes local caching, provider fallback, and standard column normalization.
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from dateutil import parser as dateparser

# -------------------------------
# Config
# -------------------------------
_CACHE = {}
_TTL_SEC = 15 * 60  # 15 minutes
_POLYGON_URL = "https://api.polygon.io/v2/aggs/ticker/{}/range/{}/{}/{}/{}"


# -------------------------------
# Utilities
# -------------------------------
def _now():
    return int(time.time())


def _cache_key(**kw):
    return "|".join(f"{k}={v}" for k, v in sorted(kw.items()))


def latest_close_date(df: pd.DataFrame) -> pd.Timestamp:
    return pd.to_datetime(df.index[-1])


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure standardized columns and datetime index."""
    rename = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
        "adjclose": "Adj Close",
        "adj_close": "Adj Close",
    }
    df = df.rename(columns=lambda c: c.strip().title())
    df = df.rename(columns=rename)
    if "Adj Close" not in df.columns and "Close" in df.columns:
        df["Adj Close"] = df["Close"]
    df.index = pd.to_datetime(df.index)
    return df.dropna(subset=["Close"])


# -------------------------------
# Yahoo Provider
# -------------------------------
def _fetch_yahoo(ticker: str, start: str, end: str, interval: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if df.empty:
        raise ValueError(f"No Yahoo data for {ticker}")
    return _normalize_df(df)


# -------------------------------
# Polygon Provider
# -------------------------------
def _polygon_aggs(ticker: str, start: str, end: str, timespan: str, multiplier: int, api_key: str) -> pd.DataFrame:
    """Calls Polygon v2 aggregates."""
    url = _POLYGON_URL.format(ticker.upper(), multiplier, timespan, start, end)
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise ValueError(f"Polygon error {r.status_code}: {r.text}")
    js = r.json()
    results = js.get("results", [])
    if not results:
        raise ValueError(f"No Polygon data for {ticker}")

    df = pd.DataFrame(results)
    df["Date"] = pd.to_datetime(df["t"], unit="ms")
    df = df.set_index("Date").rename(
        columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"}
    )
    df["Adj Close"] = df["Close"]
    return df[["Open", "High", "Low", "Close", "Adj Close", "Volume"]].sort_index()


def _fetch_polygon(ticker: str, start: str, end: str, interval: str, api_key: str) -> pd.DataFrame:
    """Top-level Polygon fetcher (daily or intraday)."""
    if not api_key:
        raise ValueError("POLYGON_API_KEY not set.")
    def norm(d):
        if d is None:
            return None
        return dateparser.parse(d).date().isoformat()
    start = norm(start) or (datetime.utcnow().date() - timedelta(days=365 * 2)).isoformat()
    end = norm(end) or datetime.utcnow().date().isoformat()

    if interval.lower() in ("1m", "1min", "minute"):
        return _polygon_aggs(ticker, start, end, "minute", 1, api_key)
    return _polygon_aggs(ticker, start, end, "day", 1, api_key)


# -------------------------------
# Unified Fetch (with fallback + cache)
# -------------------------------
def fetch_ohlcv(
    ticker: str,
    start: str = None,
    end: str = None,
    interval: str = "1d",
    provider: str = "auto",
) -> pd.DataFrame:
    """
    Fetch OHLCV data for a ticker.
    provider: 'auto' (Polygon→Yahoo), 'polygon', or 'yahoo'
    Uses in-memory cache for 15 min.
    """
    if end is None:
        end = datetime.utcnow().date().isoformat()
    if start is None:
        start = (datetime.utcnow().date() - timedelta(days=365 * 2)).isoformat()

    provider = (provider or "auto").lower().strip()
    cache_key = _cache_key(ticker=ticker.upper(), start=start, end=end, interval=interval, provider=provider)
    if cache_key in _CACHE and _now() - _CACHE[cache_key]["ts"] < _TTL_SEC:
        return _CACHE[cache_key]["df"].copy()

    df = None
    if provider in ("polygon", "auto"):
        try:
            api_key = os.getenv("POLYGON_API_KEY", "")
            df = _fetch_polygon(ticker, start, end, interval, api_key)
        except Exception as e:
            if provider == "polygon":
                raise
            print(f"[WARN] Polygon failed ({e}); falling back to Yahoo.")
            df = None

    if df is None and provider in ("yahoo", "auto"):
        df = _fetch_yahoo(ticker, start, end, interval)

    df = _normalize_df(df)
    _CACHE[cache_key] = {"ts": _now(), "df": df.copy()}
    return df
