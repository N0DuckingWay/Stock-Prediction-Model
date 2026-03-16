# -*- coding: utf-8 -*-
"""
Fetches Alpha Vantage NEWS_SENTIMENT for each (ticker, date) in Data/sampled.p,
adds news_sentiment and ticker_sentiment columns, and saves to Data/sampled_withnews.p.
"""

import time
import random
from typing import Dict, Any, Optional, Tuple

import pandas as pd
import requests
import json as js


def _av_datetime_str(d: pd.Timestamp, end_of_day: bool = False) -> str:
    """
    Alpha Vantage NEWS_SENTIMENT expects timestamps of the form YYYYMMDDTHHMM.
    Since `d` is guaranteed to be a date (no time component), we use:
      - 00:00 for start of day
      - 23:59 for end of day
    """
    d = pd.Timestamp(d)
    dt = d.replace(hour=23, minute=59) if end_of_day else d.replace(hour=0, minute=0)
    return dt.strftime("%Y%m%dT%H%M")


def _request_with_retry(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    max_retries: int = 6,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Make a GET request with retries for transient errors and Alpha Vantage throttling.
    """
    backoff = 1.5
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {resp.status_code}")

            payload = resp.json()

            # Alpha Vantage may return 200 with a throttling message
            if isinstance(payload, dict) and ("Note" in payload or "Information" in payload):
                raise RuntimeError(payload.get("Note") or payload.get("Information"))

            return payload

        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep((backoff ** attempt) + random.uniform(0, 0.25))


def _extract_sentiments_from_feed(
    payload: Dict[str, Any],
    ticker: str,
) -> Tuple[Optional[float], Optional[float]]:
    """
    From a NEWS_SENTIMENT payload, extract:
      - news_sentiment: simple average of overall_sentiment_score across all articles
      - ticker_sentiment: relevance-weighted average of ticker_sentiment_score for `ticker`

    Returns:
      (news_sentiment or None, ticker_sentiment or None)
    """
    feed = payload.get("feed", [])
    if not isinstance(feed, list) or not feed:
        return None, None

    ticker = ticker.upper()

    # News sentiment: simple mean of overall_sentiment_score across all articles
    news_scores = []
    for item in feed:
        try:
            score = float(item.get("overall_sentiment_score"))
            news_scores.append(score)
        except (TypeError, ValueError):
            continue

    news_sentiment = sum(news_scores) / len(news_scores) if news_scores else None

    # Ticker sentiment: relevance-weighted average of ticker_sentiment_score for this ticker
    weighted_sum = 0.0
    weight_sum = 0.0

    for item in feed:
        ts_list = item.get("ticker_sentiment", [])
        if not isinstance(ts_list, list):
            continue

        for ts in ts_list:
            if str(ts.get("ticker", "")).upper() != ticker:
                continue

            try:
                score = float(ts.get("ticker_sentiment_score"))
            except (TypeError, ValueError):
                continue

            try:
                rel = float(ts.get("relevance_score", 1.0))
            except (TypeError, ValueError):
                rel = 1.0

            weighted_sum += score * rel
            weight_sum += rel

    ticker_sentiment = (weighted_sum / weight_sum) if weight_sum > 0 else None

    return news_sentiment, ticker_sentiment


def add_alpha_vantage_sentiment(
    data: pd.DataFrame,
    apikey: Dict[str, str],
    *,
    requests_per_minute: int = 70,
) -> pd.DataFrame:
    """
    Fetch Alpha Vantage NEWS_SENTIMENT for every unique (ticker, date) in data.index.

    Assumptions:
      - data.index is a 2-level MultiIndex: (ticker, date)
      - date values have no time component
      - Alpha Vantage API key is stored at apikey["stockkey"]

    Adds columns:
      - news_sentiment (float, NaN if unavailable)
      - ticker_sentiment (float, NaN if unavailable)
    """
    key = apikey.get("stockkey")
    if not key:
        raise ValueError('Missing Alpha Vantage key: apikey["stockkey"]')

    idx = data.index
    if not isinstance(idx, pd.MultiIndex) or idx.nlevels != 2:
        raise ValueError("data.index must be a 2-level MultiIndex: (ticker, date)")

    rpm = max(1, int(requests_per_minute))
    min_interval = 60.0 / rpm

    session = requests.Session()
    base_url = "https://www.alphavantage.co/query"

    # Deduplicate to make exactly one API call per unique (ticker, date) pair
    unique_pairs = idx.drop_duplicates()

    # results keyed by (ticker_upper, pd.Timestamp) -> (news_sentiment, ticker_sentiment)
    results: Dict[Tuple[str, pd.Timestamp], Tuple[Optional[float], Optional[float]]] = {}

    last_call_t = 0.0

    for ticker, d in unique_pairs:
        ticker = str(ticker).upper().strip()
        d = pd.Timestamp(d)

        # Rate limiting
        now = time.time()
        elapsed = now - last_call_t
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "time_from": _av_datetime_str(d, end_of_day=False),
            "time_to": _av_datetime_str(d, end_of_day=True),
            "limit": 1000,
            "apikey": key,
        }

        try:
            payload = _request_with_retry(session, base_url, params)
            news_sent, tick_sent = _extract_sentiments_from_feed(payload, ticker)
        except Exception:
            news_sent, tick_sent = None, None

        results[(ticker, d)] = (news_sent, tick_sent)
        last_call_t = time.time()

    # Left-join results back onto original data index (preserves row count and order)
    index_tuples = [(str(t).upper().strip(), pd.Timestamp(d)) for t, d in idx]

    news_vals = []
    tick_vals = []
    for t, d in index_tuples:
        pair = results.get((t, d), (None, None))
        news_vals.append(float('nan') if pair[0] is None else pair[0])
        tick_vals.append(float('nan') if pair[1] is None else pair[1])

    out = data.copy()
    out["news_sentiment"] = news_vals
    out["ticker_sentiment"] = tick_vals
    return out


if __name__ == "__main__":
    with open('apikeys.json', 'rb+') as f:
        apikey = js.load(f)

    data = pd.read_pickle('Data/sampled.p')

    result = add_alpha_vantage_sentiment(data, apikey, requests_per_minute=70)

    result.to_pickle('Data/sampled_withnews.p')
    print(f"Saved {len(result)} rows to Data/sampled_withnews.p")
