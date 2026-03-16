# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 02:48:25 2025

@author: paperspace
"""


import time
import math
import random
from typing import Dict, Any, Optional, Tuple

import pandas as pd
import requests
import os
import json as js

querylen = 100



with open('apikeys.json','rb+') as file:
    
    apikey = js.load(file)
    os.environ['stockkey'] = apikey['stockkey']
    
data = pd.read_pickle('Data/sampled.p')


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


def _extract_ticker_sentiment_from_feed(
    payload: Dict[str, Any],
    ticker: str,
) -> Tuple[Optional[float], int]:
    """
    From a NEWS_SENTIMENT payload, compute a relevance-weighted average sentiment
    for `ticker` across all returned articles.

    Returns:
      (sentiment_score or None, number_of_articles_used)
    """
    feed = payload.get("feed", [])
    if not isinstance(feed, list) or not feed:
        return None, 0

    ticker = ticker.upper()
    weighted_sum = 0.0
    weight_sum = 0.0
    used = 0

    for item in feed:
        ts_list = item.get("ticker_sentiment", [])
        if not isinstance(ts_list, list):
            continue

        for ts in ts_list:
            if str(ts.get("ticker", "")).upper() != ticker:
                continue

            try:
                score = float(ts.get("ticker_sentiment_score"))
            except Exception:
                continue

            try:
                rel = float(ts.get("relevance_score", 1.0))
            except Exception:
                rel = 1.0

            weighted_sum += score * rel
            weight_sum += rel
            used += 1

    if used == 0 or weight_sum == 0:
        return None, 0

    return (weighted_sum / weight_sum), used


def add_alpha_vantage_sentiment(
    data: pd.DataFrame,
    apikey: Dict[str, str],
    *,
    requests_per_minute: int = 70,
) -> pd.DataFrame:
    """
    Fetch Alpha Vantage NEWS_SENTIMENT for every (ticker, date) in data.index.

    Assumptions:
      - data.index is a 2-level MultiIndex: (ticker, date)
      - no duplicate index entries
      - date values have no time component
      - Alpha Vantage API key is stored at apikey["stockkey"]

    Adds columns:
      - av_sentiment_score (float, NaN if unavailable)
      - av_sentiment_articles (int)
      - av_sentiment_ok (bool)
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

    scores = []
    article_counts = []
    oks = []

    last_call_t = 0.0

    for ticker, d in idx:
        ticker = str(ticker).upper().strip()
        d = pd.Timestamp(d)  # guaranteed date-only

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

        payload = _request_with_retry(session, base_url, params)
        score, used = _extract_ticker_sentiment_from_feed(payload, ticker)

        last_call_t = time.time()

        scores.append(score if score is not None else math.nan)
        article_counts.append(int(used))
        oks.append(score is not None)

    out = data.copy()
    out["av_sentiment_score"] = scores
    out["av_sentiment_articles"] = article_counts
    out["av_sentiment_ok"] = oks
    return out


# Example usage:
# data = add_alpha_vantage_sentiment(data, apikey, requests_per_minute=70)
