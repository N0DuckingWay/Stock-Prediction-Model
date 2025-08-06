#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  6 01:55:11 2025

@author: zdhoffman
"""
import requests, urllib3 as url
from bs4 import BeautifulSoup


user_agent = {'user-agent': 'Zachary Hoffman (zdhoffman@gmail.com)',
                  "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"}
    


def load_sec_ticker_mapping():
    """
    Fetches and returns a dict mapping uppercase ticker → zero‐padded 10‐digit CIK.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    resp = requests.get(url,headers=user_agent)
    resp.raise_for_status()
    data = resp.json()
    return {
        item["ticker"].upper(): str(item["cik_str"]).zfill(10)
        for item in data.values()
    }

# load once at module import
TICKER2CIK = load_sec_ticker_mapping()



def query_sec_edgar_for_cik(ticker: str) -> str:
    """
    Queries the SEC EDGAR company‐search API for a ticker, and returns the CIK.
    Raises ValueError if not found.
    """
    # EDGAR supports an Atom‐feed‐style XML output
    params = {
        "CIK": ticker,
        "owner": "exclude",
        "action": "getcompany",
        "output": "atom",
    }
    

    
    resp = requests.get(f'https://www.sec.gov/cgi-bin/browse-edgar',params=params,headers=user_agent)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    cik_tag = soup.find("cik")
    if not cik_tag or not cik_tag.text.strip():
        raise ValueError(f"No CIK found for ticker {ticker!r}")
    # strip leading zeros, then re‐pad to 10 digits
    num = int(cik_tag.text)
    return f"{num:010d}"

def get_cik_for_ticker(ticker: str) -> str:
    """
    Return the 10‐digit SEC CIK for any ticker, active or legacy.
    """
    t = ticker.upper().strip()
    if t in TICKER2CIK:
        return TICKER2CIK[t]
    # fallback to EDGAR search for older/delisted tickers
    return query_sec_edgar_for_cik(t)


# Examples
if __name__ == "__main__":
    for sym in ["ETRN"]:
        try:
            print(f"{sym} → CIK {get_cik_for_ticker(sym)}")
        except Exception as e:
            print(f"{sym} → ERROR: {e}")