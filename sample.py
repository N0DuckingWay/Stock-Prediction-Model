# -*- coding: utf-8 -*-
"""
Created on Sun Nov  2 22:52:56 2025

@author: paperspace
"""

import pandas as pd 
indata = pd.read_pickle('Data/financials.p')

sampsize = 300000

tickers = len(set(indata.index.get_level_values(0)))
daysperticker = max(100,round(sampsize/tickers)) #max of 100 or the average day per ticker.

tickers_by_daysofdata = indata.groupby('ticker').count()['price'].sort_values(ascending=False)

tickers_lt_daysper = tickers_by_daysofdata.loc[tickers_by_daysofdata<daysperticker]
tickers_gte_daysper = tickers_by_daysofdata.loc[tickers_by_daysofdata>=daysperticker]

tickers_lt = indata.loc[list(tickers_lt_daysper.index)]
tickers_gt = indata.loc[list(tickers_gte_daysper.index)]


sampled_init = tickers_gt.groupby('ticker',group_keys=False).apply(lambda x: x.sample(n=daysperticker))

sampleddata = pd.concat([sampled_init,tickers_lt],axis=0)

sampleddata.to_pickle('Data/sampled.p')