# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 02:48:25 2025

@author: paperspace
"""

import pandas as pd, openai as ai, json as js, os
from openai import OpenAI, APIConnectionError
from io import StringIO

querylen = 50



with open('apikeys.json','rb+') as file:
    
    apikey = js.load(file)
    os.environ['OPENAI_API_KEY'] = apikey['OPENAI_API_KEY']
    
client = OpenAI(timeout=600)

data = pd.read_pickle('Data/financials.p')
tickers = set(data.index.get_level_values(0))

sentiment = pd.DataFrame()
for t in tickers:
    tickerdata = data.loc[t].copy()
    dates = list(tickerdata.index)
    tickerdata = pd.DataFrame()
    for i in range(0,len(dates),querylen):
        start = dates[i]
        end = dates[i+querylen-1]
        
        response = client.responses.create(model="gpt-5",
        input=f'''
        Summarize the the news for the ticker symbol {t} for every calendar day between {start} and {end}, including {start} and {end}.
        Respond with only the ticker and a rating of the news on a 0-10 scale, with 0 being most negative, 5 being neutral, and 10 being most positive. Days with no news should have a null value.
        Results should be formatted as a .csv table with the ticker and date as the index, and rating as the column. Do not ask clarifying questions, and do not cite sources.''',
        tools=[{'type':'web_search'}])
        csv_text = response.output_text
        tickerdata = pd.concat([tickerdata,pd.read_csv(csv_text)])
        
        print(f'finished {i+querylen} rows of data for {t}. Length of tickerdata = {len(tickerdata)}')
    break
    




