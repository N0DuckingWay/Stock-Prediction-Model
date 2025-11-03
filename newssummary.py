# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 02:48:25 2025

@author: paperspace
"""

import pandas as pd, openai as ai, json as js, os
from openai import OpenAI, APIConnectionError
from io import StringIO

querylen = 100



with open('apikeys.json','rb+') as file:
    
    apikey = js.load(file)
    os.environ['OPENAI_API_KEY'] = apikey['OPENAI_API_KEY']
    
client = OpenAI(timeout=600)

data = pd.read_pickle('Data/sampled.p')
tickers = list(set(data.index.get_level_values(0)))

sentiment = pd.DataFrame()
for h in range(len(tickers)):
    t = tickers[h]
    tickerdata = data.loc[t].dropna(subset='price').copy()
    dates = list(tickerdata.index)
    tickerdata = pd.DataFrame()
    outputs = []
    
    tries = 0
    success = 0
    maxtries = 5
    while tries < maxtries and success == 0: 
        try:
            response = client.responses.create(model="gpt-5",
            input=f'''
            Use web search to summarize the news for ticker symbol {t} on every one of the following dates: {dates}. For each date, rate the news on that date, as well as in the 7 days leading up to
            that date, 31 days leading up to that date, 90 days leading up to that date, and 365 days leading up to that date.
            Respond with only the ticker and a rating of the news on a 0-10 scale, with 0 being most negative, 5 being neutral, and 10 being most positive.
            Days with no news should have a null value. Results should be formatted as a comma separated table with the date as the index,
            and "rating_onday","rating_lastsevendays",rating_last31days", "rating_last90days", and "rating_last365days" as the columns.
            "rating_onday" is the rating of the news on that day only. "rating_lastsevendays" is the rating of the news over the last seven days.
            "rating_last31days" is the rating over the last 31 days, "rating_last90days" is the rating over the last 90days.
            and "rating_last365days" is the rating over the last 365 days. Do not ask clarifying questions, and do not cite sources.''',
            tools=[{'type':'web_search'}])
            outtext = response.output_text
            csv_text = StringIO(outtext)
            csv = pd.read_csv(csv_text)
            if 'date' not in csv.columns or 'rating_onday' not in csv.columns or "rating_lastsevendays" not in csv.columns or "rating_last31days" not in csv.columns or "rating_last90days" not in csv.columns or "rating_last365days" not in csv.columns:
                raise ValueError(f'error pulling data for {t}. Check outtext variable for details.')
            else:
                ratingcols = [x for x in csv if 'rating' in x]
                csv[ratingcols] = csv[ratingcols].astype(float)
            if len(list(set(csv.dtypes[ratingcols]))) != 1 and list(csv.dtypes[ratingcols])[0] != float:
                raise ValueError(f'error pulling data for {t}. Check outtext variable for details.')
            
            else:
                tickerdata = pd.concat([tickerdata,csv],axis=0)
                success = 1
        except Exception as e:
            if tries <maxtries-1:
                tries +=1
            else:
                raise Exception(e)
    print(f'finished pulling data for {t}. {round(100*(h+1)/len(tickers),2)}% finished.')
    sentiment = pd.concat([sentiment,tickerdata],axis=0)
    

sentiment['date'] = pd.to_datetime(sentiment.date)
outdata = pd.merge(data,sentiment,left_on=['ticker','date'],right_on=['ticker','date'],how='outer')

outdata.sort_index(ascending=True,inplace=True)



outdata['news_weekvsyear'] = outdata['rating_lastsevendays']/outdata['rating_last365days']
outdata['news_monthvsyear'] = outdata['rating_last31days']/outdata['rating_last365days']
outdata['news_qtrvsyear'] = outdata['rating_last90days']/outdata['rating_last365days']

outdata.set_index(['ticker','date'],inplace=True)



outdata.to_pickle('Data/financials_withsentiment.p')


