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

data = pd.read_pickle('Data/financials.p')
tickers = list(set(data.index.get_level_values(0)))

sentiment = pd.DataFrame()
for i in range(len(tickers)):
    t = tickers[i]
    tickerdata = data.loc[t].dropna(subset='price').copy()
    dates = list(tickerdata.index)
    tickerdata = pd.DataFrame()
    outputs = []
    for i in range(0,len(dates),querylen):
        start = dates[i]
        endindex = min(len(dates)-1,i+querylen-1)
        end = dates[endindex]
        tries = 0
        success = 0
        maxtries = 5
        while tries < maxtries and success == 0: 
            try:
                response = client.responses.create(model="gpt-5",
                input=f'''
                Use web search to summarize the the news for the ticker symbol {t} for every calendar day between {start} and {end}, including {start} and {end}.
                Respond with only the ticker and a rating of the news on a 0-10 scale, with 0 being most negative, 5 being neutral, and 10 being most positive. Days with no news should have a null value.
                Results should be formatted as a comma separated table with the ticker and date as the index, and rating as the column. Do not ask clarifying questions, and do not cite sources.''',
                tools=[{'type':'web_search'}])
                outtext = response.output_text
                csv_text = StringIO(outtext)
                csv = pd.read_csv(csv_text)
                if 'ticker' not in csv.columns or 'date' not in csv.columns or 'rating' not in csv.columns:
                    raise ValueError(f'error pulling data for {t} between {start} and {end}. Check outtext variable for details.')
                else:
                    csv['rating'] = csv['rating'].astype(float)
                if csv.dtypes['rating'] != float:
                    raise ValueError(f'error pulling data for {t} between {start} and {end}. Check outtext variable for details.')
                
                else:
                    tickerdata = pd.concat([tickerdata,csv],axis=0)
                    success = 1
            except Exception as e:
                if tries <maxtries-1:
                    tries +=1
                else:
                    raise Exception(e)
        
    print(f'finished pulling data for {t}. {round(100*(i+1)/len(tickers),2)}% finished.')
    sentiment = pd.concat([sentiment,tickerdata],axis=0)
    

sentiment['date'] = pd.to_datetime(sentiment.date)
outdata = pd.merge(data,sentiment,left_on=['ticker','date'],right_on=['ticker','date'],how='outer')

outdata.sort_index(ascending=True,inplace=True)


outdata['lastweek_avgnews'] = outdata.groupby('ticker')['rating'].rolling(7).mean()
outdata['lastweek_bestnews'] = outdata.groupby('ticker')['rating'].rolling(7).max()
outdata['lastweek_worstnews'] = outdata.groupby('ticker')['rating'].rolling(7).min()


outdata['lastmonth_avgnews'] = outdata.groupby('ticker')['rating'].rolling(30).mean()
outdata['lastmonth_bestnews'] = outdata.groupby('ticker')['rating'].rolling(30).max()
outdata['lastmonth_worstnews'] = outdata.groupby('ticker')['rating'].rolling(30).min()

outdata['lastqtr_avgnews'] = outdata.groupby('ticker')['rating'].rolling(90).mean()
outdata['lastqtr_bestnews'] = outdata.groupby('ticker')['rating'].rolling(90).max()
outdata['lastqtr_worstnews'] = outdata.groupby('ticker')['rating'].rolling(90).min()

outdata['lastyear_avgnews'] = outdata.groupby('ticker')['rating'].rolling(365).mean()
outdata['lastyear_bestnews'] = outdata.groupby('ticker')['rating'].rolling(365).max()
outdata['lastyear_worstnews'] = outdata.groupby('ticker')['rating'].rolling(365).min()

outdata['news_weekvsyear'] = outdata['lastweek_avgnews']/outdata['lastyear_avgnews']
outdata['news_monthvsyear'] = outdata['lastmonth_avgnews']/outdata['lastyear_avgnews']
outdata['news_qtrvsyear'] = outdata['lastqtr_avgnews']/outdata['lastyear_avgnews']

outdata.set_index(['ticker','date'],inplace=True)



outdata.to_pickle('Data/financials_withsentiment.p')


