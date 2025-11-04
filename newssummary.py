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
completed = []
errortickers = {}
for h in range(len(tickers)):
    t = tickers[h]
    tickerdata = data.loc[t].dropna(subset='price').copy()
    dates = [str(x.date()) for x in tickerdata.index]
    tickerdata = pd.DataFrame()
    outputs = []
    
    
    for i in range(0,len(dates),20):
        subdates = dates[i:i+20]
        tries = 0
        success = 0
        maxtries = 5
        while tries < maxtries and success == 0: 
            try:
                response = client.responses.create(model="gpt-5-mini",
                input=f'''
                Use web search to summarize the news for ticker symbol {t} on every one of the following dates: {subdates}. For each date, rate the news in the 7 days leading up to
                that date, 31 days leading up to that date, 90 days leading up to that date, and 365 days leading up to that date.
                The news should be rated on a 0-10 scale, with 0 being most negative, 5 being neutral, and 10 being most positive.
                Days with no news should have a null value. Results should be formatted as a json with "date",
                "rating_lastsevendays",rating_last31days", "rating_last90days", "rating_last365days", and "sources" as properties.
                "date" should only contain the date, "rating_onday" should only contain the integer rating of the news on that date only. "rating_lastsevendays" should only contain the integer rating of the news over the seven days leading up to that date.
                "rating_last31days" should only contain the integer rating of the news over the 31 days leading up to that date, "rating_last90days" should only contain the integer rating of the news over the last 90 days leading up to that date.
                "rating_last365days" should only contain the integer rating of the news over the last 365 days leading up to that date. "sources" contains a list of sources, separated by semicolons (do not use commas).
                
                Do not ask follow up questions. Use only the information provided. Only return the JSON.''',
                tools=[{'type':'web_search'}])
                text={
                    'format':{
                        'type':'json_schema',
                        'name':'ratings',
                        'strict':True,
                        'schema':{
                            'type':'object',
                            'properties':{
                                'date': {
                                    'type':'string'},
                                'rating_onday':{
                                    'type': 'number',
                                    'minimum':0,
                                    'maximum':10},
                                'rating_lastsevendays':{
                                    'type': 'number',
                                    'minimum':0,
                                    'maximum':10},
                                'rating_last31days':{
                                    'type': 'number',
                                    'minimum':0,
                                    'maximum':10},
                                'rating_last90days':{
                                    'type': 'number',
                                    'minimum':0,
                                    'maximum':10},
                                'rating_last365days':{
                                    'type': 'number',
                                    'minimum':0,
                                    'maximum':10},
                                'sources':{
                                    'type':'string'},
                                'notes':{
                                    'type':'string'}
                                },
                            'required':[
                                'date','rating_onday','rating_lastsevendays','rating_last31days','rating_last90days','rating_last365days','sources','notes'],
                            'additionalproperties':False
                            }}}
                outjson = response.output_text
                
                
                resultdf = pd.read_json(outjson)
                if 'date' not in resultdf.columns or 'rating_onday' not in resultdf.columns or "rating_lastsevendays" not in resultdf.columns or "rating_last31days" not in resultdf.columns or "rating_last90days" not in resultdf.columns or "rating_last365days" not in resultdf.columns:
                    raise ValueError(f'error pulling data for {t}. Check outtext variable for details.')
                else:
                    ratingcols = [x for x in resultdf if 'rating' in x]
                    resultdf[ratingcols] = resultdf[ratingcols].astype(float)
                if len(list(set(resultdf.dtypes[ratingcols]))) != 1 and list(resultdf.dtypes[ratingcols])[0] != float:
                    raise ValueError(f'error pulling data for {t}. Check outtext variable for details.')
                
                else:
                    tickerdata = pd.concat([tickerdata,resultdf],axis=0)
                    completed.append(t)
                    print(f'Succesfully pulled data for rows {i} to {i+20} of {t}. Roughly {round(100*(h+1)/len(tickers),2)}% finished.')
                    success = 1
            except Exception as e:
                if tries <maxtries-1:
                    tries +=1
                else:
                    errortickers[t] = {'error':e,'response':outjson}
                    print(f'Failed to pull data for rows {i} to {i+20} of  {t}. Roughly {round(100*(h+1)/len(tickers),2)}% finished.')
                    tries += 1
            print(f'Completed {i} rows of {t}')
    
    tickerdata['ticker'] = t
    sentiment = pd.concat([sentiment,tickerdata],axis=0)
    

sentiment['date'] = pd.to_datetime(sentiment.date)
outdata = pd.merge(data,sentiment,left_on=['ticker','date'],right_on=['ticker','date'],how='outer')

outdata.sort_index(ascending=True,inplace=True)



outdata['news_weekvsyear'] = outdata['rating_lastsevendays']/outdata['rating_last365days']
outdata['news_monthvsyear'] = outdata['rating_last31days']/outdata['rating_last365days']
outdata['news_qtrvsyear'] = outdata['rating_last90days']/outdata['rating_last365days']

outdata.set_index(['ticker','date'],inplace=True)



outdata.to_pickle('Data/financials_withsentiment.p')


