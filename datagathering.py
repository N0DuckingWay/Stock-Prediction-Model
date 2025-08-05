#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  4 14:35:32 2025

@author: zdhoffman
"""

from sec_cik_mapper import StockMapper
import pandas as pd, urllib3 as url, json, sys, requests, warnings, numpy as np, datetime

# To ignore all warnings
warnings.filterwarnings("ignore")


sic_to_naics = pd.read_csv('sic_to_naics.csv')
def getnaics(sic):
    naicsdf = sic_to_naics.loc[sic_to_naics['SIC'] == int(sic)]['NAICS']
    if len(naicsdf) > 0:
        return naicsdf.values[0]
    else:
        return float('nan')

stockkey = 'GJT87YF8QI5GZUND'
blskey = '6eebf73b51c642fe8914b9b72cc73569'
fredkey = '84a26b17b51a63ed7dae3c7936a19d02'


sys.setrecursionlimit(10000000)

user_agent = {'user-agent': 'Zachary Hoffman (zdhoffman@gmail.com)'}
http = url.PoolManager(10,headers=user_agent)

#%% functions
def getcolnames(hint,data,missper = False):
    out = [x for x in data.columns if hint.lower() in x.lower()]
    out.sort()
    if missper == True:
        return data[out].isnull().mean().sort_values()
    return out
    

def gettotals(cols,data,maxvals=True,ticker=None):
    if maxvals == True:
        data = data[cols].max(axis=1)
    else:
        data = data[cols]
    if ticker == None:
        return data.sort_index(ascending=True)
    else:
        return data.loc[ticker].sort_index(ascending=True)
    
getvar = lambda testdata, test : [x for x in testdata.keys() if test.lower() in x.lower()]

def trackdown(dictionary, value,hint = None, outlist = [],recur=0):
    '''
    Tracks down the sequence of keys in a dictionary that leads to a known value

    Parameters
    ----------
    dictionary : DICT
        the dictionary you're trying to find a value in
    value : STR,INT, or FLOAT
        Value you're trying to track down
    outlist : list
        output list. always keep empty for initial call.

    Returns
    -------
    a list of keys

    '''
    
    for key in dictionary.keys():
        
        
        if dictionary[key] == value and (hint == None or str(hint).lower() in outlist[0].lower()):
            print('value found!')
            return outlist+[True]
        elif type(dictionary[key]) == dict:
            out = trackdown(dictionary[key],value,hint = hint,outlist=outlist+[key],recur=recur+1)
            if True in out:
                return out
            
            
        elif type(dictionary[key]) == list:
            for inval in dictionary[key]:
                if inval == value:
                    print('value found!')
                    return outlist+[True]
                elif type(inval) == dict:
                    out = trackdown(inval,value,hint = hint,outlist=outlist+[key],recur=recur+1)
                    if True in out:
                        return out
                    
    return [False]


def dataclean(data,key,unit='USD',newcol = None,form='10-Q',sheet='Income'):
    
    formcopy = form
    
    
    if newcol == None:
        newcol = key
        
    data_orig = data[key]['units'][unit].copy()
    
        
    df = pd.DataFrame(data[key]['units'][unit]).rename(columns={'val':newcol}).set_index('end')
    df = df.loc[df.form==form]
    df_noedit= df.copy()
    if len(df) > 0 and form == '10-K':
        if 'start' in df.columns and 'frame' in df.columns:
            df = df.loc[((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days >= -380) & ((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days <= -360) & ~(df.frame.astype(str).str.contains('Q'))]
        elif 'start' in df.columns and 'frame' not in df.columns:
            df = df.loc[((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days >= -380) & ((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days <= -360) & (df.fp == 'FY')]
        elif 'frame' in df.columns:
            df = df.loc[~(df.frame.astype(str).str.contains('Q'))]
        
    # if 'frame' in df.columns and len(df.dropna(subset='frame')) != 0:
    #     df = df.dropna(subset='frame')
        
    if form == '10-Q':
        if 'start' in df.columns and sheet == 'Income':
            quarter = df.loc[((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days >= -100) & ((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days <= -80)].copy()
        else:
            quarter = df.copy()
        year = dataclean(data,key,unit=unit,newcol=newcol,form='10-K',sheet=sheet).drop_duplicates(subset=key)
        year['year'] = pd.to_datetime(year.index).year
        year['yearend'] = year.index
        
        if sheet=='Income' and 'start' in df.columns: #balance sheet items don't have start dates as they are point in time
            nine = quarter.copy()
            nine.index = pd.to_datetime(nine.index)
            year.index = pd.to_datetime(year.index)
            
            nine = pd.merge_asof(nine,year['year'],left_index=True,right_index=True,tolerance=pd.to_timedelta('365D'),direction='forward')
            nine['year'] = pd.to_datetime(nine.index).year
            nine['end'] = nine.index
            nine = nine.drop_duplicates(subset=['start','end',newcol])
            ninenew = nine.groupby('year')[[newcol]].sum()
            ninenew.index = ninenew.index.astype(float)
            # ninenew_merged = pd.merge(ninenew,year[['year','yearend']],left_index=True,right_on = 'year')
            # ninenew_merged.index = ninenew_merged.year
            
            # ninenew_merged['date'] = ninenew_merged.yearend
                    
            # finalq = year.groupby('year').sum()[[newcol]]-ninenew_merged[[newcol]]
            finalq = year.groupby('year').sum()[[newcol]].fillna(0).sub(ninenew[[newcol]].fillna(0),fill_value=0)
            finalq_merged = pd.merge(finalq,year[['yearend','year']],left_index=True,right_on='year')
            finalq_merged.index = finalq_merged['yearend']
            
            
    
        
            
            
            
            out = pd.concat([quarter,finalq_merged])
        else:
            out = pd.concat([quarter,year])
            if sheet == 'CashFlow':
                out.sort_values(['start','end'],ascending=True,inplace=True)
                out['fy'] = pd.to_datetime(out['yearend'].bfill()).dt.year.fillna(2025)
                out = out.groupby(['start','end']).last()
                quarterly = out.groupby('fy')[[newcol]].diff()
                out = pd.concat([out.rename(columns={newcol:newcol+'_cum'}),quarterly],axis=1)
                out[newcol] = out[newcol].fillna(out[newcol+'_cum'])
        out = out.dropna(subset=newcol)
        if out.index.name == None:
            out.index.name = 'end'
        out.sort_values(by=['end','filed'],ascending=True,inplace=True)
        out = out.groupby('end').last()[[newcol]]
        return out
    else:
        return df
    
def merger(indata):
    
    out = pd.DataFrame()
    
    for x in indata:
        out = out.merge(x,left_index=True,right_index=True,how='outer')
    
    
    return out.drop_duplicates()
                


    
def getfinancials(ticker,maxdate = np.datetime64('today'),mindate=np.datetime64('1999-01-01')):
    '''
    Gets financials from the SEC EDGAR API when given the ticker symbol for a company

    Parameters
    ----------
    ticker : STR
        company ticker symbol.

    Returns
    -------
    Pandas dataframe containing all financial information.

    '''
    
    
    
    cik = mapper.ticker_to_cik[ticker]
    response = json.loads(http.request("GET",f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json').data)
    
    
   
    
        
        
    financials = response['facts']['us-gaap']
    out = pd.DataFrame()
    
    dei = response['facts']['dei']
    if 'EntityCommonStockSharesOutstanding' in dei.keys():
        ecsso = pd.DataFrame(response['facts']['dei']['EntityCommonStockSharesOutstanding']['units']['shares'])
        ecsso = ecsso.sort_values(by=['end','filed'],ascending=True)
        ecsso = ecsso.groupby('end').last()
        ecsso = ecsso[['val']].rename(columns={'val':'EntityCommonStockSharesOutstanding'}).drop_duplicates()
        out = pd.concat([out,ecsso],axis=1)
    else:
        ecsso = pd.DataFrame()
                
        
        
    try:
        # saved = 0
        keep=['LongTermDebt','LongTermDebtNoncurrent','LongTermDebtCurrent','DebtCurrent','LongTermDebtAndCapitalLeaseObligationsCurrent',
              'ShortTermBorrowings','OtherShortTermBorrowings','LTDebt','CurrLTDebt','STDebt','Revenues','RevenueFromContractWithCustomerIncludingAssessedTax','RevenueFromContractWithCustomerExcludingAssessedTax',
              'SalesRevenueNet','AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment','DepreciationDepletionAndAmortization','Depreciation','DepreciationAndAmortization',
              'PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization','InterestExpense','IncomeTaxExpenseBenefit',
              'NetIncomeLoss','ProfitLoss','IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
              'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
              'NetCashProvidedByUsedInOperatingActivities','NetCashProvidedByUsedInOperatingActivitiesContinuingOperations','NetCashProvidedByUsedInContinuingOperations',
              'Assets','AssetsCurrent','AssetsNoncurrent','4. close','CommonStockSharesOutstanding','EntityCommonStockSharesOutstanding',
              'sector','industry','naics_code','sic_code','sic_desc','date']
        for key in keep:
            if key in financials.keys():
            
                if 'USD' in financials[key]['units'].keys():
                    # print(f'"USD" in {key}')
                    if len([x for x in ['cash','accounts','asset','liab','debt','borrowing','accrued','accumulated','paidin'] if x.lower() in key.lower() and 'usedin' not in key.lower()]):
                        sheet = 'Balance'
                    elif ('cash' in key.lower() and 'usedin' in key.lower()) or ('depreciation' in key.lower() and 'accumulated' not in key.lower()):
                        sheet = 'CashFlow'
                    else:
                        sheet = 'Income'
                    data = dataclean(financials,key,sheet=sheet)
                    
                    out_save = out.copy()
                    out = pd.concat([out,data],axis=1)
                    
                elif key in ['CommonStockSharesOutstanding','CommonStockSharesIssued','PreferredStockSharesIssued',
                'PreferredStockSharesOutstanding']:
                    
                    data = dataclean(financials,key,unit='shares',sheet='Shares')
                    out_save = out.copy()
                    out = pd.concat([out,data],axis=1)
                
                    
                
                
                # if 'LineOfCreditFacilityCurrentBorrowingCapacity' in out.columns and saved == 0:
                #     out_init = out_save.copy()
                #     data_init = data.copy()
                #     saved = 1
            
        out['ticker'] = ticker
        out['date'] = out.index

        
        #getting stock data
        sicrequest = http.request("GET",f'https://data.sec.gov/submissions/CIK{cik}.json')
        stockrequest = requests.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&outputsize=full&apikey={stockkey}')
        companyrequest = requests.get(f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={stockkey}')
        
        
        
        if stockrequest.status_code == 200:
            stockdata = pd.DataFrame(stockrequest.json()['Time Series (Daily)']).T.sort_index(ascending=True)
        else:
            raise Exception(f'Request failed. Details: {stockrequest}')
            
        if companyrequest.status_code == 200:
            js = companyrequest.json()
            sector = js['Sector']
            industry = js['Industry']
        else:
            raise Exception(f'Request failed. Details: {companyrequest}')
            
        if sicrequest.status == 200:
            js = sicrequest.json()
            siccode = js['sic']
            sic_desc = js['sicDescription']
        else:
            raise Exception(f'Request failed. Details: {sicrequest}')
        
        
        
        
        out.sort_values(by='date',ascending=True,inplace=True)
        out['date'] = pd.to_datetime(out['date'])
        stockdata.index = pd.to_datetime(stockdata.index)
        
        outmerge = out.copy()
        tolerance = pd.to_timedelta('3D')
        out_final = pd.merge_asof(out,stockdata,left_on='date',right_index=True,tolerance=tolerance)
        
        
        out_final = out_final.set_index(['ticker','date']).sort_index(ascending=True)
        stockdata.index = [(ticker,x) for x in stockdata.index]
        out_final = pd.concat([out_final,stockdata]).drop_duplicates(subset=stockdata.columns).sort_index(ascending=True).ffill()
        out_final['sector'] = sector
        out_final['industry'] = industry
        out_final['sic_code'] = siccode
        out_final['sic_desc'] = sic_desc
        out_final['naics_code'] = getnaics(siccode)
        
        out_final = out_final.loc[out_final.index.get_level_values(1) <= maxdate]
        out_final = out_final.loc[out_final.index.get_level_values(1) >= mindate]
        
        
    except KeyError as e:
        raise KeyError(f'{e}. Ticker symbol: {ticker}. Key: {key}')
    
    
    
    dropcols = [x for x in out_final.columns if x not in keep]
    out_final_dropped = out_final.drop(columns=dropcols)
    
    out_final_dropped = out_final_dropped.dropna(axis=1,how='all')
    return out_final

mapper = StockMapper()
#%%


tickerlist = pd.read_excel('tickers.xlsx',header=1)['Ticker']

tickers = list(tickerlist)
# ciks = pd.Series(mapper.ticker_to_cik).loc[tickers]
errors = {}

allfinancials = pd.DataFrame()

print(f'Getting data for {len(tickers)} tickers. This may take a while.')
tickerlen = len(tickers)
start = datetime.datetime.now()
for i in range(tickerlen):
    ticker = tickers[i]
    if (i % 50 == 0 and i > 0) or (i == 1):
        now = datetime.datetime.now()
        diff = now-start
        pctdec = i/tickerlen
        end = start + diff/pctdec
        left = end - now
        hourspassed = int(diff.seconds/(60*60))
        minutespassed = int((diff.seconds/60)-hourspassed*60)
        
        hoursleft = int(left.seconds/(60*60))
        minutesleft = round((left.seconds/60)-hoursleft*60)
        print(f'Gathering financial data for {ticker}. Ticker {i} out of {tickerlen}. {round(100*pctdec,2)}% done gathering financial information.')
        print(f'{hourspassed} hrs, {minutespassed} minutes have passed. Estimated {hoursleft} hrs, {minutesleft} minutes until finished.')
        print(f'Estimated finish time: {end}\n\n')
    try:
        allfinancials = pd.concat([allfinancials,getfinancials(ticker)],axis=0)
        
    except Exception as e:
        errors[ticker] = e
        
    

allfinancials['date_sort'] = pd.to_datetime([x[1] for x in allfinancials.index])
allfinancials.sort_values(by='date_sort',inplace=True,ascending=True)



print('finished getting data for each ticker')

print(f'\nWARNING: {len(errors)} errors detected!')

allfinancials.to_pickle('Data/allfinancials.p')


