#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  4 14:35:32 2025

@author: zdhoffman
"""

from get_cik_from_ticker import get_cik_for_ticker
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


with open('apikeys.json','rb+') as file:
    
    apikeys = js.load(file)
    stockkey = apikeys['stockkey']
    blskey = apikeys['blskey']
    fredkey = apikeys['fredkey']
    

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
    Utility function. Tracks down the sequence of keys in a dictionary that leads to a known value

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
    '''
    Cleans data for use in getfinancials. Also can compute Q4 values that are usually reported as FY values.


    '''
  
    
    if newcol == None:
        newcol = key
        
   
        
    df = pd.DataFrame(data[key]['units'][unit]).rename(columns={'val':newcol}).set_index('end')
    df = df.loc[df.form==form]
    if len(df) > 0 and form == '10-K':
        if 'start' in df.columns and 'frame' in df.columns:
            df = df.loc[((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days >= -380) & ((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days <= -360) & ~(df.frame.astype(str).str.contains('Q'))]
        elif 'start' in df.columns and 'frame' not in df.columns:
            df = df.loc[((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days >= -380) & ((pd.to_datetime(df.start)-pd.to_datetime(df.index)).dt.days <= -360) & (df.fp == 'FY')]
        elif 'frame' in df.columns:
            df = df.loc[~(df.frame.astype(str).str.contains('Q'))]
        
        
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
    global response, stockrequest, companyrequest, ecsso, out, response_init
    
    companyrequest = requests.get(f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={stockkey}')
    companyjs = companyrequest.json()
    
    if companyrequest.status_code == 200:
        if 'AssetType' in companyjs.keys() and companyjs['AssetType'] != 'Common Stock':
            raise Exception(f'{ticker} is not common stock')
        
        if 'Sector' in companyjs.keys():
            sector = companyjs['Sector']
        else:
            sector = np.nan
        if 'Industry' in companyjs.keys():
            industry = companyjs['Industry']
        else:
            industry = np.nan
    else:
        raise Exception(f'Request failed. Details: {companyrequest}')
    
    cik = get_cik_for_ticker(ticker)
    response_init = http.request("GET",f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json').data
    if 'the specified key does not exist' in str(response_init).lower():
        raise Exception(f'{ticker} does not exist in Edgar. Likely is not common stock')
    response = json.loads(response_init)
    
    
   
    
    if len(response['facts']) > 0:
            
        if 'us-gaap' in response['facts'].keys():
            financials = response['facts']['us-gaap']
        else:
            financials = response['facts']['ifrs-full']
        out = pd.DataFrame()
        currencies = []
        if 'dei' in response['facts']:
            dei = response['facts']['dei']
            if 'EntityCommonStockSharesOutstanding' in dei.keys():
                ecsso = pd.DataFrame(response['facts']['dei']['EntityCommonStockSharesOutstanding']['units']['shares'])
                ecsso = ecsso.sort_values(by=['end','filed'],ascending=True)
                ecsso = ecsso.groupby('end').last()
                ecsso = ecsso[['val']].rename(columns={'val':'EntityCommonStockSharesOutstanding'}).drop_duplicates()
                out = pd.concat([out,ecsso],axis=1)
            else:
                ecsso = pd.DataFrame()
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
                  'Assets','AssetsCurrent','AssetsNoncurrent','5. adjusted close', '7. dividend amount','CommonStockSharesOutstanding','EntityCommonStockSharesOutstanding',
                  'sector','industry','naics_code','sic_code','sic_desc','date','currency']
            currencies = []
            for key in keep:
                if key in financials.keys():
                    

                
                    if 'USD' in financials[key]['units'].keys():
                        currency = 'USD'
                    elif 'GBP' in financials[key]['units'].keys():
                        currency = 'GBP'
                    elif 'EUR' in financials[key]['units'].keys():
                        currency = 'EUR'
                    elif 'CAD' in financials[key]['units'].keys():
                        currency = 'CAD'
                    elif 'JPY' in financials[key]['units'].keys():
                        currency = 'JPY'
                    elif 'HKD' in financials[key]['units'].keys():
                        currency = 'HKD'
                    elif 'RMB' in financials[key]['units'].keys():
                        currency = 'RMB'
                    else:
                        clist = [x for x in financials[key]['units'] if 'shares' not in x]
                        if len(clist) > 0:
                            currency = clist[0]
                        else:
                            currency = None
                    currencies.append(currency)
                    if currency != None:
                        # print(f'"USD" in {key}')
                        if len([x for x in ['cash','accounts','asset','liab','debt','borrowing','accrued','accumulated','paidin'] if x.lower() in key.lower() and 'usedin' not in key.lower()]):
                            sheet = 'Balance'
                        elif ('cash' in key.lower() and 'usedin' in key.lower()) or ('depreciation' in key.lower() and 'accumulated' not in key.lower()):
                            sheet = 'CashFlow'
                        else:
                            sheet = 'Income'
                        data = dataclean(financials,key,unit=currency, sheet=sheet)
                        
                        out = pd.concat([out,data],axis=1)
                        
                    elif key in ['CommonStockSharesOutstanding','CommonStockSharesIssued','PreferredStockSharesIssued',
                    'PreferredStockSharesOutstanding']:
                        
                        data = dataclean(financials,key,unit='shares',sheet='Shares')
                        out = pd.concat([out,data],axis=1)
                    
                        
                    
                    
                    # if 'LineOfCreditFacilityCurrentBorrowingCapacity' in out.columns and saved == 0:
                    #     out_init = out_save.copy()
                    #     data_init = data.copy()
                    #     saved = 1
            currencies = pd.Series(currencies)
            out['ticker'] = ticker
            out['date'] = out.index
            if len(currencies) > 0:
                out['currency'] = currencies.value_counts().sort_values(ascending=False).index[0]
    
            
            #getting stock data
            sicrequest = http.request("GET",f'https://data.sec.gov/submissions/CIK{cik}.json')
            stockrequest = requests.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&outputsize=full&apikey={stockkey}')
            
            
            
            
            if stockrequest.status_code == 200:
                stockdata = pd.DataFrame(stockrequest.json()['Time Series (Daily)']).T.sort_index(ascending=True)
            else:
                raise Exception(f'Request failed. Details: {stockrequest}')
                
            
                
            if sicrequest.status == 200:
                js = sicrequest.json()
                siccode = js['sic']
                sic_desc = js['sicDescription']
            else:
                raise Exception(f'Request failed. Details: {sicrequest}')
            
            
            
            
            out.sort_values(by='date',ascending=True,inplace=True)
            out['date'] = pd.to_datetime(out['date'])
            stockdata.index = pd.to_datetime(stockdata.index)
            
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
            out_final = out_final.loc[(out_final.index.get_level_values(1) >= mindate) & (out_final.index.get_level_values(1).dayofweek <= 4)]
            
            
        except KeyError as e:
            raise KeyError(f'{e}. Ticker symbol: {ticker}. Key: {key}')
        
        
        
        dropcols = [x for x in out_final.columns if x not in keep]
        out_final_dropped = out_final.drop(columns=dropcols)
        
        out_final_dropped = out_final_dropped.dropna(axis=1,how='all')
        return out_final_dropped


#%%


tickerlist = pd.read_excel('tickers.xlsx',header=1)['Ticker']
nopreferreds = tickerlist.loc[~tickerlist.astype(str).str.contains("\.|\^|\-|p",case=True)]
tickers = list(nopreferreds)

errors = {}
collect = []
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
        collect.append(getfinancials(ticker))
        
    except Exception as e:
        errors[ticker] = e

    

print('Done gathering data, concatenating now')
allfinancials = pd.concat(collect,axis=0)

allfinancials['date_sort'] = pd.to_datetime([x[1] for x in allfinancials.index])
allfinancials.sort_values(by='date_sort',inplace=True,ascending=True)



print('finished getting data for each ticker')

errors = pd.Series(errors).astype(str)
errors.loc[errors.str.contains('no cik',case=False)] = 'No CIK'
errors.loc[errors.str.contains('not common stock',case=False)] = 'Not common stock'
errors.loc[errors.str.contains('Time Series',case=False)] = 'No share price data'

finderror = lambda x: errors.loc[errors.str.contains(x,case=False)]

error_counts = errors.value_counts()


print(f'\nWARNING: {len(errors)} errors detected!')
allfinancials['currency'] = allfinancials.groupby(level=0).currency.ffill().bfill()

allfinancials.to_pickle('Data/allfinancials.p')


