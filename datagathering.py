#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  4 14:35:32 2025

@author: zdhoffman
"""

from sec_cik_mapper import StockMapper
import pandas as pd, urllib3 as url, json, sys, requests, warnings

# To ignore all warnings
warnings.filterwarnings("ignore")



stockkey = 'GJT87YF8QI5GZUND'


sys.setrecursionlimit(10000000)

user_agent = {'user-agent': 'Zachary Hoffman (zdhoffman@gmail.com)'}
http = url.PoolManager(10,headers=user_agent)

#%% functions

getvar = lambda test : [x for x in data.keys() if test.lower() in x.lower()]

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
    global year, df, quarter, data_orig, nine, finalq, ninenew, ninenew_merged,finalq_merged, df_noedit, ninenew, formcopy
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
                


    
def getfinancials(ticker):
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
    
    global companyrequest
    
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
        saved = 0
        for key in financials.keys():
            
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
            
                
            
            
            if 'LineOfCreditFacilityCurrentBorrowingCapacity' in out.columns and saved == 0:
                out_init = out_save.copy()
                data_init = data.copy()
                saved = 1
            
        out['ticker'] = ticker
        out['date'] = out.index

        
        #getting stock data
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
        
    except KeyError as e:
        raise KeyError(f'{e}. Ticker symbol: {ticker}. Key: {key}')
    
    out_final = out_final.dropna(axis=1,how='all')
    return out_final

mapper = StockMapper()
#%%



tickers = '''ADBE
AMD
GOOGL
GOOG
ADI
ANSS
AAPL
AMAT
APP
ARM
ASML
TEAM
ADSK
AVGO
CDNS
CDW
CHTR
CTSH
CRWD
DDOG
DOCU
FTNT
INTU
KLAC
LRCX
MCHP
MU
MSFT
MRVL
META
NFLX
NVDA
NXPI
ON
PANW
PYPL
QCOM
ROP
SHOP
SNPS
TSLA
TXN
TTD
WDAY
ZS
'''.split('\n')
tickers = [x for x in tickers if len(x) > 0 and x not in ['ARM','ASML']] #excluded for the moment as a foreign issuer

# ciks = pd.Series(mapper.ticker_to_cik).loc[tickers]


finout = [getfinancials(x) for x in tickers]
allfinancials = pd.DataFrame()
for x in finout:
    allfinancials = pd.concat([allfinancials,x],axis=0)
# allfinancials = pd.concat(finout,axis=0)


allfinancials['date_sort'] = pd.to_datetime([x[1] for x in allfinancials.index])
allfinancials.sort_values(by='date_sort',inplace=True,ascending=True)






#%%
def getcolnames(hint,data=allfinancials,missper = False):
    out = [x for x in data.columns if hint.lower() in x.lower()]
    out.sort()
    if missper == True:
        return data[out].isnull().mean().sort_values()
    return out
    

def gettotals(cols,maxvals=True,data=allfinancials,ticker=None):
    if maxvals == True:
        data = data[cols].max(axis=1)
    else:
        data = data[cols]
    if ticker == None:
        return data.sort_index(ascending=True)
    else:
        return data.loc[ticker].sort_index(ascending=True)

#%% creating summary values

def getdata(alldata,url,valname,period,reset_month = False,chg=False,growth=False):
    global moddata,newdata, outdata, chgdata, pctchgdata
    moddata = alldata.copy()
    moddata['date_sort'] = pd.to_datetime([x[1] for x in moddata.index])
    response = requests.get(url)
    if response.status_code == 200:
        newdata = pd.DataFrame(response.json()['data']).sort_values(by='date',ascending=True).set_index('date').rename(columns={'value':valname})
        newdata.index = pd.to_datetime(newdata.index)
        newdata = newdata.loc[newdata[valname] != '.']
        if reset_month == True:
            #resets the date to the month or quarter end
            if period == 'monthly':
                newdata.index = pd.to_datetime(newdata.index.year.astype(str)+'-'+newdata.index.month.astype(str)+'-'+newdata.index.days_in_month.astype(str))
            elif period == 'quarterly':
                #has to be repeated to reset days_in_month to the new month
                newdata.index = pd.to_datetime(newdata.index.year.astype(str)+'-'+(newdata.index.month+2).astype(str)+'-'+newdata.index.day.astype(str))
                newdata.index = pd.to_datetime(newdata.index.year.astype(str)+'-'+(newdata.index.month).astype(str)+'-'+newdata.index.days_in_month.astype(str))
                newdata[valname] = newdata[valname].astype(float)
            
    else:
        raise Exception(f'Request failed. Details: {response}')
    
    if period == 'daily':
        tolerance = pd.to_timedelta('3D')
    elif period == 'weekly':
        tolerance = pd.to_timedelta('10D')
    elif period == 'monthly':
        tolerance = pd.to_timedelta('35D')
    elif period == 'quarterly':
        tolerance = pd.to_timedelta('95D')
    else:
        raise ValueError('invalid value for period. Must be daily, weekly, monthly, or quarterly')
    if chg == True:
        newdata[valname+'_diff'] = newdata[valname].diff()
    if growth == True:    
        newdata[valname+'_pctdiff'] = newdata[valname].pct_change()
    outdata = pd.merge_asof(moddata,newdata,left_on='date_sort',right_index=True,tolerance = tolerance)
    
    outdata[valname] = outdata[valname].groupby('ticker').ffill()
    
    if chg == True:
        outdata[valname+'_diff'] = outdata[valname+'_diff'].groupby('ticker').ffill()
    if growth == True:    
        outdata[valname+'_pctdiff'] = outdata[valname+'_pctdiff'].groupby('ticker').ffill()
    
    
        
        
        
    return outdata.drop(columns='date_sort')


finalfinancials = pd.DataFrame(index=allfinancials.index)
finalfinancials.index.set_names(['ticker','date'],inplace=True)

finalfinancials['Cash'] = allfinancials.CashAndCashEquivalentsAtCarryingValue.fillna(allfinancials.Cash)

finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=TREASURY_YIELD&interval=daily&maturity=3mo&apikey={stockkey}','treasury_yield','daily',chg=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=WTI&interval=daily&apikey={stockkey}','wti_crude_price','daily',growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=ALL_COMMODITIES&interval=monthly&apikey={stockkey}','commodity_index','monthly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=REAL_GDP&interval=quarterly&apikey={stockkey}','real_gdp','quarterly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=FEDERAL_FUNDS_RATE&interval=daily&apikey={stockkey}','fed_funds_rate','daily',chg=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=CPI&interval=monthly&apikey={stockkey}','cpi','monthly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=RETAIL_SALES&apikey={stockkey}','retail_sales','monthly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=DURABLES&apikey={stockkey}','durable_goods_orders','monthly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=UNEMPLOYMENT&apikey={stockkey}','unemployment','monthly',reset_month=True,chg=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=NONFARM_PAYROLL&apikey={stockkey}','nonfarm_payroll','monthly',reset_month=True,growth=True)


finalfinancials['LTDebt'] = (allfinancials['LongTermDebt']).fillna(allfinancials['LongTermDebtNoncurrent'])
finalfinancials['CurrLTDebt']= allfinancials['LongTermDebtCurrent'].fillna(allfinancials['DebtCurrent']).fillna(allfinancials.LongTermDebtAndCapitalLeaseObligationsCurrent)
finalfinancials['STDebt'] = allfinancials['ShortTermBorrowings'].fillna(allfinancials.OtherShortTermBorrowings)

finalfinancials['Debt'] = finalfinancials[['LTDebt','CurrLTDebt','STDebt']].sum(axis=1)

finalfinancials['Revenue'] = allfinancials.Revenues.fillna(allfinancials.RevenueFromContractWithCustomerIncludingAssessedTax).fillna(allfinancials.RevenueFromContractWithCustomerExcludingAssessedTax).fillna(allfinancials.SalesRevenueNet)





finalfinancials['D&A'] = allfinancials.AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment.fillna(allfinancials.DepreciationDepletionAndAmortization).fillna(allfinancials.Depreciation).fillna(allfinancials.DepreciationAndAmortization).fillna(allfinancials.PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization)


finalfinancials['Interest'] = allfinancials.InterestExpense.fillna(allfinancials.InterestExpense.fillna(0))

finalfinancials['Tax'] = allfinancials.IncomeTaxExpenseBenefit

finalfinancials['NI'] = allfinancials.NetIncomeLoss.fillna(allfinancials.ProfitLoss)
finalfinancials['EBIT'] = allfinancials.IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest.fillna(allfinancials.IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments).fillna(finalfinancials.NI + finalfinancials.Interest+finalfinancials.Tax)
finalfinancials['EBITDA'] = finalfinancials.EBIT + finalfinancials['D&A']
finalfinancials['CFO'] = allfinancials.NetCashProvidedByUsedInOperatingActivities.fillna(allfinancials.NetCashProvidedByUsedInOperatingActivitiesContinuingOperations).fillna(allfinancials.NetCashProvidedByUsedInContinuingOperations)

finalfinancials['Assets'] = allfinancials.Assets.fillna(allfinancials.AssetsCurrent+allfinancials.AssetsNoncurrent)

finalfinancials['CurrAssets'] = allfinancials.AssetsCurrent

finalfinancials['CurrLiab'] = allfinancials.LiabilitiesCurrent
finalfinancials['price'] = allfinancials['4. close']
finalfinancials['CommonStockSharesOutstanding'] = allfinancials['CommonStockSharesOutstanding'].fillna(allfinancials.EntityCommonStockSharesOutstanding)
finalfinancials['CommonStockSharesOutstanding'] = finalfinancials['CommonStockSharesOutstanding'].groupby('ticker').ffill(3)

finalfinancials = finalfinancials.astype(float)
finalfinancials.dropna(subset='NI',inplace=True)
finalfinancials.sort_index(ascending=True,inplace=True)
finalfinancials['pct_chg_forward'] = finalfinancials.groupby('ticker').price.pct_change().shift(-90)
finalfinancials['Rev_growth_backward'] = finalfinancials.Revenue.drop_duplicates().groupby('ticker').pct_change()
finalfinancials['Rev_growth_backward'] = finalfinancials['Rev_growth_backward'].ffill()
# finalfinancials['days_between_backward'] = [float('nan')]+[finalfinancials.index[i][1] - finalfinancials.index[i-1][1] if finalfinancials.index[i][0] == finalfinancials.index[i-1][0] else float('nan') for i in range(1,len(finalfinancials.index))]
# finalfinancials['days_between_forward'] = [finalfinancials.index[i][1] - finalfinancials.index[i-1][1] if finalfinancials.index[i][0] == finalfinancials.index[i-1][0] else float('nan') for i in range(1,len(finalfinancials.index))]+[float('nan')]
finalfinancials['return_over_rf'] = finalfinancials['pct_chg_forward'] - finalfinancials.treasury_yield

# finalfinancials = finalfinancials.loc[(finalfinancials.days_between_forward.dt.days <= 95) & (finalfinancials.days_between_backward.dt.days <= 95)]

pershare = lambda col: finalfinancials[col]/finalfinancials['CommonStockSharesOutstanding']
pricerat = lambda col: finalfinancials['price']/finalfinancials[col]
margin = lambda col: finalfinancials[col]/finalfinancials['Revenue']
diff = lambda col: finalfinancials[col].groupby('ticker').diff()

finalfinancials['EPS'] = pershare('NI')
finalfinancials['EBITDA_PS'] = pershare('EBITDA')
finalfinancials['EBIT_PS'] = pershare('EBIT')
finalfinancials['CFO_PS'] = pershare('CFO')
finalfinancials['REV_PS'] = pershare('Revenue')
finalfinancials['CurrAss_PS'] = pershare('CurrAssets')
finalfinancials['CurrLiab_PS'] = pershare('CurrLiab')
finalfinancials['Cash_PS'] = pershare('Cash')
finalfinancials['Assets_PS'] = pershare('Assets')

finalfinancials['MarketCap'] = finalfinancials.price*finalfinancials.CommonStockSharesOutstanding
finalfinancials['EV'] = finalfinancials['MarketCap'].fillna(0)+finalfinancials['Debt'].fillna(0)

finalfinancials['EV/EBITDA'] = finalfinancials['EV'].fillna(0)/finalfinancials['EBITDA'].fillna(0)
finalfinancials['EV/EBIT'] = finalfinancials['EV'].fillna(0)/finalfinancials['EBIT'].fillna(0)
finalfinancials['EV/NI'] = finalfinancials['EV'].fillna(0)/finalfinancials['NI'].fillna(0)

finalfinancials['NI_MAR'] = margin('NI')
finalfinancials['EBITDA_MAR'] = margin('EBITDA')
finalfinancials['EBIT_MAR'] = margin('EBIT')
finalfinancials['CFO_MAR'] = margin('CFO')


finalfinancials['Debt_by_EBITDA'] = finalfinancials.Debt/finalfinancials.EBITDA
finalfinancials['Debt_by_EBIT'] = finalfinancials.Debt/finalfinancials.EBIT
finalfinancials['Debt_by_NI'] = finalfinancials.Debt/finalfinancials.NI
finalfinancials['Debt_by_Rev'] = finalfinancials.Debt/finalfinancials.Revenue
finalfinancials['Debt_by_Assets'] = finalfinancials.Debt/finalfinancials.Assets

for col in ['NI_MAR','EBITDA_MAR','EBIT_MAR','CFO_MAR']:
    finalfinancials[col+'_diff'] = diff(col)

finalfinancials['P/E'] = pricerat('EPS')
finalfinancials['P/EBIT'] = pricerat('EBIT_PS')
finalfinancials['P/EBITDA'] = pricerat('EBITDA_PS')
finalfinancials['P/Rev'] = pricerat('REV_PS')
finalfinancials['P/Assets'] = pricerat('Assets_PS')
finalfinancials['P/CFO'] = pricerat('CFO_PS')


sector = pd.get_dummies(allfinancials.sector.str.lower().str.replace(' ','_').str.replace('&','and').str.replace(',',''),prefix='sector')
industry = pd.get_dummies(allfinancials.industry.str.lower().str.replace(' ','_').str.replace('&','and').str.replace(',',''),prefix='industry')

finalfinancials = pd.concat([finalfinancials,sector,industry],axis=1)
#%%

finalfinancials.dropna(subset='pct_chg_forward')

finalfinancials.to_pickle('financials.p')

