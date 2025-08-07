#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  4 17:38:29 2025

@author: zdhoffman
"""
import pandas as pd, requests, json, os

os.chdir('/Users/zdhoffman/Documents/Coding Projects/Stock Market Model/')


stockkey = 'GJT87YF8QI5GZUND'
blskey = '6eebf73b51c642fe8914b9b72cc73569'
fredkey = '84a26b17b51a63ed7dae3c7936a19d02'

#%%
allfinancials = pd.read_pickle('Data/allfinancials.p')
#%%

##Only here because getfinancials.py does not have the ability to convert currencies yet.

allfinancials = allfinancials.loc[allfinancials.currency == 'USD']

#%% #Adding in BLS data
headers = {'Content-type': 'application/json'}
print('getting BLS data')
naics_sec_mapper = pd.read_excel('cesseriespub.xlsx',sheet_name='CES_Pub_NAICS_24',header=1)

naics_indata = set(allfinancials.naics_code.dropna())
naics_inmap = set(naics_sec_mapper['NAICS Code(1)'].dropna())
getsec = lambda naics: naics_sec_mapper.loc[naics_sec_mapper['NAICS Code(1)'] == naics]['NAICS Code(1)'].values[0]
naics_sec_map = {}
for naics in naics_indata:
    naics_str = str(int(naics))
    if naics_str in naics_inmap:
        naics_sec_map[naics] = getsec(naics_str)
    elif naics_str[:-1] in naics_inmap:
        naics_sec_map[naics] = getsec(naics_str[:-1])
    elif naics_str[:-2] in naics_inmap:
        naics_sec_map[naics] = getsec(naics_str[:-2])
    elif naics_str[:-3] in naics_inmap:
        naics_sec_map[naics] = getsec(naics_str[:-3])
    elif naics_str[:-4] in naics_inmap:
        naics_sec_map[naics] = getsec(naics_str[:-4])
    elif naics_str[:-5] in naics_inmap:
        naics_sec_map[naics] = getsec(naics_str[:-5])
    elif naics_str[:-6] in naics_inmap:
        naics_sec_map[naics] = getsec(naics_str[:-6])
    


fixes = {**naics_sec_map,454110.0:'455'}


allfinancials['naics_code'] = allfinancials['naics_code'].map(fixes)
naicslist = [str(int(x)) for x in set(allfinancials.naics_code.dropna())]

naics_sec_mapper['seriesID'] = 'CES'+naics_sec_mapper['CES Industry Code'].str.replace('-','')+'01'
series_naics = naics_sec_mapper[['seriesID','NAICS Code(1)']].loc[naics_sec_mapper['NAICS Code(1)'].isin(naicslist)]

notin = [x for x in naicslist if x not in series_naics['NAICS Code(1)'].values]
if len(notin) > 0:
    print('The following naics are not in the mapping and are potentially out of date:')
    print(notin)
serieslist = list(series_naics['seriesID'])

data_nineties = json.dumps({"seriesid": serieslist,"startyear":"1990", "endyear":"2009",'registrationkey':'6eebf73b51c642fe8914b9b72cc73569'})
data_twothousands = json.dumps({"seriesid": serieslist,"startyear":"2010", "endyear":"2029",'registrationkey':'6eebf73b51c642fe8914b9b72cc73569'})



p_nineties = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data_nineties, headers=headers)
p_twothousands = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data_twothousands, headers=headers)


loader = lambda x: json.loads(x.text)['Results']['series']

results = loader(p_nineties)+loader(p_twothousands)



unformatted = pd.DataFrame()
for result in results:
    intermediate = pd.DataFrame(result['data'])
    intermediate['seriesID'] = result['seriesID']
    unformatted = pd.concat([unformatted,intermediate])

unformatted['month_start'] = pd.to_datetime(unformatted['year'].astype(str)+'-'+unformatted['period'].str.replace('M','')+'-01')

unformatted['month_end'] = pd.to_datetime(unformatted['year'].astype(str)+'-'+unformatted['period'].str.replace('M','')+'-'+unformatted.month_start.dt.days_in_month.astype(str))
unformatted['value'] = unformatted['value'].astype(float)
formatted = pd.merge(unformatted,series_naics,left_on='seriesID',right_on='seriesID')[['month_end','NAICS Code(1)','value']].rename(columns={'value':'industry_employment','NAICS Code(1)':'naics_code'})

formatted['industry_employment_growth'] = formatted.groupby(['naics_code'])['industry_employment'].pct_change()
formatted['naics_code'] = formatted['naics_code'].astype(float)
formatted.sort_values(by=['month_end'],ascending=True,inplace=True)
allfinancials.naics_code = allfinancials.naics_code.astype(float)
allfinancials.sort_index(level=1,ascending=True,inplace=True)
allfinancials.reset_index(inplace=True)
allfinancials.rename(columns={'level_0':'ticker','level_1':'date'},inplace=True)
allfinancials_merged = pd.merge_asof(allfinancials,formatted,left_on='date',right_on='month_end',by='naics_code',tolerance=pd.to_timedelta('80D'))
allfinancials_merged.set_index(['ticker','date'],inplace=True)
#%% creating summary values
print('adding in computed variables and other economic data')
def getdata(alldata,url,valname,period,reset_month = False,chg=False,growth=False, source='bls',merge=True):
    global newdata, outdata, moddata
    print(f'pulling data for {valname}.')
    moddata = alldata.copy()
    moddata['date_sort'] = pd.to_datetime([x[1] for x in moddata.index])
    response = requests.get(url)
    if response.status_code == 200:
        if source == 'bls':
            newdata = pd.DataFrame(response.json()['data']).sort_values(by='date',ascending=True).set_index('date').rename(columns={'value':valname})
        elif source == 'fred':
            newdata = pd.DataFrame(response.json()['observations']).sort_values(by='date',ascending=True).set_index('date').rename(columns={'value':valname})
            newdata.drop(columns=['realtime_start','realtime_end'],inplace=True)
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
    if newdata[valname].dtype != float:
        newdata[valname]=newdata[valname].astype(float)
    
    
    if chg == True:
        newdata[valname+'_diff_'+period] = newdata[valname].diff()
        if period == 'daily': #only do this calculation for information available on a daily interval, otherwise 31 periods will be 31 months.
            newdata[valname+'_diff_monthly'] = newdata[valname].diff(periods=31)
            newdata[valname+'_diff_quarterly'] = newdata[valname].diff(periods=93)
        if period == 'weekly': 
            newdata[valname+'_diff_monthly'] = newdata[valname].diff(periods=4)
            newdata[valname+'_diff_quarterly'] = newdata[valname].diff(periods=16)
        if period == 'monthly':
            newdata[valname+'_diff_quarterly'] = newdata[valname].diff(periods=4)
    if growth == True:    
        newdata[valname+'_pctdiff_'+period] = newdata[valname].pct_change()
        if period == 'daily': #only do this calculation for information available on a daily interval, otherwise 31 periods will be 31 months.
            newdata[valname+'_pctdiff_monthly'] = newdata[valname].pct_change(periods=31)
            newdata[valname+'_pctdiff_quarterly'] = newdata[valname].pct_change(periods=93)
        if period == 'weekly': 
            newdata[valname+'_pctdiff_monthly'] = newdata[valname].pct_change(periods=4)
            newdata[valname+'_pctdiff_quarterly'] = newdata[valname].pct_change(periods=16)
        if period == 'monthly':
            newdata[valname+'_pctdiff_quarterly'] = newdata[valname].pct_change(periods=4)
    moddata.sort_values(by='date_sort',ascending=True,inplace=True)
    #this is mostly for bug fixing, by allowing the output to not be merged with moddata
    if merge == True:
        outdata = pd.merge_asof(moddata,newdata,left_on='date_sort',right_index=True,tolerance = tolerance)
        outdata.sort_index(ascending=True,inplace=True)
        outdata[valname] = outdata[valname].groupby('ticker').ffill()
        
        if chg == True:
            for c in outdata.columns:
                if '_diff_' in c:
                    outdata[c] = outdata[c].groupby('ticker').ffill()
                
        if growth == True:    
            for c in outdata.columns:
                if '_pctdiff_' in c:
                    outdata[c] = outdata[c].groupby('ticker').ffill()
    
    
        
        
        
        return outdata.drop(columns='date_sort')
    else:
        return newdata


finalfinancials = pd.DataFrame(index=allfinancials_merged.index)
finalfinancials.index.set_names(['ticker','date'],inplace=True)

finalfinancials['industry_employment_growth'] =  allfinancials_merged['industry_employment_growth']

finalfinancials['LTDebt'] = (allfinancials_merged['LongTermDebt']).fillna(allfinancials_merged['LongTermDebtNoncurrent'])
finalfinancials['CurrLTDebt']= allfinancials_merged['LongTermDebtCurrent'].fillna(allfinancials_merged['DebtCurrent']).fillna(allfinancials_merged.LongTermDebtAndCapitalLeaseObligationsCurrent)
finalfinancials['STDebt'] = allfinancials_merged['ShortTermBorrowings'].fillna(allfinancials_merged.OtherShortTermBorrowings)

finalfinancials['Debt'] = finalfinancials[['LTDebt','CurrLTDebt','STDebt']].sum(axis=1)

finalfinancials['Revenue'] = allfinancials_merged.Revenues.fillna(allfinancials_merged.RevenueFromContractWithCustomerIncludingAssessedTax).fillna(allfinancials_merged.RevenueFromContractWithCustomerExcludingAssessedTax).fillna(allfinancials_merged.SalesRevenueNet)





finalfinancials['D&A'] = allfinancials_merged.AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment.fillna(allfinancials_merged.DepreciationDepletionAndAmortization).fillna(allfinancials_merged.Depreciation).fillna(allfinancials_merged.DepreciationAndAmortization).fillna(allfinancials_merged.PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization)



      

finalfinancials['Interest'] = allfinancials_merged.InterestExpense.fillna(0)

finalfinancials['Tax'] = allfinancials_merged.IncomeTaxExpenseBenefit

finalfinancials['NI'] = allfinancials_merged.NetIncomeLoss.fillna(allfinancials_merged.ProfitLoss)
finalfinancials['EBIT'] = allfinancials_merged.IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest.fillna(allfinancials_merged.IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments).fillna(finalfinancials.NI + finalfinancials.Interest+finalfinancials.Tax)
finalfinancials['EBITDA'] = finalfinancials.EBIT + finalfinancials['D&A']
finalfinancials['CFO'] = allfinancials_merged.NetCashProvidedByUsedInOperatingActivities.fillna(allfinancials_merged.NetCashProvidedByUsedInOperatingActivitiesContinuingOperations).fillna(allfinancials_merged.NetCashProvidedByUsedInContinuingOperations)

finalfinancials['Assets'] = allfinancials_merged.Assets.fillna(allfinancials_merged.AssetsCurrent+allfinancials_merged.AssetsNoncurrent)

finalfinancials['CurrAssets'] = allfinancials_merged.AssetsCurrent

finalfinancials['CurrLiab'] = allfinancials_merged.LiabilitiesCurrent
finalfinancials['price'] = allfinancials_merged['4. close']
finalfinancials['CommonStockSharesOutstanding'] = allfinancials_merged['CommonStockSharesOutstanding'].fillna(allfinancials_merged.EntityCommonStockSharesOutstanding)
finalfinancials['CommonStockSharesOutstanding'] = finalfinancials['CommonStockSharesOutstanding'].groupby('ticker').ffill(3)

finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=TREASURY_YIELD&interval=daily&maturity=1mo&apikey={stockkey}','treasury_yield','daily',chg=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=WTI&interval=daily&apikey={stockkey}','wti_crude_price','daily',growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=ALL_COMMODITIES&interval=monthly&apikey={stockkey}','commodity_index','monthly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=REAL_GDP&interval=quarterly&apikey={stockkey}','real_gdp','quarterly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=FEDERAL_FUNDS_RATE&interval=daily&apikey={stockkey}','fed_funds_rate','daily',chg=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=CPI&interval=monthly&apikey={stockkey}','cpi','monthly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=RETAIL_SALES&apikey={stockkey}','retail_sales','monthly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=DURABLES&apikey={stockkey}','durable_goods_orders','monthly',reset_month=True,growth=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=UNEMPLOYMENT&apikey={stockkey}','unemployment','monthly',reset_month=True,chg=True)
finalfinancials = getdata(finalfinancials,f'https://www.alphavantage.co/query?function=NONFARM_PAYROLL&apikey={stockkey}','nonfarm_payroll','monthly',reset_month=True,growth=True)

feddata = lambda series,name,period,reset_month,change,pctchange,source,merge: getdata(finalfinancials,f'https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key={fredkey}&file_type=json',name,period,reset_month=reset_month,chg=change, growth=pctchange,source=source,merge=merge)

#Consumer Sentiment Data
finalfinancials = feddata('UMCSENT','consumer_sentiment','monthly',reset_month=True,change=True, pctchange=True,source='fred')

finaldata_preadp = finalfinancials.copy()

#ADP data
finalfinancials = feddata('ADPWNUSNERSA','adp_private_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWINDMANNERSA','adp_manufacturing_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWINDCONNERSA','adp_construction_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWINDINFONERSA','adp_information_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWINDLSHPNERSA','adp_hospitality_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWINDPROBUSNERSA','adp_profservices_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPMINDEDHLTNERSA','adp_ed_health_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')

finalfinancials = feddata('ADPWES1T19ENERSA','adp_smallbiz_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWES500PENERSA','adp_largebiz_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWES250T499ENERSA','adp_medlargebiz_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')


finalfinancials = feddata('ADPWINDTTUNERSA','adp_trade_transport_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWINDFINNERSA','adp_financial_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWINDNRMINNERSA','adp_mining_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')
finalfinancials = feddata('ADPWINDOTHSRVNERSA','adp_otherservices_payrolls','weekly',reset_month=True,change=True, pctchange=True,source='fred')

print('done pulling payroll data')

#PPI data
finalfinancials = feddata('PCUOMFGOMFG','ppi_total','monthly',reset_month=True,change=True, pctchange=True,source='fred')

#all manufacturing naics codes
manufacturing = allfinancials.loc[allfinancials.naics_code.astype(str).str[:2].isin(['31','32','33'])][['naics_code','sic_desc']].drop_duplicates(subset='naics_code')
man_naics = [str(int(x)) for x in manufacturing['naics_code']]
man_desc = list(manufacturing['sic_desc'])

# print('pulling ppi data')
# for i in range(len(man_naics)):
#     naics = man_naics[i]
#     try:
        
#         naics_used = naics
#         finalfinancials = feddata(f'PCU{naics_used}{naics_used}',f'ppi_{naics_used}','monthly',reset_month=True,change=True, pctchange=True,source='fred')    
#     except:
#         try:
#             naics_used = naics[:-1]
#             finalfinancials = feddata(f'PCU{naics_used}{naics_used}',f'ppi_{naics_used}','monthly',reset_month=True,change=True, pctchange=True,source='fred')
#         except:
#             try:
#                 naics_used = naics[:-2]
#                 finalfinancials = feddata(f'PCU{naics_used}{naics_used}',f'ppi_{naics_used}','monthly',reset_month=True,change=True, pctchange=True,source='fred')
#             except:
#                 try:
#                     naics_used = naics[:-3]
#                     finalfinancials = feddata(f'PCU{naics_used}{naics_used}',f'ppi_{naics_used}','monthly',reset_month=True,change=True, pctchange=True,source='fred')
#                 except:
#                     try:
#                         naics_used = naics[:-4]
#                         finalfinancials = feddata(f'PCU{naics_used}{naics_used}',f'ppi_{naics_used}','monthly',reset_month=True,change=True, pctchange=True,source='fred')
#                     except:
#                         print(f'naics {naics} not in ppi data')


# print('done creating all in one ppi')
      



finalfinancials = finalfinancials.astype(float)
finalfinancials.dropna(subset='NI',inplace=True)
finalfinancials.sort_index(ascending=True,inplace=True)
finalfinancials['pct_chg_forward_monthly'] = finalfinancials.groupby('ticker').price.pct_change().shift(-31)
finalfinancials['pct_chg_forward_weekly'] = finalfinancials.groupby('ticker').price.pct_change().shift(-7)
finalfinancials['pct_chg_forward_quarterly'] = finalfinancials.groupby('ticker').price.pct_change().shift(-91)
finalfinancials['Rev_growth_backward'] = finalfinancials.Revenue.drop_duplicates().groupby('ticker').pct_change()
finalfinancials['Rev_growth_backward'] = finalfinancials['Rev_growth_backward'].ffill()

# finalfinancials['return_over_rf'] = finalfinancials['pct_chg_forward'] - finalfinancials.treasury_yield



pershare = lambda col: finalfinancials[col]/finalfinancials['CommonStockSharesOutstanding']
pricerat = lambda col: finalfinancials['price']/finalfinancials[col]
margin = lambda col: finalfinancials[col]/finalfinancials['Revenue']
diff = lambda col: finalfinancials[col].groupby('ticker').diff()

finalfinancials['EPS'] = pershare('NI')
finalfinancials['EBITDA_PS'] = pershare('EBITDA')
finalfinancials['EBIT_PS'] = pershare('EBIT')
finalfinancials['CFO_PS'] = pershare('CFO')
finalfinancials['REV_PS'] = pershare('Revenue')

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




sector = pd.get_dummies(allfinancials_merged.sector.str.lower().str.replace(' ','_').str.replace('&','and').str.replace(',',''),prefix='sector')
industry = pd.get_dummies(allfinancials_merged.industry.str.lower().str.replace(' ','_').str.replace('&','and').str.replace(',',''),prefix='industry')

finalfinancials_wsector = pd.merge(finalfinancials,sector,how='inner',left_index=True,right_index=True)
finalfinancials_merged = pd.merge(finalfinancials_wsector,industry,how='inner',left_index=True,right_index=True)





#%%

finalfinancials.dropna(subset='pct_chg_forward')

finalfinancials.to_pickle('Data/financials.p')

