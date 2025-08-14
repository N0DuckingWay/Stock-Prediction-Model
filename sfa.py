#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 16:38:25 2025

@author: zdhoffman
"""

import pandas as pd, matplotlib.pyplot as plt, numpy as np, warnings
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif
from statsmodels.tools.tools import add_constant
from scipy.stats import shapiro, boxcox
from statsmodels.regression.linear_model import OLS
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error as rmse
import matplotlib.pyplot as plt, os

os.chdir(r'C:\Users\paperspace\Documents\Coding Projects\Stock-Prediction-Model')

warnings.filterwarnings("ignore")
y = 'pct_chg_forward_weekly'

vifcalc = lambda vfdata: pd.Series({vfdata.columns[i]:vif(vfdata.astype(float),i) for i in range(len(vfdata.columns))})







#%%

def get_timeseries(varname,y=y,n_roll = 1,lag=None,mindate=None,maxdate=None,maxvar = np.inf,minvar = -np.inf):
    global graphdata,tsdata,counts
    if n_roll < 1:
        raise ValueError('n_roll must be greater than or equal to 1')
    
    indata = data.copy()
    
    tsdata = indata.groupby('date')[[varname,y]].mean()
    counts = indata.groupby('date')[y].count()
    tsdata = tsdata.loc[counts/counts.max() > 0.25]
    if lag != None:
        tsdata[varname] = tsdata[varname].shift(lag)
    graphdata = tsdata.rolling(n_roll).mean()
    
    if mindate != None:
        graphdata = graphdata.loc[graphdata.index >= pd.to_datetime(mindate)]
    if maxdate != None:
        graphdata = graphdata.loc[graphdata.index <= pd.to_datetime(maxdate)]
        
    graphdata = graphdata.loc[graphdata.index.dayofweek <= 4]
    
    graphdata[varname] = graphdata[varname].clip(upper = maxvar,lower=minvar)
    
    
    fig, ax1 = plt.subplots()
    ax1.plot(graphdata.index,graphdata[y],color='black')
    ax1.set_ylabel(y,color='black')
    ax1.tick_params(axis='y',labelcolor='black')
    
    ax2 = ax1.twinx()
    ax2.plot(graphdata.index,graphdata[varname],color='red')
    ax2.set_ylabel(varname,color='red')
    ax2.tick_params(axis='y',color='red')
    ax1.tick_params(axis='x',rotation=45)
    
    ax1.set_xlabel('date')
    plt.title(f'{varname} vs {y} time series')
    plt.show()
    
def plot_recessions(varname,y=y,n_roll = 1,lag=None,maxvar = np.inf,minvar = -np.inf):
    get_timeseries(varname,y=y,n_roll = n_roll,lag=lag,mindate=None,maxdate='2010-12-31',maxvar=maxvar,minvar=minvar)
    get_timeseries(varname,y=y,n_roll = n_roll,lag=lag,mindate='2020-01-01',maxdate='2022-01-01',maxvar=maxvar,minvar=minvar)
    
    
    

def transform(series,choose=False):
    global norms
    
    series.hist(bins=30)
    plt.title(series.name)
    plt.show()
    
    series = series.fillna(0) #this is ok because only the slope_std values are null, and that's only for runs with two points
    norms = pd.DataFrame(index=['stat','p'])
    
    if series.max() < 0:
        series = series * -1
        t = '*-1'
    elif series.max() >= 0 and series.min() < 0:
        series = series-series.min()+1
        t = '+min+1'
    elif series.min() == 0:
        series = series + 1
        t = '+1'
    else:
        t = 'none'
    
    norms['none']= shapiro(series)
    norms['ln'] = shapiro(np.log(series))
    norms['log10'] = shapiro(np.log10(series))
    norms['sqrt'] = shapiro(np.sqrt(series))
    bc = boxcox(series)
    norms['bc'] = shapiro(bc[0])
    norms['logit'] = shapiro(np.log(series.replace(1,0.99999).replace(0,0.0001)/(1-series.replace(1,0.99999).replace(0,0.0001))))
    
    norms['bc_lambda'] = bc[1]
    norms = norms.T
    
    best = norms['p'].drop(index=['bc_lambda']).loc[norms['p'].drop(index=['bc_lambda'])==norms['p'].drop(index=['bc_lambda']).max()].index[0]
    if (norms['p'][best] <= norms['p']['none'] + .05) and (norms['p'][best] <= norms['p']['none']*2):
        best = 'none'
    
    if choose==False:
        out = pd.concat([norms['p'],pd.Series({'transform':t})])
        out['best'] = best
        return out
    else:
        maxval = norms.loc[(norms.p == norms.p.max()) & (norms.p > 0.0)].index[0]
        print(f'Transforming {series.name} using {maxval}')
        if maxval == 'ln':
            return np.log(series)
        elif maxval == 'log10':
            return np.log10(series)
        elif maxval == 'sqrt':
            return np.sqrt(series)
        elif maxval == 'bc':
            return boxcox(series)[0]
        elif maxval == 'logit':
            return np.log(series.replace(1,0.99999).replace(0,0.0001)/(1-series.replace(1,0.99999).replace(0,0.0001)))
        else:
            return series
        

def relgraph(x):
    rollingmean = meanplotdata[[x,'price']].sort_values(by=x).dropna().rolling(1000).mean()
    rollingmean[x] = rollingmean[x].clip(lower = clip[x]['lower'],upper = clip[x]['upper'])
    
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True)
    
    rollingmean.plot(x=x,y='price',ax=axes[0])
    rollingmean[x].hist(ax=axes[1])
    plt.title(f'Graph of price by {x}')
    plt.show()
    plt.savefig(f'Plots\price_by_{x.replace("/","_")}.png')

        #%%

data = pd.read_pickle('Data/financials.p').drop(columns='sector')

depvars = [x for x in data if 'pct_chg_forward' in x]



#%% Plotting relationships

meanplotdata = data.groupby('date').mean()

clip = {x:{'lower':data[x].mean()-data[x].std()*1.96,'upper':data[x].mean()+data[x].std()*1.96} for x in data.columns if data[x].dtype != bool}

for x in data.columns:
    if x not in depvars and 'price' not in x and x in clip.keys():
        relgraph(x)


#%% Correlations, Etc.

keepcols = [x for x in data.columns if '_MAR' in x or 'EV' in x or '_by' in x or '_diff' in x or '_growth' in x in x or '/' in x or 'chg' in x or 'yield' in x or 'return_over' in x  or 'fed_funds' in x or 'unemployment' in x]
clippeddata= data.copy()
corrs_all = data.corr()
corrs_all.to_excel('Analysis/corrs.xlsx')
corrs = data.corr()[keepcols]

vifdata = add_constant(data.fillna(data.mean()))

for col in data.columns:
    minmax = vifdata[col].loc[(vifdata[col] != float('inf')) & (vifdata[col] != float('-inf'))]
    min_byticker = minmax.groupby('ticker').min()
    max_byticker = minmax.groupby('ticker').max()
    vifdata[col] = vifdata[col].clip(lower=min_byticker.min(),upper=max_byticker.max())
    
    vifdata[col] = vifdata[col].fillna(minmax.mean())
    clippeddata[col] = clippeddata[col].clip(lower=min_byticker.min(),upper=max_byticker.max())
    clippeddata[col] = clippeddata[col].fillna(minmax.mean())


vifs = vifcalc(vifdata[[x for x in vifdata.columns if x != y and 'days_between' not in x.lower()]])

highvif = vifs.loc[(vifs >= 5) & (vifs.index != 'const')].sort_values(ascending=False)

vif_corr = pd.DataFrame({col:corrs_all[col].loc[(corrs_all[col] < 1) & (corrs_all[col] > 0.5)].to_dict() for col in highvif.index})



    



drop = ['EV','EV/EBIT','EV/NI','Debt_by_NI','P/EBITDA',]

vifdata_dropped = vifdata.drop(columns=drop)
vifs_final = vifcalc(vifdata_dropped)


data_mc_dropped = clippeddata.copy()[keepcols].drop(columns=drop)




#%% Normalizing

sh_result = pd.Series()
for c in data_mc_dropped.columns:
    stat,p = shapiro(data_mc_dropped[c].dropna())
    sh_result[c] = p

sh_result.sort_values(ascending=False,inplace=True)

  
transforms = pd.DataFrame(columns = data_mc_dropped.columns)
for c in transforms.columns:
    transforms[c] = transform(data_mc_dropped[c])

    
data_transformed= data_mc_dropped.copy()
for c in data_transformed.columns:
    data_transformed[c] = transform(data_transformed[c],choose=True)
    

#%% Choosing from buckets for high vif variables that serve similar purposes:

stats = {}

for key in data_transformed.columns:
    if key not in ['pct_chg_forward']:
        data_const = add_constant(data_transformed[[key,y]])
        model = OLS(exog=data_const[[key,'const']],endog=data_const[y],hasconst=True)
        results = model.fit()
        pvalue = results.pvalues[key]
        
        coef = results.params[key]
        rsquared = results.rsquared_adj
        stats[key] = {'R2':rsquared,'coef':coef,'p':pvalue}
stats = pd.DataFrame(stats).T.sort_values(by='p',ascending=True)


buckets = [[x for x in stats.index if 'PS' in x],
           [x for x in stats.index if 'P/' in x or 'EV/' in x],
           [x for x in stats.index if '_MAR' in x],
           [x for x in stats.index if 'debt_by' in x.lower()]]
bucketedcols = [x for y in buckets for x in y]
unbucketedcols = [x for x in stats.index if x not in bucketedcols and x not in ['pct_chg_forward']]
allcols = unbucketedcols + buckets


