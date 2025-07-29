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
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

vifcalc = lambda vfdata: pd.Series({vfdata.columns[i]:vif(vfdata.astype(float),i) for i in range(len(vfdata.columns))})
y = 'pct_chg_forward'

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
        



        #%%

data = pd.read_pickle('Data/financials.p').drop(columns=['treasury_yield'])

#%% Plotting relationships

for x in data.columns:
    if x != y:
        rolling = data[[x,y]].sort_values(by=x).dropna().rolling(1000).mean()
        rolling[x] = rolling[x].clip(lower = rolling[x].quantile(0.05),upper = rolling[x].quantile(0.95))
        rolling.plot(x=x,y=y)


#%% Correlations, Etc.

keepcols = [x for x in data.columns if '_MAR' in x or 'EV' in x or '_by' in x or '_diff' in x or '_growth' in x in x or '/' in x or 'chg' in x or 'yield' in x or 'return_over' in x]
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

highvif = vifs.loc[(vifs >= 5) & (vifs.index != 'const')]

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


#%% Train Test

data_model = add_constant(data_transformed)

X_train, X_test, y_train, y_test = train_test_split(
    data_model[unbucketedcols+bucketedcols+['const']], data_model[y], test_size=0.33, random_state=42)

#%% forward selection

bucketcompares = []


forwardcols = []
threshold = 0.1
notins = ['EBITDA_MAR_diff','REV_PS']
for c in allcols:
    if c not in notins:
        if type(c) != list:
            model = OLS(exog=data_model[['const'] + [c] + forwardcols],endog=data_model[y],hasconst=True)
            results = model.fit()
            pvalue = results.pvalues
            if pvalue[c] <= 0.1:
                forwardcols.append(c)
        elif type(c) == list:
            compare = pd.DataFrame(index=['p','r2'])
            for col in c:
                if col not in notins:
                    model = OLS(exog=data_model[['const'] + [col] + forwardcols],endog=data_model[y],hasconst=True)
                    results = model.fit()
                    pvalue = results.pvalues
                    rsquared = results.rsquared
                    compare[col] = {'p':pvalue[col],'r2':rsquared}
            compare = compare.T
            compare.sort_values(by='r2',ascending=False,inplace=True)
            compare = compare.loc[compare.p <= threshold]
            bucketcompares.append(compare)
            if len(compare.index) > 0:
                forwardcols.append(compare.index[0])
            
forwardmodel = OLS(exog=X_train[['const'] + forwardcols],endog=y_train,hasconst=True)
forwardresults = forwardmodel.fit()
print(forwardresults.summary())

yhat_train = forwardresults.predict(X_train[['const'] + forwardcols])
yhat_test = forwardresults.predict(X_test[['const'] + forwardcols])

rmse_train_forward = rmse(y_train,yhat_train)
rmse_test_forward = rmse(y_test,yhat_test)

print(f'\nTraining Percent RMSE (forward selection): {rmse_train_forward/yhat_train.mean()}')
print(f'Test Percent RMSE (forward selection): {rmse_test_forward/yhat_test.mean()}')
            

#%% backward selection:


backwardstart = list(stats.sort_values(by='p',ascending=False).index)
removed = []
notins = ['EBIT_MAR','EBITDA_MAR','REV_PS','EBIT_MAR_DIFF','P/E','P/EBIT','Debt_by_EBIT', 'Debt_by_Assets_diff','Debt_by_NI_diff']
for c in backwardstart:
    keep = [x for x in backwardstart if x not in removed and x not in notins]
    model = OLS(exog=data_model[['const'] + keep],endog=data_model[y],hasconst=True)
    results = model.fit()
    pvalue = results.pvalues
    drop = pvalue.loc[(pvalue == pvalue.max()) & (pvalue > threshold)]
    if len(drop) > 0:
        removed.append(drop.index[0])

backwardcols = [x for x in backwardstart if x not in removed and x not in notins]

backwardmodel = OLS(exog=X_train[['const'] + backwardcols],endog=y_train,hasconst=True)
backwardresults = backwardmodel.fit()
print(backwardresults.summary())
vifs = vifcalc(data_model[['const'] + backwardcols])
yhat_train = backwardresults.predict(X_train[['const'] + backwardcols])
yhat_test = backwardresults.predict(X_test[['const'] + backwardcols])

rmse_train_backward = rmse(y_train,yhat_train)
rmse_test_forward = rmse(y_test,yhat_test)

print(f'\nTraining Percent RMSE (backward selection): {rmse_train_backward/yhat_train.mean()}')
print(f'Test Percent RMSE (backward selection): {rmse_test_forward/yhat_test.mean()}')
