#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 16:38:25 2025

@author: zdhoffman
"""

import pandas as pd, matplotlib.pyplot as plt, numpy as np, warnings
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif
from scipy.stats import shapiro, boxcox
import os, gc

os.chdir(r'C:\Users\paperspace\Documents\Coding Projects\Stock-Prediction-Model')

warnings.filterwarnings("ignore")
y = 'pct_chg_forward_weekly'

vifcalc = lambda vfdata: pd.Series({vfdata.columns[i]:vif(vfdata.astype(float),i) for i in range(len(vfdata.columns))})

normalize = False





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
    
    
    

def find_normalization(series):
    series.hist(bins=30)
    plt.title(series.name)
    plt.savefig(rf'Distribution Plots/{series.name}.png')
    plt.show()
    
    if len(set(series.dropna())) > 1: #if series is constant for any reason.
    
        series = series.dropna()
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
        log = np.log(series)
        norms['ln'] = shapiro(log)
        log10 = np.log10(series)
        norms['log10'] = shapiro(log10)
        sqrt=np.sqrt(series)
        norms['sqrt'] = shapiro(sqrt)
        bc = boxcox(series)
        norms['bc'] = shapiro(bc[0])
        logit = np.log(series.replace(1,0.99999).replace(0,0.0001)/(1-series.replace(1,0.99999).replace(0,0.0001)))
        norms['logit'] = shapiro(logit)
        
        transform_p = norms.copy()
        norms['bc_lambda'] = bc[1]
        norms = norms.T
        
        best = norms['p'].drop(index=['bc_lambda']).loc[norms['p'].drop(index=['bc_lambda'])==norms['p'].drop(index=['bc_lambda']).max()].index[0]
        if (norms['p'][best] <= norms['p']['none'] + .05) and (norms['p'][best] <= norms['p']['none']*2):
            best = 'none'
        
        
        out = pd.concat([norms['p'],pd.Series({'transform':t})])
        out['best'] = best
        return out

    else:
        return series
        

def normalize(inseries,first_transform,normalizer,bc_lambda=None):
    '''
    

    Parameters
    ----------
    inseries : Pandas series
        the series that is being transformed
    first_transform : str
        the first transform, to make sure that values in the series conform to the needs of the normalizer function.
        Must be either '*-1','+min+1','+1', or 'none'
    normalizer : str
        The name of the normalizer function. Must be either 'ln', 'log10', 'sqrt', ' bc', 'logit', or 'none'
    bc_lambda : float, optional
        The lambda for the box cox transformation. Mandatory if normlizer == 'bc'. The default is None.

    Returns
    -------
    a transformed series.

    '''
    
    if first_transform == '*-1':
        data = inseries*-1
    elif first_transform == '+min+1':
        data = inseries-inseries.min()+1
    elif first_transform == '+1':
        data = inseries+1
    elif first_transform == 'none':
        data = inseries.copy()
    else:
        raise ValueError("Invalid value for first_transform parameter. Must be either '*-1','+min+1','+1', or 'none'.")
    
    if normalizer=='ln':
        return np.log(data)
    elif normalizer == 'log10':
        return np.log10(data)
    elif normalizer == 'sqrt':
        return np.sqrt(data)
    elif normalizer == 'bc':
        if bc_lambda != None:
            return boxcox(data,lmbda=bc_lambda)
        else:
            return ValueError('Invalid value for bc_lambda. Since normalizer=="bc", bc_lambda cannot be "None"')
    elif normalizer == 'logit':
        return np.log(data.replace(1,0.99999).replace(0,0.0001)/(1-data.replace(1,0.99999).replace(0,0.0001)))
    elif normalizer == 'none':
        return data
    else:
        raise ValueError("Invalid value for normalizer parameter. Must be either 'ln', 'log10', 'sqrt', ' bc', 'logit', or 'none'.")
    


        #%%

data = pd.read_pickle('Data/train_init.p').drop(columns='sector')









#%%
print('Capping and flooring data')

clippeddata= data.copy()




for col in data.columns:
    
    minmax = clippeddata[col].loc[(clippeddata[col] != float('inf')) & (clippeddata[col] != float('-inf'))]
    # min_byticker = minmax.groupby('ticker').min()
    # max_byticker = minmax.groupby('ticker').max()
    if col == 'pct_chg_forward_weekly':
        clippeddata[col] = clippeddata[col].clip(lower=-0.25,upper=0.25)
    elif col == 'pct_chg_forward_monthly':
        clippeddata[col] = clippeddata[col].clip(lower=-0.55,upper=0.55)
    elif col == 'pct_chg_forward_quarterly':
        clippeddata[col] = clippeddata[col].clip(lower=-1.2,upper=1.2)
    else:
        clippeddata[col] = clippeddata[col].clip(lower=minmax.min(),upper=minmax.max())
    # clippeddata[col] = clippeddata[col].fillna(minmax.mean())

del data
gc.collect()




    



drop = ['man_by_ppi_ind','man_by_ppi_ind_pctchg_monthly','man_by_ppi_ind_pctchg_quarterly'
        #'EV','EV/EBIT','EV/NI','Debt_by_NI','P/EBITDA',
        ]




data_mc_dropped = clippeddata.copy().drop(columns=drop)

del clippeddata
gc.collect()

#%%
print('Normalizing')

if normalize == True:
    sh_result = pd.Series()
    for c in data_mc_dropped.columns:
        stat,p = shapiro(data_mc_dropped[c].dropna())
        sh_result[c] = p
    
    sh_result.sort_values(ascending=False,inplace=True)
    
      
    transforms = pd.DataFrame(columns = data_mc_dropped.columns)
    for c in transforms.columns:
        if len(set(data_mc_dropped[c].round(5))) > 2:
            print(f'Getting best transformation for {c}')
            transforms[c] = find_normalization(data_mc_dropped[c])
        else:
            transforms[c] = None
    transforms.to_excel('transforms.xlsx')
else:
    transforms = pd.read_excel('transforms.xlsx',index_col=0)
    
data_transformed= data_mc_dropped.copy()
for c in data_transformed.columns:
    if len(transforms[c].dropna()) >0:
        transform = transforms[c]['transform']
        normalizer = transforms[c]['best']
        bc_lambda = transforms[c]['bc_lambda']
        if len(set(data_mc_dropped[c])) > 2 and transforms[c]['best'] != 'none': 
            print(f'Transforming {c}')
            data_transformed[c] = normalize(data_transformed[c],first_transform=transform,normalizer=normalizer,bc_lambda=bc_lambda)
            gc.collect()

data_transformed.to_pickle(r'Data\normalized.p')
del data_mc_dropped
gc.collect()
