# -*- coding: utf-8 -*-
"""
Created on Fri Aug 15 11:26:26 2025

@author: paperspace
"""
import pandas as pd, matplotlib.pyplot as plt, numpy as np, warnings
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif
from statsmodels.tools.tools import add_constant
from scipy.stats import shapiro, boxcox
from statsmodels.regression.linear_model import OLS
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error as rmse
import matplotlib.pyplot as plt, os, gc

vifcalc = lambda vfdata: pd.Series({vfdata.columns[i]:vif(vfdata.astype(float),i) for i in range(len(vfdata.columns))})
warnings.filterwarnings("ignore")
y = 'pct_chg_forward_weekly'



def relgraph(meanplotdata,x,y='price',roll=500):
    rollingmean = meanplotdata[[x,y]].sort_values(by=x).dropna().rolling(roll).mean()
    rollingmean[x] = rollingmean[x].clip(lower = clip[x]['lower'],upper = clip[x]['upper'])
    if y != 'price':
        rollingmean[y] = rollingmean[y].clip(lower = -1,upper = min(1,rollingmean[y].median()+1.96*rollingmean[y].std()))
    
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True)
    
    rollingmean.plot(x=x,y=y,ax=axes[0])
    rollingmean[x].hist(ax=axes[1],bins=min(30,len(meanplotdata[x].round(5).drop_duplicates())))
    plt.title(f'Graph of {y} by {x}')
    plt.savefig(f'Plots\{y}_by_{x.replace("/","_")}.png')
    plt.show()
    
    #TODO: put in code to graph the "x" and "y" variables as y variables on the same graph, with the x axis being the date.



drop = ['man_by_ppi_ind','man_by_ppi_ind_pctchg_monthly','man_by_ppi_ind_pctchg_quarterly'
        #'EV','EV/EBIT','EV/NI','Debt_by_NI','P/EBITDA',
        ]
#%%
print('Plotting relationships')


data_transformed = pd.read_pickle(r'Data\normalized.p')
bools = list(data_transformed.dtypes.loc[data_transformed.dtypes=='bool'].index)
data_transformed[bools] = data_transformed[bools].astype(int)
keepcols = [x for x in data_transformed.columns]
            
meanplotdata = data_transformed.groupby('date').mean()

clip = {x:{'lower':data_transformed[x].mean()-data_transformed[x].std()*1.96,'upper':data_transformed[x].mean()+data_transformed[x].std()*1.96} for x in data_transformed.columns if data_transformed[x].dtype != bool}
depvars = [x for x in data_transformed if 'pct_chg_forward' in x]
for x in data_transformed.columns:
    if x not in depvars and 'price' not in x and "pct_chg_forward" not in x and x in clip.keys():
        relgraph(meanplotdata,x)
        relgraph(meanplotdata,x,y='pct_chg_forward_weekly')
        relgraph(meanplotdata,x,y='pct_chg_forward_monthly')
        relgraph(meanplotdata,x,y='pct_chg_forward_quarterly')
        
        #%% data capping and flooring based on above results

        

#%% 
print('Choosing from buckets for high vif variables that serve similar purposes:')
stats = {}

for key in data_transformed.columns: 
    if key not in depvars:
        data_const = add_constant(data_transformed[[key,y]],has_constant='add')
        model = OLS(exog=data_const[[key,'const']],endog=data_const[y],hasconst=True)
        results = model.fit()
        pvalue = results.pvalues[key]
        
        coef = results.params[key]
        rsquared = results.rsquared_adj
        stats[key] = {'R2':rsquared,'coef':coef,'p':pvalue}
stats = pd.DataFrame(stats).T.sort_values(by='p',ascending=True)



#%%
print('Getting VIF data')
vifdata = add_constant(data_transformed.fillna(data_transformed.mean()).copy())

corrs_all = data_transformed.corr()
corrs = data_transformed.corr()[keepcols]

del data_transformed
gc.collect()

for col in vifdata.columns:
    if col != 'const':
        minmax = vifdata[col].loc[(vifdata[col] != float('inf')) & (vifdata[col] != float('-inf'))]
        min_byticker = minmax.groupby('ticker').min()
        max_byticker = minmax.groupby('ticker').max()
        vifdata[col] = vifdata[col].clip(lower=min_byticker.min(),upper=max_byticker.max())
        
        vifdata[col] = vifdata[col].fillna(minmax.mean())



vifs = vifcalc(vifdata[[x for x in vifdata.columns if x != y and 'days_between' not in x.lower()]])

highvif = vifs.loc[(vifs >= 5) & (vifs.index != 'const')].sort_values(ascending=False)

vif_corr = pd.DataFrame({col:corrs_all[col].loc[(corrs_all[col] < 1) & (corrs_all[col] > 0.5)].to_dict() for col in highvif.index})

vifdata_dropped = vifdata.drop(columns=drop)
vifs_final = vifcalc(vifdata_dropped)

with pd.ExcelWriter('Analysis/sfa.xlsx') as writer:
    stats.to_excel(writer,sheet_name='stats')
    vifs_final.to_excel(writer,sheet_name='vifs')
    vif_corr.to_excel(writer,sheet_name='vif_corr')
    corrs.to_excel(writer,sheet_name='corrs')
    