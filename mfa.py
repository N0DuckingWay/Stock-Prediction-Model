# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 14:58:11 2025

@author: paperspace
"""

import pandas as pd, sklearn as sk
from sklearn.linear_model import LinearRegression as lr
from sklearn.model_selection import train_test_split

data = pd.read_pickle(r'Data\normalized.p')

data_sample = data.sample(frac=0.03,random_state=42)
yvar = 'pct_chg_forward_quarterly'
y=data_sample[yvar]
X=data_sample.drop(columns = [x for x in data_sample.columns if 'chg' in x])
X_train,X_test,y_train,y_test = train_test_split(X,y,random_state=42,test_size=0.2)
