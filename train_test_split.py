# -*- coding: utf-8 -*-
"""
Created on Mon Oct 27 15:06:23 2025

@author: paperspace
"""

import pandas as pd

data = pd.read_pickle('Data/financials.p')
train = data.sample(frac=0.8,random_state=42)
test = data.loc[~data.index.isin(train.index)]

train.to_pickle('Data/train_init.p')
test.to_pickle('Data/test_init.p')