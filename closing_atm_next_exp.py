# -*- coding: utf-8 -*-
"""
Created on Mon May  8 18:41:36 2023

@author: Dell
"""

import pandas as pd 
import numpy as np 
# from MRK import Spot_Fut as fut, Option_Data as opt
from MRK import *


fut = Spot_Fut()
opt = Option_Data()
ticker = 'SPY'

spot_df, resmpld = fut.outliers_resampled(fut.get_spot(ticker))
exp = opt.get_expiry('SPY')

date_list = spot_df['Date'].unique()

def between_dates(DF, start_date, end_date):
    df = DF.copy()
    
    df = df.loc[(df.index.date > start_date) & (df.index.date <= end_date)]
    return df
    
 
main_df = pd.DataFrame()

for idx, date in enumerate(date_list):
    # break
    
    day_df = spot_df.loc[spot_df['Date'] == date].reset_index(drop=True)
    
    last_price = day_df.iloc[-2]['Last']
    last_time = day_df.iloc[-2]['Datetime']
    
    strike = round(last_price, 0)
    
    expiry = [i for i in exp['Date'] if i.date() >= date][0]
    
    try:
        call_df = between_dates(opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type = 'C')), 
                                start_date = date, end_date=expiry.date()
                                ).drop(['RIC', 'Opt_Type'], axis=1)
        put_df = between_dates(opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type = 'P')), 
                               start_date = date, end_date=expiry.date()
                               ).drop(['RIC', 'Opt_Type'], axis=1)
    except:
        print(f'no data for : date : {date}, expiry : {expiry}, strike : {strike}')
        continue
    
    merge_df = pd.merge(call_df, put_df, on=['Datetime', 'Expiry', 'Strike'], how='outer', suffixes=['_C', '_P']).reset_index()
    merge_df['Last'] = last_price 
    
    main_df = pd.concat([main_df, merge_df], ignore_index=True).drop_duplicates()
    
    
main_df.to_csv('D:\Coding\Extracted Data\closing_atm_next_exp.csv', index=False)
        
    



























