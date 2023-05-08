# -*- coding: utf-8 -*-
"""
Created on Wed May  3 22:36:48 2023

@author: Dell
"""

import pandas as pd 
import numpy as np 
from MRK import *

fut = Spot_Fut()
opt = Option_Data()

exp = opt.get_expiry('SPY')

spot_df, resamp = fut.outliers_resampled(fut.get_spot('SPY'))



sl_pct = 1.5

def open_trade(date, strike, call_df, put_df, sl_pct, start_time = pd.Timestamp('09:30:00').time(), spot=None):
    open_trades = pd.DataFrame()
    
    datetime = pd.to_datetime(date.strftime('%Y-%m-%d') + ' ' + start_time.strftime('%H:%M:%S'))
    
    call_entry = call_df.loc[call_df.index.time >= start_time]['Last'].values[0]
    put_entry = put_df.loc[put_df.index.time >= start_time]['Last'].values[0]
    
    call_entry_time = call_df.loc[call_df.index.time >= start_time].index[0].time()
    put_entry_time = put_df.loc[put_df.index.time >= start_time].index[0].time()
    
    call_sl = round(call_entry * sl_pct, 2)
    put_sl = round(put_entry * sl_pct, 2)
    
    if spot:
        open_trades.loc[datetime, 'spot'] = spot
    
    open_trades.loc[datetime, 'strike'] = call_df.iloc[0]['Strike']
    
    open_trades.loc[datetime, 'call_entry_time'] = call_entry_time 
    open_trades.loc[datetime, 'call_entry'] = call_entry 
    # open_trades.loc[datetime, 'call_status'] = True
    
    open_trades.loc[datetime, 'put_entry_time'] = put_entry_time 
    open_trades.loc[datetime, 'put_entry'] = put_entry 
    open_trades.loc[datetime, 'status'] = 'CP'
    
    open_trades.loc[datetime, 'call_sl'] = call_sl 
    open_trades.loc[datetime, 'put_sl'] = put_sl 
    
    return open_trades, datetime


def sl_check(call_df, call_sl, call_entry_time, put_df, put_sl, put_entry_time):
    call_sl_df = call_df.loc[(call_df.index.time >= call_entry_time) & (call_df['Last'] >= call_sl)]
    put_sl_df = put_df.loc[(put_df.index.time >= put_entry_time) & (put_df['Last'] >= put_sl)]
    
    sl_dict = {}
    
    if (not call_sl_df.empty) and (not put_sl_df.empty):
        
        call_sl_time = call_sl_df.index[0]
        put_sl_time = put_sl_df.index[0]
        
        if call_sl_time < put_sl_time:
            call_actual_sl = call_sl_df.iloc[0]['Last']
            
            sl_dict['opt_type'] = 'C'
            sl_dict['sl_time'] = call_sl_time
            sl_dict['actual_sl'] = call_actual_sl
            
            return sl_dict
        
        else:
            put_actual_sl = put_sl_df.iloc[0]['Last']
            
            sl_dict['opt_type'] = 'P'
            sl_dict['sl_time'] = put_sl_time
            sl_dict['actual_sl'] = put_actual_sl
            
            return sl_dict
        
    if not call_sl_df.empty:
        call_sl_time = call_sl_df.index[0]
        call_actual_sl = call_sl_df.iloc[0]['Last']
        
        sl_dict['opt_type'] = 'C'
        sl_dict['sl_time'] = call_sl_time
        sl_dict['actual_sl'] = call_actual_sl
        
        return sl_dict
    
    elif not put_sl_df.empty:
        put_sl_time = put_sl_df.index[0]
        put_actual_sl = put_sl_df.iloc[0]['Last']
        
        sl_dict['opt_type'] = 'P'
        sl_dict['sl_time'] = put_sl_time
        sl_dict['actual_sl'] = put_actual_sl
        
        return sl_dict
    
    
    return False
        

def re_check_sl(DF, sl, start_time):
    
    df = DF.copy()
    exit_dict = {}
    temp_df = df.loc[(df.index.time > start_time) & (df['Last'] >= sl)]
    
    if not temp_df.empty:
        exit_dict['exit_time'] = temp_df.index[0]
        exit_dict['exit_price'] = temp_df.iloc[0]['Last']
        exit_dict['exit_type'] = 'SL'
        return exit_dict 
    
    else:
        
        try:
            exit_dict['exit_time'] = df.loc[(df.index.time >= pd.Timestamp('15:59:00').time())].index[0]
            exit_dict['exit_price'] = df.loc[(df.index.time >= pd.Timestamp('15:59:00').time())].iloc[0]['Last']
        
        except:
            exit_dict['exit_time'] = df.index[-1]
            exit_dict['exit_price'] = df.iloc[-1]['Last']
        
        # exit_dict['exit_time'] = df.loc[(df.index.time >= pd.Timestamp('15:59:00').time())].index[0]
        # exit_dict['exit_price'] = df.loc[(df.index.time >= pd.Timestamp('15:59:00').time())].iloc[0]['Last']
        exit_dict['exit_type'] = 'MKT Closing'
        
        return exit_dict
        
def exit_straddle(DF, exit_time = pd.Timestamp('15:59:00').time()):
    
    df = DF.copy()
    exit_dict = {}
    
    try:
        exit_price = df.loc[df.index.time >= exit_time].iloc[0]['Last']
        exit_time = df.loc[df.index.time >= exit_time].index[0]
    except:
        exit_price = df.iloc[-1]['Last']
        exit_time = df.index[-1]
    
    exit_dict['exit_price'] = exit_price 
    exit_dict['exit_time'] = exit_time 
    
    return exit_dict


def straddle_re_entry(ticker, spot_df, sl_pct, start_time = pd.Timestamp('09:30:00').time()):
    
    half_day = ['2016-11-25','2017-07-03', '2017-11-24', '2018-11-23', '2019-11-29', '2019-12-24', '2020-11-27', '2020-12-24', '2021-11-26', '2018-12-24']  
        
    date_list = spot_df['Date'].unique()
    tradebook = pd.DataFrame()      
    exception = {}
    
    for i, date in enumerate(date_list):
        # break
        # if i == 10:
            # break
        
        day_spot = spot_df[['Date', 'Time', 'Last']].loc[spot_df['Date'] == date].reset_index(drop=True)
        
        spot = day_spot.iloc[0]['Last']
        strike = round(spot, 0)
        
        expiry = [i for i in exp['Date'] if i.date() >= date][0]
        
        try:
            
            if date.strftime('%Y-%m-%d') in half_day:
                call_df = opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type='C', Date = date)).between_time('09:30', '13:00')
                put_df = opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type='P', Date = date)).between_time('09:30', '13:00')
            else:
                call_df = opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type='C', Date = date)).between_time('09:30', '16:00')
                put_df = opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type='P', Date = date)).between_time('09:30', '16:00')
            
        except Exception as e:
            print(e)
            continue
            
        
        if (not call_df.empty) and (not put_df.empty):
            try:
                open_trades, datetime = open_trade(date, strike, call_df, put_df, sl_pct, spot=spot)
            except:
                continue
                        
        else:
            print(f'no entry for : date : {date}, expiry : {expiry}, strike : {strike}')
            exception[f'{date}'] = f'date : {date}, expiry : {expiry}, strike : {strike}'
            continue
        
        call_sl = open_trades.loc[datetime, 'call_sl']
        put_sl = open_trades.loc[datetime, 'put_sl']
        
        call_entry_time = open_trades.loc[datetime, 'call_entry_time']
        put_entry_time = open_trades.loc[datetime, 'put_entry_time']
        
        check_sl = sl_check(call_df, call_sl, call_entry_time, put_df, put_sl, put_entry_time)
        
        if not check_sl:
            call_exit = exit_straddle(call_df)
            put_exit = exit_straddle(put_df)
            
            open_trades.loc[datetime, 'call_exit'] = call_exit['exit_price']
            open_trades.loc[datetime, 'call_exit_time'] = call_exit['exit_time']
    
            open_trades.loc[datetime, 'put_exit'] = put_exit['exit_price']
            open_trades.loc[datetime, 'put_exit_time'] = put_exit['exit_time']
    
            tradebook = pd.concat([tradebook, open_trades])
        
        
        while check_sl:
            
            if check_sl['opt_type'] == 'C':
                open_trades.loc[datetime, 'status'] = 'P'
                open_trades.loc[datetime, 'call_exit'] = check_sl['actual_sl']
                open_trades.loc[datetime, 'call_exit_time'] = check_sl['sl_time']
                
                sl = open_trades.loc[datetime, 'put_entry']
                
                sl_hit_time = check_sl['sl_time']
                sl_hit_price = check_sl['actual_sl']
                
                exit_dict = re_check_sl(put_df, sl, start_time = sl_hit_time.time())
                
                open_trades.loc[datetime, 'put_exit'] = exit_dict['exit_price']
                open_trades.loc[datetime, 'put_exit_time'] = exit_dict['exit_time']
                if exit_dict['exit_type'] == 'SL':
                    open_trades.loc[datetime, 'status'] = None
                
                tradebook = pd.concat([tradebook, open_trades])
                
            elif check_sl['opt_type'] == 'P':
                
                open_trades.loc[datetime, 'status'] = 'C'
                open_trades.loc[datetime, 'put_exit'] = check_sl['actual_sl']
                open_trades.loc[datetime, 'put_exit_time'] = check_sl['sl_time']
                
                sl = open_trades.loc[datetime, 'call_entry']
                
                sl_hit_time = check_sl['sl_time']
                sl_hit_price = check_sl['actual_sl']
                
                exit_dict = re_check_sl(call_df, sl, start_time = sl_hit_time.time())
                
                open_trades.loc[datetime, 'call_exit'] = exit_dict['exit_price']
                open_trades.loc[datetime, 'call_exit_time'] = exit_dict['exit_time']
                
                if exit_dict['exit_type'] == 'SL':
                    open_trades.loc[datetime, 'status'] = None
                
                tradebook = pd.concat([tradebook, open_trades])
                
            
            
            try:
                spot = day_spot.loc[day_spot['Time'] >= sl_hit_time.time()].iloc[0]['Last']
            except:
                break
            
            strike = round(spot, 0)
            try:
                if date.strftime('%Y-%m-%d') in half_day:
                    call_df = opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type='C', Date = date)).between_time('09:30', '13:00')
                    put_df = opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type='P', Date = date)).between_time('09:30', '13:00')
                else:
                    call_df = opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type='C', Date = date)).between_time('09:30', '16:00')
                    put_df = opt.frwd_fill(opt.read_df(ticker, strike = strike, expiry = expiry, opt_type='P', Date = date)).between_time('09:30', '16:00')
            
            except Exception as e:
                print(e)
                break
            
            if (not call_df.empty) and (not put_df.empty):
                try:
                    open_trades, datetime = open_trade(date, strike, call_df, put_df, sl_pct, start_time = sl_hit_time.time(), spot=spot)
                except:
                    break
                        
            else:
                print('no entry after sl : date : {date}, expiry : {expiry}, strike : {strike}')
                exception[f'{date}_sl'] = f'date : {date}, expiry : {expiry}, strike : {strike}'
                break
                
            
            call_sl = open_trades.loc[datetime, 'call_sl']
            put_sl = open_trades.loc[datetime, 'put_sl']
            
            call_entry_time = open_trades.loc[datetime, 'call_entry_time']
            put_entry_time = open_trades.loc[datetime, 'put_entry_time']
            
            check_sl_try = sl_check(call_df, call_sl, call_entry_time, put_df, put_sl, put_entry_time)
            
            if not check_sl_try:
                
                call_exit = exit_straddle(call_df)
                put_exit = exit_straddle(put_df)
                
                open_trades.loc[datetime, 'call_exit'] = call_exit['exit_price']
                open_trades.loc[datetime, 'call_exit_time'] = call_exit['exit_time']
    
                open_trades.loc[datetime, 'put_exit'] = put_exit['exit_price']
                open_trades.loc[datetime, 'put_exit_time'] = put_exit['exit_time']
    
                tradebook = pd.concat([tradebook, open_trades])
    
            check_sl = check_sl_try
    
    tradebook['Call_pl'] = tradebook['call_entry'] - tradebook['call_exit']
    tradebook['Put_pl'] = tradebook['put_entry'] - tradebook['put_exit']

    tradebook['PL'] = tradebook['Call_pl'] + tradebook['Put_pl']
    return tradebook



''' diff SLs '''
tickers = ['SPY', 'QQQ']

ticker = 'SPY'

sl_pct_list = [1.50, 1.55, 1.60, 1.65, 1.70, 1.75, 1.80, 1.85, 1.90, 1.95, 2.00]

result = {}
points = {}


for ticker in tickers:
    spot_df, resamp = fut.outliers_resampled(fut.get_spot(ticker))
    half_day = ['2016-11-25','2017-07-03', '2017-11-24', '2018-11-23', '2019-11-29', '2019-12-24', '2020-11-27', '2020-12-24', '2021-11-26', '2018-12-24']  
    date_list = spot_df['Date'].unique()
    if ticker == 'QQQ':
        spot_df = spot_df.loc[spot_df['Datetime'].dt.year >= 2012].reset_index(drop=True)
    
for sl_pct in sl_pct_list[1:]:
    # break
    tradebook = straddle_re_entry(ticker, spot_df, sl_pct = sl_pct)
    
    result[f'{sl_pct}'] = tradebook
    points[f'{sl_pct}'] = tradebook['PL'].sum()
    
    print(sl_pct)
    





tradebook['PL'].sum()

mdf = pd.pivot_table(tradebook, values='PL', index = tradebook.index.year, columns = tradebook.index.month, aggfunc='sum')
counts = pd.pivot_table(tradebook, values='PL', index = tradebook.index.year, columns = tradebook.index.month, aggfunc='count')

mdf.sum(axis=1)
counts.sum(axis=1)

mdf.sum(axis=0)
counts.sum(axis=0)

tradebook.to_csv('D:\\Coding\\straddle_with_sl_entry_again\\straddle_with_sl_entry_again.csv')


np_sl = tradebook.loc[(tradebook['status'] == 'C')]
nc_sl = tradebook.loc[(tradebook['status'] == 'P')]

tdf = tradebook.fillna(0)
both_sl = tdf.loc[tdf['status'] == 0]

no_sl = tradebook.loc[tradebook['status'] == 'CP']

def profit_loss_number(DF):
    
    df = DF.copy()
    
    pdf = df.loc[df['PL'] > 0]
    ldf = df.loc[df['PL'] <= 0]
    
    profit = len(df.loc[df['PL'] > 0])
    loss = len(df.loc[df['PL'] <= 0])
    
    return profit, loss, [pdf, ldf]

profit, loss, dfs = profit_loss_number(tradebook)

dfs[1]['PL'].mean()










































