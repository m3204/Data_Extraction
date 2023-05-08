# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 23:19:52 2023

@author: Dell
"""

import pandas as pd
import numpy as np 
from MRK import *


fut = Spot_Fut()
opt = Option_Data()

exp_dates = opt.get_expiry('SPY')

exp_date = exp_dates[463:].reset_index(drop=True)

main_df = pd.DataFrame()
# ticker = 'SPY'
def get_opt_first_leg_data(ticker, expiry, opt_type, Date):
    df1 = opt.read_df(ticker, expiry=expiry, opt_type=opt_type, Date=Date)
    df1 = opt.get_time_wise_data(df1, time_range=['155500', '160000'])
    df1 = df1.loc[(df1['Last'].between(0.08, 0.12)) | (df1['Open'].between(0.08, 0.12))].reset_index(drop=True)
    
    # conditions = [df1['Last'] == 0.10, 
    #               (df1['Last'] == 0.11) & ~(df1['Last'] == 0.10),
    #               (df1['Last'] == 0.09) & ~(df1['Last'] == 0.11) & ~(df1['Last'] == 0.10),
    #               (df1['Last'] == 0.12) & ~(df1['Last'] == 0.09) & ~(df1['Last'] == 0.11) & ~(df1['Last'] == 0.10)]
    
    values = [0.10, 0.11, 0.09, 0.12, 0.08]
    
    for val in values:
        # break
        if not df1.loc[df1['Last'] == val].empty:
            if opt_type == 'C':
                final_leg = df1.loc[df1['Last'] == val].sort_values(by=['Strike'], ascending=False).reset_index(drop=True).iloc[0:1]
                return final_leg
            else:
                final_leg = df1.loc[df1['Last'] == val].sort_values(by=['Strike'], ascending=True).reset_index(drop=True).iloc[0:1]
                return final_leg

    
def get_opt_second_leg_data(ticker, expiry, opt_type, Date):
    df1 = opt.read_df(ticker, expiry=expiry, opt_type=opt_type, Date=Date)
    df1 = opt.get_time_wise_data(df1, time_range=['155500', '160000'])
    df1 = df1.loc[(df1['Last'].between(0.03, 0.06)) | (df1['Open'].between(0.03, 0.06))].reset_index(drop=True)
    
    values = [0.05, 0.04, 0.06, 0.03]
    
    # if opt_type == 'C':
    #     df1 = df1.sort_values(by=['Strike'], ascending=True)
    # else:
    #     df1 = df1.sort_values(by=['Strike'], ascending=False)
    
    # return df1.reset_index(drop=True).iloc[0:1]
    
    for val in values:
        # break
        if not df1.loc[df1['Last'] == val].empty:
            if opt_type == 'C':
                final_leg = df1.loc[df1['Last'] == val].sort_values(by=['Strike'], ascending=True).reset_index(drop=True).iloc[0:1]
                return final_leg
            else:
                final_leg = df1.loc[df1['Last'] == val].sort_values(by=['Strike'], ascending=False).reset_index(drop=True).iloc[0:1]
                return final_leg

    
    
main_df = pd.DataFrame()

for i in range(len(exp_date) - 1):
    
    expiry1 = exp_date.loc[i, 'Date']
    expiry2 = exp_date.loc[i+1, 'Date']
    
    date_range = pd.date_range(expiry1, expiry2)
    
    call1 = get_opt_first_leg_data('SPY', expiry=expiry2, opt_type='C', Date=expiry1)
    put1 = get_opt_first_leg_data('SPY', expiry=expiry2, opt_type='P', Date=expiry1)
    
    # short_call = call_df.iloc[0]
    # short_put = put_df.iloc[0]
    call2 = get_opt_second_leg_data('SPY', expiry=expiry2, opt_type='C', Date=expiry1)
    put2 = get_opt_second_leg_data('SPY', expiry=expiry2, opt_type='P', Date=expiry1)
        
    
    try:
        merge1 = pd.merge(call1, put1, on=['Expiry', 'Datetime'], suffixes=['_C1', '_P1'], how='outer').reset_index(drop=True).fillna(method='ffill').dropna()
        merge2 = pd.merge(call2, put2, on=['Expiry', 'Datetime'], suffixes=['_C2', '_P2'], how='outer').reset_index(drop=True).fillna(method='ffill').dropna()
    except:
        print(f'exp : {expiry2}, date : {expiry1}')
        continue
        
        
    try:
        
        short_pair1 = merge1.iloc[0][['Datetime', 'Expiry', 'Strike_C1', 'Open_C1', 'Last_C1', 'Strike_P1', 'Open_P1', 'Last_P1']]
        short_pair2 = merge2.iloc[0][['Datetime', 'Expiry', 'Strike_C2', 'Open_C2', 'Last_C2', 'Strike_P2',  'Open_P2','Last_P2']]
        
        short_merge = pd.merge(pd.DataFrame([short_pair1]), pd.DataFrame([short_pair2]), on=['Datetime', 'Expiry'], how='outer').reset_index(drop=True).fillna(method='ffill').dropna()
        
    except:
        continue
    main_df = pd.concat([main_df, short_merge], ignore_index=True) 
    strike_call1 = short_pair1['Strike_C1']
    strike_put1 = short_pair1['Strike_P1']
    strike_call2 = short_pair2['Strike_C2']
    strike_put2 = short_pair2['Strike_P2']
    
    
    for date in date_range[1:]:
        # break
        call_df1 = opt.frwd_fill(opt.read_df('SPY', strike=strike_call1, expiry=expiry2, opt_type='C', Date=date))
        put_df1 = opt.frwd_fill(opt.read_df('SPY', strike=strike_put1, expiry=expiry2, opt_type='P', Date=date))
                
        call_df2 = opt.frwd_fill(opt.read_df('SPY', strike=strike_call2, expiry=expiry2, opt_type='C', Date=date))
        put_df2 = opt.frwd_fill(opt.read_df('SPY', strike=strike_put2, expiry=expiry2, opt_type='P', Date=date))
                
        
        try:
            merge_df1 = pd.merge(call_df1, put_df1, on=['Expiry', 'Datetime'], suffixes=['_C1', '_P1'], how='outer').fillna(method='ffill').reset_index()
            merge_df2 = pd.merge(call_df2, put_df2, on=['Expiry', 'Datetime'], suffixes=['_C2', '_P2'], how='outer').fillna(method='ffill').reset_index()
            
            merge_df1.fillna(0, inplace=True)
            merge_df2.fillna(0, inplace=True)
            
        except:
            print(f'date : {date}, exp : {expiry2}')
            continue
        

        pair1 = merge_df1[['Datetime', 'Expiry', 'Strike_C1', 'Open_C1', 'Last_C1', 'Strike_P1', 'Open_P1', 'Last_P1']]
        pair2 = merge_df2[['Datetime', 'Expiry', 'Strike_C2', 'Open_C2', 'Last_C2', 'Strike_P2', 'Open_P2', 'Last_P2']]
        pair_merge = pd.merge(pair1, pair2, on=['Datetime', 'Expiry'], how='outer').reset_index(drop=True).fillna(method='ffill').dropna()

        main_df = pd.concat([main_df, pair_merge], ignore_index=True) 
    
    
    print(i)
    # if i >=10:
    #     break
    
    
main_df.to_csv('D:\\Coding\\Extracted Data\\iron_condor_0.08_to_0.12_next_exp.csv', index=False)   

'''
314 exp : 2020-02-26 00:00:00, date : 2020-02-24 00:00:00
320 exp : 2020-03-11 00:00:00, date : 2020-03-09 00:00:00
321 exp : 2020-03-13 00:00:00, date : 2020-03-11 00:00:00
322 exp : 2020-03-16 00:00:00, date : 2020-03-13 00:00:00
323 exp : 2020-03-18 00:00:00, date : 2020-03-16 00:00:00
435 exp : 2020-11-30 00:00:00, date : 2020-11-27 00:00:00
447 exp : 2020-12-28 00:00:00, date : 2020-12-24 00:00:00
490 exp : 2021-04-07 00:00:00, date : 2021-04-05 00:00:00
525 exp : 2021-06-28 00:00:00, date : 2021-06-25 00:00:00
543 exp : 2021-08-09 00:00:00, date : 2021-08-06 00:00:00
554 exp : 2021-09-03 00:00:00, date : 2021-09-01 00:00:00
592 exp : 2021-11-29 00:00:00, date : 2021-11-26 00:00:00
608 exp : 2022-01-05 00:00:00, date : 2022-01-03 00:00:00
750 exp : 2022-11-28 00:00:00, date : 2022-11-25 00:00:00
'''




''' BACKTEST '''

df = pd.read_csv('D:\\Coding\\Extracted Data\\iron_condor_0.08_to_0.12_next_exp.csv')

df['Short'] = df['Last_C1'] + df['Last_P1']
df['Long'] = df['Last_C2'] + df['Last_P2']


df['Total'] = df['Short'] - df['Long']

df['Datetime'] = pd.to_datetime(df['Datetime'])
df['Expiry'] = pd.to_datetime(df['Expiry']).dt.date

entry_dates = df['Expiry'].unique()

entry_points = df.loc[(df['Datetime'].dt.date.isin(entry_dates)) & (df['Datetime'].dt.time >= pd.Timestamp('15:55:00').time()) & (df['Datetime'].dt.date != df['Expiry'])]

def time_to_sell(cp, current_time, current_date, entry,target=0, sl_mult=2,  last_time_to_sell= pd.Timestamp('15:59:00').time()):
    bp= entry['Total']
    sl= bp*sl_mult
    
    if cp>=sl:
        return True
    elif cp<=target:
        return True
    elif current_time==last_time_to_sell and entry['Expiry']== current_date:
        return True
    else: 
        return False


def strngl_exit(current_price, current_time, sl, tgt, last_time =pd.Timestamp('16:00:00').time()):
    
    if current_time >= last_time:
        return True
    
    if sl:
    
        if current_price >= sl:
            return True 
    
    if tgt:
    
        if current_price <= tgt:
            return True 
    
    return False

# transactions= []
# df= df.sort_values('Datetime')
# for i, ep in entry_points.iterrows():
#     temp_df= df[(df['Expiry']== ep['Expiry']) & (df['Datetime']>ep['Datetime'])].reset_index(drop=True)
#     found=False
#     for i,exp in temp_df.iterrows(): 
        
#         if time_to_sell(exp['Total'],exp['Datetime'].time(),exp['Datetime'].date(),entry= ep):
# #             print ('in')
#             transactions.append((ep, exp))
#             found=True
#             break
#     if not found:
#         print ('no match')
#         transactions.append((ep, exp))


groups = df.groupby(df['Expiry'])

group = groups.get_group(pd.Timestamp('2018-03-26').date())

def irncondor_bt(group):
    group = group.copy()
    
    group.reset_index(drop=True, inplace=True)
    
    dfs = pd.DataFrame()
    group = group.copy()
    
    group = group.loc[(group['Datetime'].dt.time >= pd.Timestamp('09:30:00').time()) &
                      (group['Datetime'].dt.time <= pd.Timestamp('16:00:00').time())].dropna()
    
    group.reset_index(drop=True, inplace=True)
    
    date = group['Datetime'].dt.date.at[0]
    
    dfs.loc[date, 'start_time'] = group.loc[0, 'Datetime']
    dfs.loc[date, 'Opening'] = group.loc[0, 'Total']
    
    dfs.loc[date, 'end_time'] = group.iloc[-1]['Datetime']
    dfs.loc[date, 'Closing'] = group.iloc[-1]['Total']
    
    dfs.loc[date, 'max_time'] = group.loc[group['Total'].idxmax(), 'Datetime']
    dfs.loc[date, 'Max'] = group['Total'].max()
    
    dfs.loc[date, 'min_time'] = group.loc[group['Total'].idxmin(), 'Datetime']
    dfs.loc[date, 'Min'] = group['Total'].min()
    
    return dfs

dfss = groups.apply(irncondor_bt).reset_index().drop('level_1', axis=1)

dfss['PL'] = dfss['Opening'] - dfss['Closing']

dfss['PL'].sum() / len(dfss)


mdf = pd.pivot_table(dfss, values='PL', index=dfss['start_time'].dt.year, columns = dfss['start_time'].dt.month, aggfunc='sum')
mdf.sum(axis=1)



def strngl_exit(current_price, current_time, sl, tgt, last_time =pd.Timestamp('16:00:00').time()):
    
    if current_time >= last_time:
        return True
    
    elif current_price >= sl:
        return True 
    
    elif current_price <= tgt:
        return True 
    
    return False


























