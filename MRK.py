# -*- coding: utf-8 -*-
"""
Created on Thu Dec 29 17:18:32 2022

@author: Dell
"""

import pandas as pd
import datetime as dt
import os
import numpy as np
import glob
import pickle

# def get_spot(ticker):
    
#     file_name = os.listdir(f'D:\\Data\\Index_Future\\{ticker}')[0]
    
#     df = pd.read_csv(f'D:\Data\Index_Future\{ticker}\\{file_name}')
    

class Spot_Fut(object):
    
    def get_spot(self, ticker):
        df = pd.read_parquet(f'D:\\Spot_Fut\\SPOT\\{ticker}_SPOT.parquet')
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        df['Date'] = df['Datetime'].dt.date
        df['Time'] = df['Datetime'].dt.time
        return df.loc[(df['Time'] >= pd.to_datetime('09:30:00').time()) & (df['Time'] <= pd.to_datetime('16:00:00').time())].reset_index(drop=True)
        
    
    def get_fut(self, ticker):
        df = pd.read_parquet(f'D:\\Spot_Fut\\Index_Future\\{ticker}\\{ticker}_Fut.parquet')
        # df['Date'] = df['Datetime'].dt.date
        # df['Time'] = df['Datetime'].dt.time
        return df
    
    def day_df(self, ticker, timeframe=None, agg_func = {'Date' : 'first', 'Open':'first','High':'max','Low': 'min','Last' : 'last', 'Volume':'sum'}):
        df = pd.read_parquet(f'D:\\Spot_Fut\\1 DAY\\{ticker}.parquet')
        
        df['Date'] = pd.to_datetime(df['Date'])
        
        if timeframe != None:
        # df.set_index('Date', inplace=True)
            df.index = df['Date']
            
            try:
                df.rename(columns = {'Close' : 'Last'}, inplace=True)
            except:
                pass
            
            df.columns
            df = df.resample(timeframe, label='right', origin='start').agg(agg_func).dropna()
            
            return df.rename_axis('Date_last').reset_index()
        
        else:
            return df
    
    # assigning some required variables as constructer
    def __init__(self):
        pass
        # self.df = pd.read_csv("C:\\Users\\Dell\\Desktop\\BANKNIFTY_1minute.csv")
        # self.df.loc[:,'Date'] = pd.to_datetime(self.df.Date.astype(str)+' '+self.df.Time.astype(str))
        # self.df.drop('Time', axis=1, inplace=True)
        # self.df.set_index('Date', inplace=True)
        
        ''' https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
        reference link to provide start date, interval, end date in required format
        '''
        
        
        # self.time = time
        # self.start = pd.to_datetime(start)
        # self.end = pd.to_datetime(end)
    
    
    # converting data in demanded time frame and returning it
    def convert_interval(self, DF, interval, sec_type = None, custom_origin = None, agg_func={'Time' : 'last', 'Date' : 'last','Open':'first','High':'max','Low': 'min','Last' : 'last', 'Volume':'sum'}):
        df = DF.copy()
        if sec_type == 'Spot':
            try:
                df.loc[:,'Datetime'] = pd.to_datetime(df.Date.astype(str)+' '+df.Time.astype(str))
            except:
                pass
            # df.drop('Time', axis=1, inplace=True)
            
            # df.set_index('Datetime', inplace=True)
            def convert(grp):
                grp = grp.loc[(grp['Time'] >= pd.to_datetime('09:30:00').time()) & (grp['Time'] <= pd.to_datetime('16:00:00').time())]
                
                grp.set_index('Datetime', inplace=True)
                
                grp = grp.resample(interval, origin='start').agg(agg_func)
                
                return grp.reset_index()
            
            
            # df_grp = df.groupby(pd.Grouper(freq=interval)).agg(agg_func)
            
            day_df = df.groupby(df['Date'])
            
            
            
            return pd.DataFrame(day_df.apply(convert)).reset_index(drop=True)
        elif sec_type == 'Fut':
            
            origin = '17:00:00'
            
            if custom_origin != None:
                origin = custom_origin
            
            
            # start_time = origin
            # end_time = end_time
            
            try:
                df.set_index('Datetime', inplace=True)
            except:
                pass
            
            
            if agg_func == {'Time' : 'last', 'Date' : 'last','Open':'first','High':'max','Low': 'min','Last' : 'last', 'Volume':'sum'}:
                agg_func = {'Time' : 'last', 'Date' : 'last','Open':'first','High':'max','Low': 'min','Last' : 'last', 'Volume':'sum', 'No. Trades' : 'sum', 'RIC' : 'first'}
            
            def convert(group):
                group = group.resample(rule = interval, origin='start').agg(agg_func)
                
                return group.reset_index()
            
            # if end_time != None:
            #     groups = df.groupby(pd.Grouper(freq='24H', origin=origin, label='right', dropna=True, Closed = 'right')).apply(lambda x : x.between_time(start_time, end_time))
            #     groups = groups.reset_index(level=[1])
            #     groups.reset_index(inplace=True, drop=True)
            #     groups.set_index('Datetime', inplace=True)
            #     groups = groups.groupby(pd.Grouper(freq='24H', origin=origin, label='right', dropna=True, closed = 'right'))
                
            # else:
            groups = df.groupby(pd.Grouper(freq='24H', origin=origin, label='right', dropna=True))
            
            return pd.DataFrame(groups.apply(convert)).dropna().reset_index(drop=True)
    
            
    #  returning filter data
    def get_datewise_data(self, DF, start=None,  end=None, interval=None, date=None):
        data = DF.copy()
        
        

        if interval != None:
            data = self.convert_interval(data, interval)
        
        # elif interval == None:
        #     try:
        #         data['Datetime'] = pd.to_datetime(data['Date'].astype(str) + ' ' + data['Time'].astype(str))
        #         # date = data.set_index('Datetime')
        #     except:
        #         pass
        try:
            data['Datetime'] = pd.to_datetime(data['Datetime'])
        except:
            pass

        if end==None and start != None:
            start_date = pd.to_datetime(start).date()
            return data.loc[data.Datetime.dt.date >= start_date].reset_index(drop=True)
        elif end != None and start != None:
            start_date = pd.to_datetime(start).date()
            end_date = pd.to_datetime(end).date()
            return data.loc[(data.Datetime.dt.date >= start_date) & (data.Datetime.dt.date <= end_date)].reset_index(drop=True)
        elif end != None and start == None:
            end_date = pd.to_datetime(end).date()
            return data.loc[(data.Datetime.dt.date <= end_date)].reset_index(drop=True)
        
        if date != None:
            date = pd.Timestamp(date).date()
            return data.loc[data.Datetime.date == date].reset_index(drop=True)
            

    def convert_daywise_interval_data(self, DF, dayname, interval='1t', agg_func={'Time' : 'first', 'Date' : 'first', 'WeekDay' : 'first','Open':'first','High':'max','Low': 'min','Last' : 'last', 'Volume':'sum'}):
        df = DF.copy()
        try:
            df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
        except:
            pass
        
        df.insert(1, 'WeekDay', df['Datetime'].dt.day_name())
        
        if type(dayname) == list:
            day_df = df.loc[df['WeekDay'].isin(dayname)]
        else:
            day_df = df.loc[df['WeekDay'] == dayname]
        
        day_df.set_index('Datetime', inplace=True)

        day_df = day_df.groupby(pd.Grouper(freq=interval)).agg(agg_func)
        return day_df.dropna()       

  
    
    def get_timely_data(self, DF, time=None, time_range=None, start_time=None):
        df = DF.copy()
        
        try:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
        except:
            pass
        
        try:
            df['Time'] = pd.to_datetime(df['datetime']).dt.time
        except:
            df['Time'] = pd.to_datetime(df['Datetime']).dt.time
        
        if time_range != None:
            try:
                try:
                    time = (pd.to_datetime(pd.Series(time_range), format='%I%M%p')).dt.time
                    
                except:
                    time = (pd.to_datetime(pd.Series(time_range), format='%H%M%S')).dt.time
            except ValueError:
                print('Provided Time Format Error.... Please Input Time in Right Format... ')
            # time = (pd.to_datetime(pd.Series(time_range), format='%I%M%p')).dt.time
            
            time_df = df.loc[(df['Time'] >= time[0]) & (df['Time'] <= time[1])]
        elif start_time != None:
            try:
                try:
                    start_time = pd.to_datetime(pd.Series(time), format='%I%M%p').dt.time[0]
                    
                except:
                    start_time = pd.to_datetime(pd.Series(time), format='%H%M%S').dt.time[0]
            except ValueError:
                print('Provided Time Format Error.... Please Input Time in Right Format... ')
            
            # time_df = pd.DataFrame(columns=df.columns)
            # for date in df.Date.unique():
            #     value = df[df['Date'] == date].loc[df.Time == time]
            #     if value.empty:
            #         value = df[df['Date'] == date].loc[df.Time >= time].iloc[0]
            #     time_df = pd.concat([time_df, value])
            
            time_df = df.loc[(df['Time'] >= start_time)]
            
        elif time != None:
            try:
                try:
                    time = pd.to_datetime(pd.Series(time), format='%I%M%p').dt.time[0]
                    
                except:
                    time = pd.to_datetime(pd.Series(time), format='%H%M%S').dt.time[0]
            except ValueError:
                print('Provided Time Format Error.... Please Input Time in Right Format... ')
            
            # time_df = pd.DataFrame(columns=df.columns)
            # for date in df.Date.unique():
            #     value = df[df['Date'] == date].loc[df.Time == time]
            #     if value.empty:
            #         value = df[df['Date'] == date].loc[df.Time >= time].iloc[0]
            #     time_df = pd.concat([time_df, value])
            
            time_df = df.loc[(df['Time'] == time)]
            
        return time_df.reset_index(drop=True)
    def outliers_removed(self, DF):
        df = DF.copy()

        df['oh'] = (df['High']/df['Open']) - 1
        df['ol'] = (df['Low'] / df['Open']) - 1
        df['oc'] = (df['Last'] / df['Open']) - 1


        mask = ((df['oh'].between(-0.05, 0.05)) & (df['ol'].between(-0.05, 0.05)) & (df['oc'].between(-0.05, 0.05)))

        outlier = df.drop(df[mask].index)
        date_list = outlier['Date'].unique().tolist()
        
        df = df.loc[~df['Date'].isin(date_list)]
        return df, date_list

    def outliers_resampled(self, DF):
        df = DF.copy()

        df['oh'] = (df['High']/df['Open']) - 1
        df['ol'] = (df['Low'] / df['Open']) - 1
        df['oc'] = (df['Last'] / df['Open']) - 1

        removed = df[~(df['oh'].between(-0.05, 0.05)) | ~ (df['ol'].between(-0.05, 0.05)) | ~(df['oc'].between(-0.05, 0.05))]

        df = df[(df['oh'].between(-0.05, 0.05)) & (df['ol'].between(-0.05, 0.05)) & (df['oc'].between(-0.05, 0.05))]

        def group_loop(group):
            group.set_index('Datetime', inplace=True)
            group = group.resample('1t').ffill().between_time(start_time='09:30', end_time='16:00')
            group = group.drop(['oh', 'ol', 'oc'], axis=1).reset_index()
            return group
        
        groups = df.groupby('Date')
        cdf = groups.apply(group_loop).reset_index(drop=True)

        return cdf, removed

class Option_Data(object):
    
    def encoder(self, ticker, strike, opt_type, expiry):
        
        strike = str(round(int(float(strike)*100), 5)).zfill(5)[:5]
        # strike.zfill(5)
        call_month = {'01' : 'A', '02':'B', '03':'C', '04':'D', '05':'E', '06':'F', '07':'G', '08':'H', '09':'I', '10':'J', '11':'K', '12':'L'}
        put_month = {'01' : 'M', '02':'N', '03':'O', '04':'P', '05':'Q', '06':'R', '07':'S', '08':'T', '09':'U', '10':'V', '11':'W', '12':'X'} 
        
        if type(expiry) != str:
            try:
                expiry  = pd.to_datetime(expiry).date()
            except:
                pass
            expiry = expiry.strftime('%Y-%m-%d')
        
        # expiry_date = pd.to_datetime(expiry_date)
        month = expiry[5:7]
        day = expiry[8:10]
        year = expiry[2:4]
        
        if opt_type == 'C':
            if expiry[4] == '-':
                month_code = call_month[month]
                
            else:
                month = expiry[4:6]
                year = expiry[2:4]
                day = expiry[6:8]
                month_code = call_month[month]
            return f'{ticker}{month_code}{day}{year}{strike}'
            
        elif opt_type == 'P':
            if expiry[4] == '-':
                month_code = put_month[month]
                
            else:
                month = expiry[4:6]
                year = expiry[2:4]
                day = expiry[6:8]
                month_code = put_month[month]
            return f'{ticker}{month_code}{day}{year}{strike}'    
    
    def year_date_month_code(self, expiry=None, opt_type=None, strike=None):
        call_month = {'01' : 'A', '02':'B', '03':'C', '04':'D', '05':'E', '06':'F', '07':'G', '08':'H', '09':'I', '10':'J', '11':'K', '12':'L'}
        put_month = {'01' : 'M', '02':'N', '03':'O', '04':'P', '05':'Q', '06':'R', '07':'S', '08':'T', '09':'U', '10':'V', '11':'W', '12':'X'} 
        
        if type(expiry) != str:
            try:
                expiry  = pd.to_datetime(expiry).date()
            except:
                pass
            expiry = expiry.strftime('%Y-%m-%d')
        
        month = expiry[5:7]
        day = expiry[8:10]
        year = expiry[2:4]
        
        
        
        if opt_type == 'C':
            if expiry[4] == '-':
                month_code = call_month[month]
                
            else:
                month = expiry[4:6]
                year = expiry[2:4]
                day = expiry[6:8]
                month_code = call_month[month]
            return f'{month_code}{day}{year}'
            
        elif opt_type == 'P':
            if expiry[4] == '-':
                month_code = put_month[month]
                
            else:
                month = expiry[4:6]
                year = expiry[2:4]
                day = expiry[6:8]
                month_code = put_month[month]
            return f'{month_code}{day}{year}'
        elif opt_type == None:
            if expiry[4] == '-':
                month_codep = put_month[month]
                month_codec = call_month[month]
            else:
                month = expiry[4:6]
                year = expiry[2:4]
                day = expiry[6:8]
                month_codep = put_month[month]
                month_codec = call_month[month]
                
            return (f'{month_codec}{day}{year}', f'{month_codep}{day}{year}')

    
    def read_df(self, ticker, strike=None, expiry=None, opt_type=None, Date=None):   #, strike_range=None):
        try:
            if opt_type != None and strike != None and expiry != None and Date == None:
                ric = self.encoder(ticker, strike=strike, opt_type=opt_type, expiry=expiry)
                df = pd.read_csv(f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\{ric}.csv')
                return df
            
            
            elif opt_type == None and strike != None and Date == None and expiry != None:
                ric_call = self.encoder(ticker, strike, opt_type='C', expiry=expiry)
                ric_put = self.encoder(ticker, strike, opt_type='P', expiry=expiry)
                try:
                    cdf = pd.read_csv(f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\{ric_call}.csv')
                    # cdf['Date'] = pd.to_datetime(df['Datetime']).dt.date
                    # cdf['Time'] = pd.to_datetime(df['Datetime']).dt.time
                    pdf = pd.read_csv(f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\{ric_put}.csv')
                    
                    df = pd.concat([cdf, pdf], ignore_index=True)
                    return df
                    
                except Exception as e:
                    print(e)
                    print('please input confirmed expiry and strike... :)')
    
        
            elif strike == None and Date == None and expiry != None:
                if opt_type == None:
                    ric_c = self.year_date_month_code(expiry)[0]
                    ric_p = self.year_date_month_code(expiry)[1]
                    
                    path_c = f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\*{ric_c}*.csv'
                    dir_c = glob.glob(path_c)
                    
                    path_p = f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\*{ric_p}*.csv'
                    dir_p = glob.glob(path_p)
                    
                    dfc = pd.DataFrame()
                    dfp = pd.DataFrame()
                    
                    for c in dir_c:
                        cdf = pd.read_csv(c)
                        dfc = pd.concat([dfc, cdf], ignore_index=True)
                    for p in dir_p:
                        pdf = pd.read_csv(p)
                        dfp = pd.concat([dfp, pdf], ignore_index=True)
                        
                    df = pd.concat([dfc, dfp], ignore_index=True)
                    
                    return df
                elif opt_type != None:
                    ric = self.year_date_month_code(expiry, opt_type)
                    path = f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\*{ric}*.csv'
                    dire = glob.glob(path)
                    df = pd.DataFrame()
                    for i in dire:
                        dft = pd.read_csv(i)
                        df = pd.concat([df, dft], ignore_index=True)
                        
                        
                    return df
            elif strike != None and expiry == None:
                if opt_type == None:
                    strike = str(round(int(float(strike)*100), 5)).zfill(5)[:5]
                    path_s = f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\*{strike}.csv'
                    
                    dir_s = glob.glob(path_s)
                    
                    df = pd.DataFrame()
                    
                    for s in dir_s:
                        sdf = pd.read_csv(s)
                        df = pd.concat([df, sdf], ignore_index=True)
                    
                    if Date != None:
                        try:
                            Date = pd.to_datetime(Date).date()
                        except:
                            pass
                        date_df = df.loc[pd.to_datetime(df['Datetime']).dt.date == Date].reset_index(drop=True)
                        return date_df
                    elif Date == None:
                        return df
                    
                elif opt_type != None:
                    strike = str(round(int(float(strike)*100), 5)).zfill(5)[:5]
                    path_s = f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\*{strike}.csv'
                    
                    dir_s = glob.glob(path_s)
                    
                    df = pd.DataFrame()
                    
                    for s in dir_s:
                        sdf = pd.read_csv(s)
                        df = pd.concat([df, sdf], ignore_index=True)
                    
                    if Date != None:
                        try:
                            Date = pd.to_datetime(Date).date()
                        except:
                            pass
                        date_df = df.loc[pd.to_datetime(df['Datetime']).dt.date == Date].reset_index(drop=True)
                        date_df = date_df.loc[date_df['Opt_Type'] == opt_type]
                        return date_df
                    elif Date == None:
                        df = df.loc[df['Opt_Type'] == opt_type]
                        return df
                        
                        
                    
                
                
                
            elif Date != None:
                try:
                    Date = pd.to_datetime(Date).date()
                except:
                    pass
                if strike == None and expiry != None:
                    if opt_type == None:
                        ric_c = self.year_date_month_code(expiry)[0]
                        ric_p = self.year_date_month_code(expiry)[1]
                        
                        path_c = f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\*{ric_c}*.csv'
                        dir_c = glob.glob(path_c)
                        
                        path_p = f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\*{ric_p}*.csv'
                        dir_p = glob.glob(path_p)
                        
                        dfc = pd.DataFrame()
                        dfp = pd.DataFrame()
                        
                        for c in dir_c:
                            cdf = pd.read_csv(c)
                            dfc = pd.concat([dfc, cdf], ignore_index=True)
                        for p in dir_p:
                            pdf = pd.read_csv(p)
                            dfp = pd.concat([dfp, pdf], ignore_index=True)
                            
                        df = pd.concat([dfc, dfp], ignore_index=True)
                    
                    elif opt_type != None:
                        ric = self.year_date_month_code(expiry, opt_type)
                        path = f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\*{ric}*.csv'
                        dire = glob.glob(path)
                        df = pd.DataFrame()
                        for i in dire:
                            dft = pd.read_csv(i)
                            df = pd.concat([df, dft], ignore_index=True)
                            
                    date_df = df.loc[pd.to_datetime(df['Datetime']).dt.date == Date].reset_index(drop=True)
                    return date_df
                            
                
                    
                    
                    
                elif strike != None and expiry != None:
                    if opt_type == None:
                        ric_call = self.encoder(ticker, strike, opt_type='C', expiry=expiry)
                        ric_put = self.encoder(ticker, strike, opt_type='P', expiry=expiry)
                        try:
                            cdf = pd.read_csv(f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\{ric_call}.csv')
                            # cdf['Date'] = pd.to_datetime(df['Datetime']).dt.date
                            # cdf['Time'] = pd.to_datetime(df['Datetime']).dt.time
                            pdf = pd.read_csv(f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\{ric_put}.csv')
                            
                            df = pd.concat([cdf, pdf], ignore_index=True)
                            
                           
                            
                            
                        except Exception as e:
                            print(e)
                            print('please input confirmed expiry and strike... :)')
                    
                    elif opt_type != None:
                        ric = self.encoder(ticker, strike=strike, opt_type=opt_type, expiry=expiry)
                        df = pd.read_csv(f'D:\\Options RIC\\{ticker}\\{ticker} RIC\\{ric}.csv')
                        
                    
                    
                    date_df = df.loc[pd.to_datetime(df['Datetime']).dt.date == Date].reset_index(drop=True)
                    return date_df
        except Exception as e:

            print(e)
            print('some error in finding RIC Code')
            
            
             
    def get_strike_range(self, ticker, strike_range=None, strike_list= None, expiry=None, opt_type=None, Date=None):
        
        df = pd.DataFrame()
        if strike_range == None and strike_list != None:
            
            for s in strike_list:
                strike_df = self.read_df(ticker, strike = s, expiry=expiry, opt_type=opt_type, Date=Date)
                df = pd.concat([df, strike_df], ignore_index=True)
                
            return df
        
        elif strike_range != None and strike_list == None:
            
            strike_list = list(range(strike_range[0], strike_range[1]+1))
            
            for s in strike_list:
                strike_df = self.read_df(ticker, strike = s, expiry=expiry, opt_type=opt_type, Date=Date)
                df = pd.concat([df, strike_df], ignore_index=True)
                
            return df
        
    def frwd_fill(self, df, agg_func={'Datetime':'last','Expiry':'last', 'Strike' : 'last','Open':'first','High':'max','Low': 'min','Last' : 'last', 'Volume':'sum', 'Opt_Type' : 'last', 'RIC':'first'}):

        df = df.copy()
        df.index = pd.to_datetime(df['Datetime'])
        df = df.resample('1t').agg(agg_func)   
        df.ffill(inplace=True)
        df.drop(['Datetime'], axis=1, inplace=True)
        return df.between_time('09:30', '16:14')   
    
    def get_expiry(self, ticker='SPY'):
        df = pd.read_parquet(f'D:\\Options RIC\\{ticker}\\Expiry_Date.parquet')
        return df
        
    def get_time_wise_data(self, df, time=None, time_range=None, start_time=None):
        df = df.copy()
        
        if type(df.index) == pd.core.indexes.datetimes.DatetimeIndex:
            df.reset_index(inplace=True)
        
        try:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
        except:
            pass
        
        
            
        
        if time is not None:
            try:
                time = pd.to_datetime(time, format='%H%M%S').time()
            except:
                pass 
    
            df = df.loc[df['Datetime'].dt.time == time]
            return df
        
        if time_range is not None:
            try:
                time_range = pd.to_datetime(pd.Series(time_range), format='%H%M%S').dt.time
            except:
                pass

            df = df.loc[(df['Datetime'].dt.time >= time_range[0]) & (df['Datetime'].dt.time < time_range[1])]       
            
            return df
        
        elif start_time is not None:
            try:
                start_time = pd.to_datetime(start_time, format='%H%M%S').time()
            except:
                pass
            
            df = df.loc[df['Datetime'].dt.time >= start_time]
            return df
                            
                        
                    
                    
                
                    
                    
                    
                    
                    
            
            
    def convert_interval(self, DF, interval, agg_func={'WeekDay':'last','Expiry':'last', 'Strike' : 'last','Open':'first','High':'max','Low': 'min','Last' : 'last', 'Volume':'sum', 'Opt_Type' : 'last', 'RIC':'first'}):
        df = DF.copy()
        try:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df['Time'] = df['Datetime'].dt.time 
            df['Date'] = df['Datetime'].dt.date
        except:
            pass
        
        df.insert(1, 'WeekDay', df['Datetime'].dt.day_name())
        df.reset_index(drop=True, inplace=True)
        
        df.sort_values(by=['Strike','Date', 'Time'], inplace=True)
        
        df.set_index('Datetime', inplace=True)
        
        unq_date = df['Date'].unique()
        opt_typ = df['Opt_Type'].unique()
        
        dfd = pd.DataFrame()
        
        
        for i in range(len(unq_date)):
            for o in opt_typ:
                
                day_df = df.loc[(df['Date'] == unq_date[i]) & (df['Opt_Type'] == o)]
                
                day_df = day_df.resample(interval, origin='start').agg(agg_func)
                day_df.reset_index(inplace=True)
                
                dfd = pd.concat([dfd, day_df], ignore_index=True)
            
        
        # df_grp = df.groupby(pd.Grouper(freq=interval)).agg(agg_func)
        return dfd.dropna()
        
        

class Perfromance_Measure:
    
    
    def cagr(self, DF, factor=1):
        df = DF.copy()
        # df["ret"] = df['Adj Close'].pct_change()
        df['Cummulative_Return'] = (1+df["ret"]).cumprod()
        n = len(df)/(252*factor)
        CAGR = ((df['Cummulative_Return'].iloc[-1])**(1/n)) - 1 
        return CAGR
        
    
    
    
    def volatility(self, DF, factor=1):
        df = DF.copy()
    #     df['Return'] = df['Adj Close'].pct_change()
        vol = df['ret'].std() * np.sqrt(252*factor)
        # vol = (df['Last'].pct_change()).std()*np.sqrt(252*factor)
        
        return vol
    
    
    
    def sharpe(self, DF, rf):
        df = DF.copy()
        return (self.cagr(df) - rf)/self.volatility(df)
    
    
    def sortino(self, DF, rf):
        df = DF.copy()
        # df['ret'] = df['Last'].pct_change()
        negetive_return = np.where(df['ret']>0, 0, df['ret'])
        neg_vol = pd.Series(negetive_return[negetive_return!=0]).std()
        return (self.cagr(df) - rf)/neg_vol

    
    def max_drawdown(self, DF):
        df = DF.copy()
        # df['ret'] = df['Last'].pct_change()
        df['cum_return'] = (1+df['ret']).cumprod()
        df['cum_roll_max'] = df['cum_return'].cummax()
        df['drawdown'] = df['cum_roll_max'] - df['cum_return']
        return (df['drawdown']/df['cum_roll_max']).max()



class Indicators():
    
    
    
    def atr(self, DF, n=14):
        df = DF.copy()
        cols = df.columns
        try:
            df['H-L'] = df['High'] - df['Low']
            df['H-PC'] = abs(df['High'] - df['Last'].shift(1))
            df['L-PC'] = abs(df['Low'] - df['Last'].shift(1))
            df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1, skipna=False)
            df['ATR'] = df['TR'].ewm(span=n, min_periods=n-1).mean()
            return df["ATR"]
        except:
            df.columns = ['date', 'open', 'high', 'low', 'Last', 'volume']
            
            df['H-L'] = df['high'] - df['low']
            df['H-PC'] = abs(df['high'] - df['Last'].shift(1))
            df['L-PC'] = abs(df['low'] - df['Last'].shift(1))
            df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1, skipna=False)
            df['ATR'] = df['TR'].ewm(span=n, min_periods=n-1).mean()
            return df["ATR"]
    
    
    def macd(self, Df, a=12, b=26, c=9):
        df = Df.copy()
    #     df['ma_fast'] = df['Last'].rolling(12).mean()   # Simple moving avg
        df['ma_fast'] = df['Last'].ewm(span=a, min_periods=a-1).mean()
        df['ma_slow'] = df['Last'].ewm(span=b, min_periods=b-1).mean()
        df['macd'] = df['ma_fast'] - df['ma_slow']
        df['signal'] = df['macd'].ewm(span=c, min_periods=c-1).mean()
        
        return df.loc[:, ['signal', 'macd']]
    
    def bb(self, DF, n=14, stdev=2):
        df = DF.copy()
        df['Middle_Band'] = df['Last'].rolling(n).mean()
        df['Upper_Band'] = df['Middle_Band'] + stdev*df['Last'].rolling(n).std(ddof=0)
        df['Low_Band'] = df['Middle_Band'] - stdev*df['Last'].rolling(n).std(ddof=0)
        df['BB_Width'] = df['Upper_Band'] - df['Low_Band']
        return df['Middle_Band'], df['Upper_Band'], df['Low_Band'], df['BB_Width']
        
    
    def rsi(self, DF, n=14):
        df = DF.copy()
        df['change'] = df['Last'] - df['Last'].shift(1)
        df['Gain'] = np.where(df['change'] >= 0, df['change'], 0)
        df['Loss'] = np.where(df['change'] < 0, -1*df['change'], 0)
        df['Avg Gain'] = df['Gain'].ewm(alpha=1/n, min_periods=n).mean()
        df['Avg Loss'] = df['Loss'].ewm(alpha=1/n, min_periods=n).mean()
        df['rs'] = df['Avg Gain']/df['Avg Loss']
        df['rsi'] = 100 - (100/(1+df['rs']))
        return df['rsi']
    
    
    def adx(self, DF, n=14):
        df = DF.copy()
        df['upmove'] = df['High'] - df['High'].shift(1)
        df['downmove'] = df['Low'].shift(1) - df['Low']
        df['+dm'] = np.where((df['upmove']>df['downmove']) & (df['upmove']>0), df['upmove'], 0)
        df['-dm'] = np.where((df['upmove']<df['downmove']) & (df['downmove']>0), df['downmove'], 0)
        
        # Calculating Average True Range(ATR)
        
        df['H-L'] = df['High'] - df['Low']
        df['H-PC'] = abs(df['High'] - df['Last'].shift(1))
        df['L-PC'] = abs(df['Low'] - df['Last'].shift(1))
        df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1, skipna=False)
        df['ATR'] = df['TR'].ewm(com=n, min_periods=n-1).mean()
        
        # Calculating Average Directional Index(ADX)
        
        df['+di'] = 100*(df['+dm']/df['ATR']).ewm(com=n, min_periods=n).mean()
        df['-di'] = 100*(df['-dm']/df['ATR']).ewm(com=n, min_periods=n).mean()
        
        df['di_sum'] = df['+di'] + df['-di']
        df['di_diff'] = abs(df['+di'] - df['-di'])
        
        df['dx'] = 100 * (df['di_diff']/df['di_sum'])
        df['adx'] = df['dx'].ewm(com=n, min_periods=n).mean()
        
        return df['adx']

    def renko(self, DF, hourly_df):
        df = DF.copy()
        df.drop('Last', axis=1, inplace=True)
        df.reset_index(inplace=True)
        
        df.columns = ['date', 'open', 'high', 'low', 'Last', 'volume']
        
        renko_df = Renko(df)
        
        renko_df.brick_size = 3 * round(self.atr(hourly_df, 120).iloc[-1], 0)
        
        renko_df = renko_df.get_ohlc_data()
        
        return renko_df
    
    
    def obv(self, DF):
        df = DF.copy()
        
        df['Daily_Ret'] = df['Adj Last'].pct_change()
        
        df['direction'] = np.where(df['Daily_Ret'] >=0, 1, -1)
        df['direction'][0] = 0
        df['vol_adj'] = df['Volume'] * df['direction']
        df['obv'] = df['vol_adj'].cumsum()
        
        return df['obv']


    def slope(self, DF, column, n):
        df = DF.copy()
        ser = df[f'{column}']
        
        slopes = [i*0 for i in range(n-1)]
        
        for i in range(n, len(ser)+1):
            y = ser[i-n: i]
            x = np.array(range(n))
            
            y_scaled = (y-y.min()) / (y.max() - y.min())
            x_scaled = (x - x.min()) / (x.max() - x.min())
            
            x_scaled = sm.add_constant(x_scaled)
            
            model = sm.OLS(y_scaled, x_scaled)
            
            results = model.fit()
            
            # results.summary()
            
            slopes.append(results.params[-1])
            
        slope_angle = (np.rad2deg(np.arctan(np.array(slopes))))
        
        return np.array(slope_angle)

    
    def supertrend(self, DF, atr_period, multiplier):
        df = DF.copy()
        
        high = df['High']
        low = df['Low']
        Last = df['Last']
        
        # calculate ATR
        price_diffs = [high - low, 
                       high - Last.shift(), 
                       Last.shift() - low]
        true_range = pd.concat(price_diffs, axis=1)
        true_range = true_range.abs().max(axis=1)
        # default ATR calculation in supertrend indicator
        atr = true_range.ewm(alpha=1/atr_period,min_periods=atr_period).mean() 
        # df['atr'] = df['tr'].rolling(atr_period).mean()
        
        # HL2 is simply the average of high and low prices
        hl2 = (high + low) / 2
        # upperband and lowerband calculation
        # notice that final bands are set to be equal to the respective bands
        final_upperband = upperband = hl2 + (multiplier * atr)
        final_lowerband = lowerband = hl2 - (multiplier * atr)
        
        # initialize Supertrend column to True
        supertrend = [True] * len(df)
        
        for i in range(1, len(df.index)):
            curr, prev = i, i-1
            
            # if current Last price crosses above upperband
            if Last[curr] > final_upperband[prev]:
                supertrend[curr] = True
            # if current Last price crosses below lowerband
            elif Last[curr] < final_lowerband[prev]:
                supertrend[curr] = False
            # else, the trend continues
            else:
                supertrend[curr] = supertrend[prev]
                
                # adjustment to the final bands
                if supertrend[curr] == True and final_lowerband[curr] < final_lowerband[prev]:
                    final_lowerband[curr] = final_lowerband[prev]
                if supertrend[curr] == False and final_upperband[curr] > final_upperband[prev]:
                    final_upperband[curr] = final_upperband[prev]

            # to remove bands according to the trend direction
            if supertrend[curr] == True:
                final_upperband[curr] = np.nan
            else:
                final_lowerband[curr] = np.nan
        
        df2 = pd.DataFrame({
            'Supertrend': supertrend,
            'Final Lowerband': final_lowerband,
            'Final Upperband': final_upperband
        }, index=df.index)
        
        df = df.join(df2)
        
        return df
    
    def vwap(self, DF):
        
        df = DF.copy()
        
        try:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
        except:
            pass
        
        def operations(group):
            hlc3 = (group['High'] + group['Low'] + group['Last']) / 3
            
            cumvol = group['Volume'].cumsum()
            
            cumvolprice = (hlc3 * group['Volume']).cumsum()
            
            group['VWAP'] = cumvolprice / cumvol
            
            return group['VWAP']
        
        df = DF.copy()
        
        group = df.groupby(df['Datetime'].dt.date)

        return group.apply(operations).values
        
        # for d in df['Datetime'].dt.date.unique():
        #     # break
        #     df.loc[(df['Date'] == d), 'CumVol'] = df.loc[(df['Date'] == d), 'Volume'].cumsum().astype(int)
            
        #     df.loc[(df['Date'] == d), 'hlc3'] = (df.loc[(df['Date'] == d), 'High'] + 
        #                                          df.loc[(df['Date'] == d), 'Low'] + 
        #                                          df.loc[(df['Date'] == d), 'Last'])/3
            
        #     df.loc[(df['Date'] == d), 'VolPrice'] = (df.loc[(df['Date'] == d), 'hlc3'] * 
        #                                              df.loc[(df['Date'] == d), 'Volume'])
        
        #     df.loc[(df['Date'] == d), 'CumVolPrice'] = (df.loc[(df['Date'] == d), 'VolPrice']).cumsum()
            
        #     df.loc[(df['Date'] == d), 'VWAP'] = (df.loc[(df['Date'] == d), 'CumVolPrice'] /
        #                                          df.loc[(df['Date'] == d), 'CumVol'])
            
        #     df.loc[(df['Date'] == d), 'VWAP_STD'] = df.loc[(df['Date'] == d), 'VWAP']
            
            
        # return df['VWAP']

    def heiken_ashi(self, DF):
        df = DF.copy()
        
        df.reset_index(drop=True, inplace=True)
        
        df['Last'] = (df['Open'] + df['High'] + df['Low'] + df['Last']) / 4
        # df['Open'].iloc[0] = (df['Open'][0] + df['Last'][0]) / 2
        df.loc[1:, 'Open'] = (df['Open'].shift(1) + df['Last'].shift(1)) / 2
        
        
        df['High'] = df[['High', 'Last', 'Open']].max(axis=1)
        df['Low'] = df[['Low', 'Open', 'Last']].min(axis=1)
        
        return df


class save_file(object):
    
    def save_pickle(self, df, path):
        
        with open(path, 'wb') as f:
            pickle.dump(df, f)
            
        print('Done')


    def open_df(self, path):
        
        with open(path, 'rb') as f:
            data = pickle.load(f)
        
        return data






































































