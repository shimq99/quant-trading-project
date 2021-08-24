# coding=utf-8
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# encoding:UTF-8
import sys
import datetime
import logging
from base.Base import Base
# # Generate dataset
import pandas as pd
import numpy as np
import os
import time
import re
import datetime as dt
from statsmodels.tsa.stattools import adfuller


pd.set_option('display.max_columns', 10)
pd.set_option('precision', 15)
import decimal

decimal.getcontext().prec = 10
from tools import PandasDBUtils as pdUtil
from log.LogManager import LogManager

class QTKlineModel(Base):
    def __init__(self):
        LogManager('QTKlineModel')
        logging.info('QTKlineModel')
        self.filename =''
        self.opentimes =[]

    def Kline_1min(self,tick_data):
        interval = '1min'
        tick_data = self.filter_data(tick_data,'closed')
        data_interval = self.intervaliteratewindowTraDay(interval, tick_data)
        data_interval['Date'] = pd.to_datetime(data_interval['Date'])
        data_interval['Vol'] = data_interval['Vol'].fillna(0)
        data_interval = data_interval.fillna(method='ffill')
        data_interval = data_interval.dropna()  # it's necessary for the first trading day case since it only trade at day and night except early morning
        data_interval = self.filter_data(data_interval,'open')
        return data_interval

    def resetDatenTime(self,data_interval):
        data_interval['DateTime'] = data_interval['DateTime'].astype(str)
        data_interval[['Date', 'Time']] = data_interval['DateTime'].str.split(expand=True)
        data_interval['DateTime'] = pd.to_datetime(data_interval['DateTime'])
        return data_interval

    def getTindex(self,certaindate_data, date, interval):
        nighttralist = certaindate_data['Time'].loc[(certaindate_data['Time'] > '19:00:00') | (certaindate_data['Time'] < '03:00:00')].tolist()
        # 取第一个 get time series and used for later resample
        diffday = certaindate_data['diffDay'].iloc[0]
        if (len(nighttralist) == 0) | (diffday > 4):
            starttime = dt.datetime.strptime(str(date) + " " + '09:00:00', "%Y%m%d %H:%M:%S")
            endtime = dt.datetime.strptime(str(date) + " " + '15:15:00', "%Y%m%d %H:%M:%S")
        else:
            starttime = dt.datetime.strptime(str(date), "%Y%m%d")
            endtime = dt.datetime.strptime(str(date) + " " + '23:59:59', "%Y%m%d %H:%M:%S")
        t_index = pd.DatetimeIndex(pd.date_range(start=starttime, end=endtime, freq=interval))
        # then reset the index and fill the na's with 0
        return t_index

    def filter_data(self,tick_data,filtertype):
        filtered_times = self.getcontracttradtime(tick_data,filtertype)
        ### filter out non-trading time data
        df = tick_data.loc[filtered_times]
        df.reset_index(inplace=True, drop=True)
        df['diffDay'] = df['TimeStamp'].diff()
        ### check time gap between tick data to indentify whether it's weekends / holiday
        df['diffDay'] = df['diffDay'] / 60 / 60 / 24
        df = df.fillna(0)
        return df

    def Kline_5min(self,data_interval,interval):
        data_interval['DateTime'] = pd.to_datetime(data_interval['DateTime'])
        data_interval = self.filter_data(data_interval, 'closed')
        data_5min = self.intervaliterate5min(interval, data_interval)
        data_5min['DateTime'] = data_5min['DateTime'].astype(str)
        data_5min[['Date', 'Time']] = data_5min['DateTime'].str.split(expand=True)
        data_5min['DateTime'] = pd.to_datetime(data_5min['DateTime'])

        data_5min['Vol'] = data_5min['Vol'].fillna(0)
        data_5min = data_5min.fillna(method='ffill')
        data_5min = self.filter_data(data_5min,'open')
        return data_5min

    def closedopen_condition(self,tick_data,filtertype):
        ### closed or open boundary
        if filtertype =='open':
            time_0900_1015 = ('09:00:00' < tick_data['Time']) & (tick_data['Time'] <= '10:15:00')
            time_1030_1130 = ('10:30:00' < tick_data['Time']) & (tick_data['Time'] <= '11:30:00')
            time_1330_1500 = ('13:30:00' < tick_data['Time']) & (tick_data['Time'] <= '15:00:00')
            time_2100_0230 = ('21:00:00' < tick_data['Time']) | (tick_data['Time'] <= '02:30:00')
            time_2100_0100 = ('21:00:00' < tick_data['Time']) | (tick_data['Time'] <= '01:00:00')
            time_2100_2300 = ('21:00:00' < tick_data['Time']) & (tick_data['Time'] <= '23:00:00')
            time_0930_1130 = ('09:30:00' < tick_data['Time']) & (tick_data['Time'] <= '11:30:00')
            time_1300_1500 = ('13:00:00' < tick_data['Time']) & (tick_data['Time'] <= '15:00:00')
            time_1300_1515 = ('13:00:00' < tick_data['Time']) & (tick_data['Time'] <= '15:15:00')
        elif filtertype =='closed':
            time_0900_1015 = ('09:00:00' <= tick_data['Time']) & (tick_data['Time'] <= '10:15:00')
            time_1030_1130 = ('10:30:00' <= tick_data['Time']) & (tick_data['Time'] <= '11:30:00')
            time_1330_1500 = ('13:30:00' <= tick_data['Time']) & (tick_data['Time'] <= '15:00:00')
            time_2100_0230 = ('21:00:00' <= tick_data['Time']) | (tick_data['Time'] <= '02:30:00')
            time_2100_0100 = ('21:00:00' <= tick_data['Time']) | (tick_data['Time'] <= '01:00:00')
            time_2100_2300 = ('21:00:00' <= tick_data['Time']) & (tick_data['Time'] <= '23:00:00')
            time_0930_1130 = ('09:30:00' <= tick_data['Time']) & (tick_data['Time'] <= '11:30:00')
            time_1300_1500 = ('13:00:00' <= tick_data['Time']) & (tick_data['Time'] <= '15:00:00')
            time_1300_1515 = ('13:00:00' <= tick_data['Time']) & (tick_data['Time'] <= '15:15:00')
        return time_0900_1015, time_1030_1130,time_1330_1500,time_2100_0230,time_2100_0100,time_2100_2300,time_0930_1130,time_1300_1500,time_1300_1515

    def getcontracttradtime(self,tick_data,filtertype):
        ### return trading time list based on certain category
        res = re.findall('([a-zA-Z ]*)\d*.*', self.filename)
        catname = str(res[0]).upper()
        time_0900_1015, time_1030_1130, time_1330_1500, time_2100_0230, time_2100_0100, time_2100_2300, time_0930_1130, time_1300_1500, time_1300_1515 = self.closedopen_condition(tick_data,filtertype)
        if catname in ['WR', 'BB', 'FB', 'JD', 'LH', 'AP', 'CJ', 'JR', 'RI', 'LR', 'PM', 'RS', 'SF', 'SM', 'WH', 'UR','PK']:
            filtered_times = time_0900_1015 | time_1030_1130 | time_1330_1500
            opentimes = ['09:00:00', '10:30:00', '13:30:00']
        elif catname in ['AU', 'AG', 'SC']:
            filtered_times = time_0900_1015 | time_1030_1130 | time_1330_1500 | time_2100_0230
            opentimes = ['09:00:00', '10:30:00', '13:30:00', '21:00:00']
        elif catname in ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'SS', 'BC']:
            filtered_times = time_0900_1015 | time_1030_1130 | time_1330_1500 | time_2100_0100
            opentimes = ['09:00:00', '10:30:00', '13:30:00', '21:00:00']
        elif catname in ['RU', 'FU', 'RB', 'HC', 'BU', 'SP', 'NR', 'LU', 'C', 'CS', 'M', 'Y', 'A', 'B', 'P', 'J', 'JM',
                         'PG', 'RR','EG', 'EB', 'L', 'PP', 'V', 'I', 'CF', 'CY', 'SR', 'TA', 'RM', 'FG', 'MA', 'SA', 'PF', 'OI','ZC']:
            filtered_times = time_0900_1015 | time_1030_1130 | time_1330_1500 | time_2100_2300
            opentimes = ['09:00:00', '10:30:00', '13:30:00', '21:00:00']
        elif catname in ['IC', 'IF', 'IH']:
            filtered_times = time_0930_1130 | time_1300_1500
            opentimes = ['09:30:00', '13:00:00']
        elif catname in ['T', 'TF', 'TS']:
            filtered_times = time_0930_1130 | time_1300_1515
            opentimes = ['09:30:00', '13:00:00']
        ### assign trading open time
        self.opentimes = opentimes
        return filtered_times

    def correctmidnighttradingday(self,tick_data):
        tick_data['Date'] = tick_data['Date'].astype(str)
        tick_data['Time'] = tick_data['Time'].astype(str)
        tick_data['CompareTime'] = tick_data['Time']
        tick_data['Time'] = pd.to_datetime(tick_data['Time'], infer_datetime_format=True)

        tick_data.loc[tick_data['Time'] < '08:59:00', 'Date'] = np.NaN
        tick_data['Date'] = tick_data['Date'].fillna(method='ffill')

        tick_data['DateTime'] = pd.to_datetime(tick_data['Date'] + ' ' + tick_data['CompareTime'])
        tick_data['DateTimeNextCalenDay'] = tick_data['DateTime'] + dt.timedelta(days=1)
        tick_data['DateTime'] = np.where(tick_data['Time'] < '08:59:00', tick_data['DateTimeNextCalenDay'],
                                         tick_data['DateTime'])
        tick_data = tick_data.drop_duplicates(subset=['DateTime'])
        return tick_data

    def resampleopentimes(self,certaindate_data,  columns_list, fun,interval):
        r = re.compile("([0-9]+)([a-zA-Z]+)")
        m = r.match(interval)
        timenum = int(m.group(1))
        timeperiod = m.group(2)

        result = pd.DataFrame()
        for start_time in self.opentimes:
            start_datetime = dt.datetime.strptime(start_time, '%H:%M:%S')
            if timeperiod =='min':
                nextbartime = str((start_datetime + dt.timedelta(minutes=timenum)).time())
            elif timeperiod =='h':
                nextbartime = str((start_datetime + dt.timedelta(hours=timenum)).time())
            df = certaindate_data[(certaindate_data['Time'] >= start_time) & (certaindate_data['Time'] <= nextbartime)]
            df['DateTime'] = nextbartime
            t = df.groupby(['DateTime']).agg(dict(zip(columns_list, fun))).reset_index()
            result = pd.concat([result, t], ignore_index=True)

        if not result.empty:
            result['DateTime'] = pd.to_datetime(result['Date'] + ' ' + result['Time'])
        return result

    def intervaliterate5min(self,interval, a):
        a['DateTime'] = a['DateTime'].astype(str)
        a[['Date', 'Time']] = a['DateTime'].str.split(expand=True)
        a['DateTime'] = pd.to_datetime(a['DateTime'])

        columns_list = ['Open', 'High', 'Low', 'Close', 'Vol', 'OI', 'Buy1', 'BuyVol1', 'Sale1', 'SaleVol1','TimeStamp', 'Time', 'Date', 'InfoSequence','TraDate']
        fun = ['first', 'max', 'min', 'last', 'sum', 'last', 'last', 'last', 'last', 'last', 'last', 'last', 'last','last','last']
        datelist = list(set(a['Date'].tolist()))
        result = pd.DataFrame()

        for date in datelist:
            certaindate_data = a[a['Date'] == date].copy()
            certaindate_data['DateTime'] = pd.to_datetime(certaindate_data['DateTime'])

            opendata = self.resampleopentimes(certaindate_data, columns_list, fun,interval)
            opendatetime = opendata['DateTime'].tolist()
            data_interval = certaindate_data.resample(rule=interval, on='DateTime', base=0, label='right',closed='right').agg(dict(zip(columns_list, fun)))
            data_interval = data_interval.rename_axis('DateTime').reset_index()

            data_interval = data_interval.loc[~data_interval['DateTime'].isin(opendatetime)]
            data_interval = pd.merge(opendata, data_interval, how='outer')
            result = pd.concat([result, data_interval], ignore_index=True)

        result = result.sort_values(by=['DateTime', 'InfoSequence'])
        result.reset_index(inplace=True, drop=True)
        result = result.drop_duplicates(subset=['DateTime'])
        return result


    def correctDate(self,data_interval, preTraDay):
        ### in raw data, nightshift market data is marked in the next trading date which needs correction
        # like real trading datetime is Jan 2, 2020, 23:00 (Thursday night), the raw data is marked as Jan 3, 2020, 23:00 (Friday)
        # real trading datetime is Jan 3, 2020, 23:00 (Friday night), the raw data is marked as Jan 6, 2020, 23:00 (Monday)
        data_interval.loc[(data_interval['Time'] < '08:59:00') | (data_interval['Time'] > '15:16:00'), 'Date'] = np.NaN
        data_interval['Date'] = data_interval['Date'].fillna(preTraDay)
        data_interval['DateTime'] = pd.to_datetime(data_interval['Date'] + ' ' + data_interval['Time'])
        data_interval['DateTimeNextCalenDay'] = data_interval['DateTime'] + dt.timedelta(days=1)
        data_interval['DateTime'] = np.where(data_interval['Time'] < '08:59:00', data_interval['DateTimeNextCalenDay'],data_interval['DateTime'])
        return data_interval

    def getpreTraDay(self,firstdate):
        firstdate = dt.datetime.strptime(str(firstdate), "%Y%m%d")
        offset = max(1, (firstdate.weekday() + 6) % 7 - 3)
        timedelta = datetime.timedelta(offset)
        most_recent = firstdate - timedelta
        most_recent = most_recent.strftime("%Y%m%d")
        return most_recent

    def intervaliteratewindowTraDay(self,interval, a):
        columns_list = ['Open', 'High', 'Low', 'Close', 'Vol', 'OI', 'Buy1', 'BuyVol1', 'Sale1', 'SaleVol1','TimeStamp', 'Time', 'Date', 'InfoSequence','TraDate']
        fun = ['first', 'max', 'min', 'last', 'sum', 'last', 'last', 'last', 'last', 'last', 'last', 'last', 'last','last','last']
        tradaylist = list(set(a['Date'].tolist()))
        tradaylist.sort()
        result = pd.DataFrame()
        a = a.drop_duplicates()
        a['TradeType'] = 1
        a['TradeType'][a['Time'] > '19:00:00'] = 0
        date = tradaylist[0]
        preTraDay = self.getpreTraDay(date)
        certaindate_data = a[a['Date'] == date].copy()
        certaindate_data['DateTime'] = pd.to_datetime(certaindate_data['DateTime'])
        certaindate_data = certaindate_data.sort_values(by=['TradeType', 'DateTime', 'InfoSequence'],ascending=[True, True, True])
        certaindate_data['Vol'] = certaindate_data['Vol'].diff().fillna(certaindate_data['Vol'])
        t_index = self.getTindex(certaindate_data, date, interval)
        data_interval = certaindate_data.resample(rule=interval, on='DateTime', base=0, label='right',closed='right').agg(dict(zip(columns_list, fun)))
        data_interval = data_interval.reindex(t_index)
        data_interval = data_interval.rename_axis('DateTime').reset_index()
        data_interval = self.resetDatenTime(data_interval)
        result = self.correctDate(data_interval, preTraDay)
        result = self.resetDatenTime(result)
        data_interval['TradeType'] = 1
        data_interval['TradeType'][data_interval['Time'] > '19:00:00'] = 0
        data_interval = data_interval.sort_values(by=['TradeType', 'DateTime', 'InfoSequence'],ascending=[True, True, True])
        data_interval[['Buy1', 'BuyVol1', 'Sale1', 'SaleVol1']] = data_interval[['Buy1', 'BuyVol1', 'Sale1', 'SaleVol1']].fillna(method='ffill')
        data_interval[['Buy1', 'BuyVol1', 'Sale1', 'SaleVol1']] = data_interval[['Buy1', 'BuyVol1', 'Sale1', 'SaleVol1']].fillna(0)
        for i in range(len(tradaylist) - 1):
            preTraDay = tradaylist[i]
            date = tradaylist[i + 1]
            certaindate_data = a[a['Date'] == date].copy()
            certaindate_data['DateTime'] = pd.to_datetime(certaindate_data['DateTime'])
            certaindate_data = certaindate_data.sort_values(by=['TradeType', 'DateTime', 'InfoSequence'],ascending=[True, True, True])
            certaindate_data['Vol'] = certaindate_data['Vol'].diff().fillna(certaindate_data['Vol'])
            t_index = self.getTindex(certaindate_data, date, interval)
            # then reset the index and fill the na's with 0
            ### resample data, cast data from tickdata to 1min Kline data
            data_interval = certaindate_data.resample(rule=interval, on='DateTime', base=0, label='right',closed='right').agg(dict(zip(columns_list, fun)))
            data_interval = data_interval.reindex(t_index)
            data_interval = data_interval.rename_axis('DateTime').reset_index()
            data_interval = self.resetDatenTime(data_interval)
            data_interval = self.correctDate(data_interval, preTraDay)
            data_interval['TraDate'] = date
            data_interval['TradeType'] = 1
            data_interval['TradeType'][data_interval['Time'] > '19:00:00'] = 0
            data_interval = data_interval.sort_values(by=['TradeType', 'DateTime', 'InfoSequence'],ascending=[True, True, True])
            data_interval[['Buy1', 'BuyVol1', 'Sale1', 'SaleVol1']] = data_interval[['Buy1', 'BuyVol1', 'Sale1', 'SaleVol1']].fillna(method='ffill')
            data_interval[['Buy1', 'BuyVol1', 'Sale1', 'SaleVol1']] = data_interval[['Buy1', 'BuyVol1', 'Sale1', 'SaleVol1']].fillna(0)
            data_interval = data_interval.sort_values(by=['DateTime', 'InfoSequence'])
            result = pd.concat([result, data_interval], ignore_index=True)
        result = result.sort_values(by=['DateTime', 'InfoSequence'])
        result = result.drop_duplicates(subset=['DateTime'])
        result.reset_index(inplace=True, drop=True)

        return result

    def run1min(self,tick_data):
        tick_data['Date'] = tick_data['Date'].astype(str)
        tick_data['Time'] = tick_data['Time'].astype(str)
        tick_data['DateTime'] = pd.to_datetime(tick_data['Date'] + ' ' + tick_data['Time'])
        tick_data['TimeStamp'] = tick_data['DateTime'].apply(lambda x: time.mktime(x.timetuple()))
        tick_data = tick_data.rename(columns={'Price': 'Close'})
        ### clean dirty data in 'High', 'Low' column, High > 0, Low < 0, dirty data cast as 'Close'
        tick_data['HighCheck'] = tick_data['High'].diff().fillna(0)
        tick_data['LowCheck'] = tick_data['Low'].diff().fillna(0)
        tick_data['High'] = np.where(tick_data['HighCheck'] <= 0, tick_data['Close'], tick_data['High'])
        tick_data['Low'] = np.where(tick_data['LowCheck'] >= 0, tick_data['Close'], tick_data['Low'])
        tick_data['Low'] = np.where(tick_data['Low'] == 0, tick_data['Close'], tick_data['Low'])
        tick_data['Open'] = tick_data['Close']
        tick_data['TraDate'] = tick_data['Date']
        ### Clean dirty data as Close==0
        tick_data = tick_data[tick_data.Close != 0]
        ### Mark data sequence based on trading time
        tick_data = tick_data.rename_axis('InfoSequence').reset_index()
        tick_data = self.filter_data(tick_data, 'closed')
        kline1min_data = self.Kline_1min(tick_data)
        kline1min_data.to_csv('F:\\KlineData\\' + self.filename + '_1min.csv')
        return kline1min_data

    def run3min(self,kline1min_data):
        kline5min_data = self.Kline_5min(kline1min_data, '3min')
        ### OI for each data only calculate at the desired Kline
        kline5min_data['OI_total'] = kline5min_data['OI']
        kline5min_data['OI'] = kline5min_data['OI'].diff().fillna(0)
        kline5min_data['OI'].iloc[0] = 0
        kline5min_data.fillna(0, inplace=True)
        kline5min_data.to_csv('F:\\KlineData\\' + self.filename + '_3min.csv')
        return kline5min_data

    def run1h(self,kline1min_data):
        kline5min_data = self.Kline_5min(kline1min_data, '1h')
        kline5min_data['OI'] = kline5min_data['OI'].diff().fillna(0)
        kline5min_data['OI'].iloc[0] = 0
        kline5min_data.fillna(0, inplace=True)
        kline5min_data.to_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\Combine2019_2020\\Combined\\' + self.filename + '_5min.csv')
        return kline5min_data

    def run1day(self,a):
        columns_list = ['Open', 'High', 'Low', 'Close', 'Vol', 'OI', 'Buy1', 'BuyVol1', 'Sale1', 'SaleVol1','TimeStamp', 'Time', 'Date', 'InfoSequence', 'DateTime']
        fun = ['first', 'max', 'min', 'last', 'sum', 'last', 'last', 'last', 'last', 'last', 'last', 'last', 'last','last', 'last']
        a = a.sort_values(by = 'DateTime', ascending= True)
        a = a.groupby('TraDate').agg(dict(zip(columns_list, fun)))
        # a['OI'] = a['OI'].diff().fillna(0)
        # a['OI'].iloc[0] = 0
        a.fillna(0, inplace=True)
        a = a.reset_index(inplace=False)
        a.to_csv('F:\\KlineData\\' + self.filename + '_1D.csv')

    def find_filenames(self,search_path):
        ### Combine Raw Data for Y2020
        file_list0 = []
        for dir, dir_name, file_list in os.walk(search_path):
            for file in file_list:
                if file.endswith(".csv"):
                    file_list0.append(file)
        return list(set(file_list0))

    def find_cat_filespath(self,cat, search_path):
        ### for certain category find all contracts paths of the category, return a list of paths
        result = []
        for dir, dir_name, file_list in os.walk(search_path):
            for files in file_list:
                if (re.findall('([a-zA-Z ]*)\d*.*', files)[0] == cat.lower()) & (files.endswith('.csv')):
                    result.append(os.path.join(search_path, dir, files))
        return result

    def runKline(self):
        # name = ['a2011', 'a2101', 'a2103', 'a2105', 'a2107','a2109', 'a2111']
        path = 'F:\\1920total'
        # filenames = self.find_filenames(path)
        for cat in ['J','JM','Y','M','P','AG','AU']:
            pathnames = self.find_cat_filespath(cat, path)
            # pathnames = ['F:\\1920total\\rb2112.csv']
            for pathname in pathnames:
                logging.info('running'+pathname)
                # pathname = 'F:\\1920total\\'+aname
                tick_data = pd.read_csv(pathname)
                base = os.path.basename(pathname)
                ### get contract name from path
                self.filename = os.path.splitext(base)[0]
                ### every Kline data need to run 1min Kline first as base
                kline1min_data = self.run1min(tick_data)
                # kline1min_data = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\2020A\\' + self.filename + '_1mintest.csv')
                ### after 1min, run should based on 1min data
                kline3min_data = self.run3min(kline1min_data)
                # kline1h_data = self.run1h(kline1min_data)
                kline1day_data = self.run1day(kline1min_data)


if __name__ == '__main__':
    QTKlineModel = QTKlineModel()
    QTKlineModel.runKline()
