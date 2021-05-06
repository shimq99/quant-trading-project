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

    def Kline_1min(self,tick_data):
        interval = '1min'
        tick_data = self.filter_data('al2012.csv', tick_data)
        data_interval = self.intervaliteratewindowTraDay(interval, tick_data)

        data_interval['Vol'] = data_interval['Vol'].fillna(0)
        data_interval = data_interval.fillna(method='ffill')
        data_interval = data_interval.dropna()  # it's necessary for the first trading day case since it only trade at day and night except early morning

        data_interval = self.filter_data('al2012.csv', data_interval)
        return data_interval

    def getTindex(self,certaindate_data, date, interval):
        nighttralist = certaindate_data['Time'].loc[(certaindate_data['Time'] > '19:00:00') | (certaindate_data['Time'] < '03:00:00')].tolist()
        # 取第一个
        diffday = certaindate_data['diffDay'].iloc[0]

        if (len(nighttralist) == 0) | (diffday > 4):
            starttime = dt.datetime.strptime(str(date) + " " + '08:59:00', "%Y%m%d %H:%M:%S")
            endtime = dt.datetime.strptime(str(date) + " " + '15:16:00', "%Y%m%d %H:%M:%S")
        else:
            starttime = dt.datetime.strptime(str(date), "%Y%m%d")
            endtime = dt.datetime.strptime(str(date) + " " + '23:59:59', "%Y%m%d %H:%M:%S")

        t_index = pd.DatetimeIndex(pd.date_range(start=starttime, end=endtime, freq=interval))
        # then reset the index and fill the na's with 0
        return t_index

    def intervaliteratewindowTraDay(self,interval, a):
        columns_list = ['Open', 'High', 'Low', 'Close', 'Vol', 'OI', 'Buy1', 'BuyVol1', 'Sale1', 'SaleVol1','TimeStamp', 'Time', 'Date']
        fun = ['first', 'max', 'min', 'last', 'sum', 'last', 'last', 'last', 'last', 'last', 'last', 'last', 'last']

        tradaylist = list(set(a['Date'].tolist()))
        tradaylist.sort()

        a = a.drop_duplicates()
        a['TradeType'] = 1
        a['TradeType'][a['Time'] > '19:00:00'] = 0

        preTraDay = '20200101'
        date = tradaylist[0]
        certaindate_data = a[a['Date'] == date].copy()
        certaindate_data['DateTime'] = pd.to_datetime(certaindate_data['DateTime'])
        certaindate_data = certaindate_data.sort_values(by=['TradeType', 'DateTime'], ascending=[True, True])
        certaindate_data['Vol'] = certaindate_data['Vol'].diff().fillna(certaindate_data['Vol'])
        t_index = self.getTindex(certaindate_data, date, interval)
        # then reset the index and fill the na's with 0

        data_interval = certaindate_data.resample(rule=interval, on='DateTime', base=0, label='right',
                                                  closed='right').agg(dict(zip(columns_list, fun)))
        data_interval = data_interval.reindex(t_index)
        data_interval = data_interval.rename_axis('DateTime').reset_index()

        data_interval = self.resetDatenTime(data_interval)
        result = self.correctDate(data_interval, preTraDay)

        for i in range(len(tradaylist) - 1):
            preTraDay = tradaylist[i]
            date = tradaylist[i + 1]
            certaindate_data = a[a['Date'] == date].copy()
            certaindate_data['DateTime'] = pd.to_datetime(certaindate_data['DateTime'])

            certaindate_data = certaindate_data.sort_values(by=['TradeType', 'DateTime'], ascending=[True, True])
            certaindate_data['Vol'] = certaindate_data['Vol'].diff().fillna(certaindate_data['Vol'])
            t_index = self.getTindex(certaindate_data, date, interval)
            # then reset the index and fill the na's with 0

            data_interval = certaindate_data.resample(rule=interval, on='DateTime', base=0, label='right',
                                                      closed='right').agg(dict(zip(columns_list, fun)))
            data_interval = data_interval.reindex(t_index)
            data_interval = data_interval.rename_axis('DateTime').reset_index()

            data_interval = self.resetDatenTime(data_interval)
            data_interval = self.correctDate(data_interval, preTraDay)
            result = pd.concat([result, data_interval], ignore_index=True)

        result = result.sort_values(by='DateTime')
        result = result.drop_duplicates(subset=['DateTime'])
        result.reset_index(inplace=True, drop=True)
        return result

    def correctDate(self,data_interval, preTraDay):
        data_interval.loc[(data_interval['Time'] < '08:59:00') | (data_interval['Time'] > '15:16:00'), 'Date'] = np.NaN
        data_interval['Date'] = data_interval['Date'].fillna(preTraDay)
        data_interval['DateTime'] = pd.to_datetime(data_interval['Date'] + ' ' + data_interval['Time'])

        data_interval['DateTimeNextCalenDay'] = data_interval['DateTime'] + dt.timedelta(days=1)
        data_interval['DateTime'] = np.where(data_interval['Time'] < '08:59:00', data_interval['DateTimeNextCalenDay'],
                                             data_interval['DateTime'])
        return data_interval

    def resetDatenTime(self,data_interval):
        data_interval['DateTime'] = data_interval['DateTime'].astype(str)
        data_interval[['Date', 'Time']] = data_interval['DateTime'].str.split(expand=True)
        data_interval['DateTime'] = pd.to_datetime(data_interval['DateTime'])
        return data_interval

    def intervaliterateLater(self,interval, a):
        a['DateTime'] = a['DateTime'].astype(str)
        a[['Date', 'Time']] = a['DateTime'].str.split(expand=True)
        a['DateTime'] = pd.to_datetime(a['DateTime'])

        columns_list = ['Open', 'High', 'Low', 'Close', 'Vol', 'OI', 'Buy1', 'BuyVol1', 'Sale1', 'SaleVol1',
                        'TimeStamp']
        fun = ['first', 'max', 'min', 'last', 'sum', 'last', 'last', 'last', 'last', 'last', 'last']

        datelist = list(set(a['Date'].tolist()))
        result = pd.DataFrame()
        for date in datelist:
            certaindate_data = a[a['Date'] == date].copy()
            certaindate_data['DateTime'] = pd.to_datetime(certaindate_data['DateTime'])

            data_interval = certaindate_data.resample(rule=interval, on='DateTime', base=0, label='right',
                                                      closed='right').agg(dict(zip(columns_list, fun)))
            data_interval = data_interval.rename_axis('DateTime').reset_index()
            result = pd.concat([result, data_interval], ignore_index=True)

        result = result.sort_values(by='DateTime')
        #     print(result)
        result.reset_index(inplace=True, drop=True)
        result = result.drop_duplicates(subset=['DateTime'])
        return result

    def Kline_5minon1min(self,data_interval):
        data_5min = self.intervaliterateLater('5min', data_interval)

        data_5min['DateTime'] = data_5min['DateTime'].astype(str)
        data_5min[['Date', 'Time']] = data_5min['DateTime'].str.split(expand=True)
        data_5min['DateTime'] = pd.to_datetime(data_5min['DateTime'])

        data_5min['Vol'] = data_5min['Vol'].fillna(0)
        data_5min = data_5min.fillna(method='ffill')

        data_5min = self.filter_data('al2012.csv', data_5min)
        return data_5min

    def getcontracttradtime_initial2(self,filename, tick_data):
        res = re.findall('([a-zA-Z ]*)\d*.*', filename)
        catname = str(res[0]).upper()

        time_0900_1015 = ('09:00:00' < tick_data['Time']) & (tick_data['Time'] <= '10:15:00')
        time_1030_1130 = ('10:30:00' < tick_data['Time']) & (tick_data['Time'] <= '11:30:00')
        time_1330_1500 = ('13:30:00' < tick_data['Time']) & (tick_data['Time'] <= '15:00:00')
        time_2100_0230 = ('21:00:00' < tick_data['Time']) | (tick_data['Time'] <= '02:30:00')
        time_2100_0100 = ('21:00:00' < tick_data['Time']) | (tick_data['Time'] <= '01:00:00')
        time_2100_2300 = ('21:00:00' < tick_data['Time']) & (tick_data['Time'] <= '23:00:00')
        time_0930_1130 = ('09:30:00' < tick_data['Time']) & (tick_data['Time'] <= '11:30:00')
        time_1300_1500 = ('13:00:00' < tick_data['Time']) & (tick_data['Time'] <= '15:00:00')
        time_1300_1515 = ('13:00:00' < tick_data['Time']) & (tick_data['Time'] <= '15:15:00')
        #     time_weekend_0230 = (tick_data['WeekDay'] > 4) & ( tick_data['Time']<= '02:30:00')
        #     time_weekend_0100 = (tick_data['WeekDay'] > 4) & ( tick_data['Time']<= '01:00:00')

        if catname in ['WR', 'BB', 'FB', 'JD', 'LH', 'AP', 'CJ', 'JR', 'RI', 'LR', 'PM', 'RS', 'SF', 'SM', 'WH', 'UR',
                       'PK']:
            filtered_times = time_0900_1015 | time_1030_1130 | time_1330_1500
        elif catname in ['AU', 'AG', 'SC']:
            filtered_times = time_0900_1015 | time_1030_1130 | time_1330_1500 | time_2100_0230
        elif catname in ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'SS', 'BC']:
            filtered_times = time_0900_1015 | time_1030_1130 | time_1330_1500 | time_2100_0100
        elif catname in ['RU', 'FU', 'RB', 'HC', 'BU', 'SP', 'NR', 'LU', 'C', 'CS', 'M', 'Y', 'A', 'B', 'P', 'J', 'JM',
                         'PG', 'RR', 'EG', 'EB', 'L', 'PP', 'V', 'I', 'CF', 'CY', 'SR', 'TA', 'RM', 'FG', 'MA', 'SA',
                         'PF', 'OI', 'ZC']:
            filtered_times = time_0900_1015 | time_1030_1130 | time_1330_1500 | time_2100_2300
        elif catname in ['IC', 'IF', 'IH']:
            filtered_times = time_0930_1130 | time_1300_1500
        elif catname in ['T', 'TF', 'TS']:
            filtered_times = time_0930_1130 | time_1300_1515

        return filtered_times

    def filter_data(self,filename, tick_data):
        filtered_times = self.getcontracttradtime_initial2(filename, tick_data)
        df = tick_data.loc[filtered_times]
        df.reset_index(inplace=True, drop=True)

        df['diffDay'] = df['TimeStamp'].diff()
        df['diffDay'] = df['diffDay'] / 60 / 60 / 24
        df = df.fillna(0)
        return df


    def runKline(self):
        tick_data = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\al2012.csv')
        # 这样resample万一后时间点没有数值，后面的时间点就补不全
        tick_data['Date'] = tick_data['Date'].astype(str)
        tick_data['Time'] = tick_data['Time'].astype(str)
        # tick_data['Time'] = pd.to_datetime(tick_data['Time'], infer_datetime_format=True)
        tick_data['DateTime'] = pd.to_datetime(tick_data['Date'] + ' ' + tick_data['Time'])
        tick_data['TimeStamp'] = tick_data['DateTime'].apply(lambda x: time.mktime(x.timetuple())) ### slow
        a = tick_data
        a = a.rename(columns={'Price': 'Close'})
        a['Open'] = a['Close']
        a['Low'] = a['Close']
        a['High'] = a['Close']
        a = a[a.Close != 0]
        a = self.filter_data('al2012.csv', a)
        kline1min_data = self.Kline_1min(a)
        kline1min_data.to_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\al2012_1min1.csv')
        kline1min_data.head(70)

        kline5min_data = self.Kline_5minon1min(kline1min_data)
        # kline5min_data['Vol'] = kline5min_data['Vol'].diff()
        kline5min_data['OI'] = kline5min_data['OI'].diff().fillna(0)
        kline5min_data.fillna(0, inplace=True)

        kline5min_data.to_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\al2012_5min1.csv')


if __name__ == '__main__':
    QTKlineModel = QTKlineModel()
    QTKlineModel.runKline()
