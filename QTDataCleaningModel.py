# coding=utf-8
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# encoding:UTF-8
import sys
import datetime
import logging
from base.Base import Base
import pandas as pd
import os
import numpy as np
import time
import re
import datetime

pd.set_option('display.max_columns', 10)
pd.set_option('precision', 15)
import decimal

decimal.getcontext().prec = 10
from tools import PandasDBUtils as pdUtil
from log.LogManager import LogManager

class QTDataCleaning(Base):
    def __init__(self):
        LogManager('QTDataCleaning')
        logging.info('QTDataCleaning')

    def find_filenames(self,search_path):
        ### Combine Raw Data for Y2020
        file_list0 = []
        for dir, dir_name, file_list in os.walk(search_path):
            for file in file_list:
                if file.endswith(".csv"):
                    file_list0.append(file)
        return list(set(file_list0))

    def find_filespath(self,filename, search_path):
        ### Combine Raw Data for Y2020
        result = []

        # Walking top-down from the root
        for dir, dir_name, file_list in os.walk(search_path):
            if filename in file_list:
                result.append(os.path.join(search_path, dir, filename))
        return result

    def combinedcontract(self,name, path, storedir):
        ### Combine Raw Data for Y2020
        files = self.find_filespath(name, path)
        combined_df = pd.concat([pd.read_csv(file) for file in files])
        combined_df.to_csv(storedir + '\\' + name)
        return combined_df

    def runrawdata(self):
        ### Combine Raw Data for Y2020
        storedir = 'C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\2020tickdata'
        path = r'C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data'
        # filenames = self.find_filenames(path)
        # for name in filenames:
        #     logging.info('running'+' '+name)
        #     self.combinedcontract(name, path, storedir)

        path0 = r'C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\2020tickdata'
        a = self.find_filenames(path0)
        path = r'C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data'
        b = self.find_filenames(path)
        filenames = list(set(b) - set(a))
        for name in filenames:
            logging.info('running'+' '+name)
            self.combinedcontract(name, path, storedir)

    def completetimeseries(self,tick_data):
        #     idx = pd.date_range("2020-01-02 08:59:00", '2020-12-31 23:59:59', freq="1S")
        #     ts = pd.DataFrame(data=idx, columns=['DateTime'])
        ts = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\completetimeseries2020.csv')
        ts['DateTime'] = pd.to_datetime(ts['DateTime'])
        tick_data = pd.merge(ts, tick_data, how='left', on='DateTime')
        tick_data = tick_data.ffill()
        return tick_data

    def Kline_1min(self,tick_data):
        interval = '1min'
        tick_data = tick_data.rename(columns={'Price': 'Close'})
        columns_list = ['Open', 'High', 'Low', 'Close', 'Vol', 'OI', 'Buy1', 'BuyVol1', 'Sale1', 'SaleVol1','TimeStamp']
        fun = ['first', 'max', 'min', 'last', 'last', 'last', 'last', 'last', 'last', 'last','last']
        data_interval = tick_data.resample(rule=interval, on='DateTime', base=0, label='right', closed='right').agg(dict(zip(columns_list, fun)))
        # 规范化处理
        data_interval.reset_index(inplace=True)
        return data_interval

    def Kline_5minon1min(self,data_interval):
        columns_list = ['Open', 'High', 'Low', 'Close', 'Vol', 'OI', 'Buy1', 'BuyVol1', 'Sale1', 'SaleVol1','TimeStamp']
        fun = ['first', 'max', 'min', 'last', 'last', 'last', 'last', 'last', 'last', 'last', 'last']
        data_5min = data_interval.resample(rule='5min', on='DateTime', base=0, label='right', closed='right').agg(dict(zip(columns_list, fun)))
        # 规范化处理
        data_5min.reset_index(inplace=True)
        data_5min = data_5min.fillna(method='ffill')
        return data_5min

    def cleanData(self):
        storedir = 'C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\2020tickdata'
        path = r'C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data'
        tick_data = self.combinedcontract('cu2012.csv', path, storedir)
        # tick_data = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\2020tickdata\\cu2012.csv')
        # tick_data['Date'] = tick_data['Date'].astype(str)
        # tick_data['Time'] = tick_data['Time'].astype(str)
        # combine date and time into datetime and get timestamp
        # tick_data['DateTime'] = pd.to_datetime(tick_data['Date'] + ' ' + tick_data['Time'])
        # tick_data['TimeStamp'] = tick_data['DateTime'].apply(lambda x: time.mktime(x.timetuple()))



    def getcontracttradtime(self,filename, tick_data):
        res = re.findall('([a-zA-Z ]*)\d*.*', filename)
        catname = str(res[0]).upper()

        time_0900_1015 = ('09:00:00' <= tick_data['Time']) & (tick_data['Time'] <= '10:15:00')
        time_1030_1130 = ('10:30:00' <= tick_data['Time']) & (tick_data['Time'] <= '11:30:00')
        time_1330_1500 = ('13:30:00' <= tick_data['Time']) & (tick_data['Time'] <= '15:00:00')
        time_2100_0230 = ('21:00:00' <= tick_data['Time']) | (tick_data['Time'] <= '02:30:00')
        time_2100_0100 = ('21:00:00' <= tick_data['Time']) | (tick_data['Time'] <= '01:00:00')
        time_2100_2300 = ('21:00:00' <= tick_data['Time']) & (tick_data['Time'] <= '23:00:00')
        time_0930_1130 = ('09:30:00' <= tick_data['Time']) & (tick_data['Time'] <= '11:30:00')
        time_1300_1500 = ('13:00:00' <= tick_data['Time']) & (tick_data['Time'] <= '15:00:00')
        time_1300_1515 = ('13:00:00' <= tick_data['Time']) & (tick_data['Time'] <= '15:15:00')

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
        filtered_times = self.getcontracttradtime(filename , tick_data)
        df = tick_data.loc[filtered_times]
        return df

    def correctnighttradingdate(self, tick_data):
        tick_data['Date'] = tick_data['Date'].astype(str)
        tick_data['Time'] = tick_data['Time'].astype(str)
        tick_data['CompareTime'] = tick_data['Time']
        tick_data['Time'] = pd.to_datetime(tick_data['Time'], infer_datetime_format=True)
        tick_data.loc[(tick_data['Time'] > '15:16:00') | (tick_data['Time'] < '08:59:00'), 'Date'] = np.NaN
        tick_data['Date'] = tick_data['Date'].fillna(method='ffill')

        tick_data['DateTime'] = pd.to_datetime(tick_data['Date'] + ' ' + tick_data['CompareTime'])
        tick_data['TimeStamp'] = tick_data['DateTime'].apply(lambda x: time.mktime(x.timetuple()))
        tick_data['DateTimeNextCalenDay'] = tick_data['DateTime'] + datetime.timedelta(days=1)
        tick_data['DateTime'] = np.where(tick_data['Time'] < '8:59:00', tick_data['DateTimeNextCalenDay'],
                                         tick_data['DateTime'])
        return tick_data

    def testclean(self):
        tick_data = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\al2012_test.csv')

        tick_data = self.correctnighttradingdate(tick_data)

        tick_data['High'] = tick_data[['Price', 'High']].max(axis=1)
        tick_data['Low'] = np.where(tick_data['Low'] == 0, tick_data['Price'], tick_data['Low'])

        tick_data = self.completetimeseries(tick_data)
        filtered_data  = self.filter_data('al2012.csv',tick_data)
        filtered_data['Open'] = filtered_data['Price'].shift(1)
        filtered_data.fillna(0, inplace=True)
        kline1min_data = self.Kline_1min(filtered_data)
        kline1min_data.to_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\al2012_test_1min.csv')
        kline5min_data = self.Kline_5minon1min(kline1min_data)
        kline5min_data['Vol'] = kline5min_data['Vol'].diff()
        kline5min_data['OI'] = kline5min_data['OI'].diff()
        kline5min_data.fillna(0, inplace=True)
        kline5min_data.to_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\al2012_test_5min.csv')
        return



if __name__ == '__main__':
    QTDataCleaning = QTDataCleaning()
    QTDataCleaning.cleanData()
