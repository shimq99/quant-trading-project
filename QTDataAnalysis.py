# coding=utf-8
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# encoding:UTF-8
import sys
import datetime
import logging
# from base.Base import Base
import pandas as pd
import numpy as np
import time

pd.set_option('display.max_columns', 10)
pd.set_option('precision', 15)
import decimal
# from dateutil import relativedelta
# import statsmodels.api as sm
import math

decimal.getcontext().prec = 10
from tools import PandasDBUtils as pdUtil
from log.LogManager import LogManager


def getRawData():
    tick_data = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\tickdatapart1\\20200102\\cu2012.csv', warn_bad_lines=True)
    timeframe = '1min'
    tick_data['Date'] = tick_data['Date'].astype(str)
    tick_data['Time'] = tick_data['Time'].astype(str)
    # combine date and time into datetime and get timestamp
    tick_data['DateTime'] = pd.to_datetime(tick_data['Date'] + ' ' + tick_data['Time'])
    # tick_data['TimeStamp'] = tick_data.DateTime.values.astype(np.int64)
    tick_data['TimeStamp'] = tick_data['DateTime'].apply(lambda x:time.mktime(x.timetuple()))

    interval ='5min'
    tick_data = tick_data.rename(columns={'Price': 'Close'})

    columns_list = ['Open', 'High', 'Low', 'Close', 'Vol']
    fun = ['first', 'max', 'min', 'last', 'sum']
    sinceTime = '2020-01-02T00:00:00.000Z'
    endTime = '2020-05-04T00:00:00.000Z'
    data_interval = tick_data.resample(rule=interval, on='DateTime', base=0, label='left', closed='left').agg(dict(zip(columns_list, fun)))
    # 规范化处理
    data_interval.reset_index(inplace=True)
    data_interval = data_interval.fillna(method='ffill')  # 如果是期货数据，则删除非交易时间段数据
    # 筛选时间
    sinceTime_ = pd.to_datetime(sinceTime, '%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
    endTime_ = pd.to_datetime(endTime, '%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
    data_interval = data_interval[(data_interval['timestamp'] >= sinceTime_) & (data_interval['timestamp'] <= endTime_)]




    tick_data.set_index('DATETIME', inplace=True)

    ohlcv_data = pd.DataFrame(columns=['SYMBOL_N', 'open', 'high', 'low', 'close', 'volume'])

    for symbol in tick_data['SYMBOL_N'].unique():
        ohlcv_symbol = tick_data.loc[tick_data['SYMBOL_N'] == symbol, 'PRICE'].resample(timeframe).ohlc()
        ohlcv_symbol['SYMBOL_N'] = symbol
        ohlcv_symbol['volume'] = (tick_data.loc[tick_data['SYMBOL_N'] == symbol, 'VOLUME'].resample(timeframe).max() - tick_data.loc[tick_data['SYMBOL_N'] == symbol, 'VOLUME'].resample(timeframe).max().shift(1))

    ohlcv_data = ohlcv_data.append(ohlcv_symbol, sort=False)

    print()


# class QTDataAnalysis(Base):
#     def __init__(self):
#         LogManager('QTDataAnalysis')
#         # self.count = 100
#         # self.abnormal_thred = 0.01
#         # self.abnormal_st_v0 = 2.5
#         # self.abnormal_st_v1 = -2.5
#         # self.mkt_id = None
#         # self.factor_info = None
#         # self.mv_weight = None
#         # self.sectormv_weight = None
#         # self.start_date = None
#         # self.end_date = None
#         # self.estimate_security_data = None
#         # self.estimate_des_info_for_portfolio = None
#         # self.estimate_univ_info = pd.DataFrame(columns=['Date', 'MarketId', 'DescriptorFactorId', 'Name', 'Value'])
#         # self.common_cols = ['FactorId', 'FactorName', 'FactorCategory', 'Date', 'SecurityId', 'Ticker', 'FactorValue','FactorValue_st', 'FactorValue_ratio', 'FactorValue_st_ratio']
#         # self.descriptor_info_result = pd.DataFrame(columns=['Date', 'MarketId', 'DescriptorId', 'Name', 'Value'])
#         # self.descriptor_infobySector_result = pd.DataFrame(columns=['Date', 'MarketId', 'SectorId', 'DescriptorId', 'Name', 'Value'])
#         # self.descriptor_result = pd.DataFrame(columns=['FactorId', 'FactorName', 'FactorCategory', 'Date', 'SecurityId', 'Ticker', 'FactorValue','FactorValue_ratio', 'FactorValue_st', 'FactorValue_st_ratio'])
#         # logging.warn('FactorModelAnalysis')
#
#     def getRawData(self):
#         rawdata1 = pd.read_csv('C:\temp\a2001.csv', warn_bad_lines=True)
#         print()


if __name__ == '__main__':
    getRawData()
    # QTDataAnalysis = QTDataAnalysis()
    # QTDataAnalysis.getRawData()
    # QTDataAnalysis.initSqlServer('prod')
    # QTDataAnalysis.runEstimateUniv('2019-07-03', '2020-07-03', 1)
    # # factorModelAnalysis.factor_regression('2019-04-01','2020-04-01',1)
    # # factorModelAnalysis.factor_checking('2019-04-01', '2020-04-01', 1)
    # # factorModelAnalysis.descriptor_regression('2019-07-17','2020-07-17',1)
    # # factorModelAnalysis.runPortfolio('2019-07-01','2020-07-01', 1,'PMSF','T10')
    # # factorModelAnalysis.test('2019-04-01','2020-04-01',2)
    # QTDataAnalysis.closeSqlServerConnection()
