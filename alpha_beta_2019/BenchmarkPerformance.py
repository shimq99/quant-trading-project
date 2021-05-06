# encoding:UTF-8
from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import pandas as pd
import logging
from benchmark.tools import PandasDBUtils as pdUtil
from benchmark.marketdata.MarketDataDownloader import *
from benchmark.base.CommonEnums import AlphaBetaType
from benchmark.base.CommonEnums import PortfolioType
from benchmark.tools import Utils
import numpy as np
import datetime
import json
from decimal import *
import statsmodels.api as sm
getcontext().prec = 32
import sys
import pyodbc
from benchmark.tools.IndicatorCalculation import IndicatorCalculation

class BenchmarkPerformance(Base):
    def __init__(self, env):
        self.env = env
        self.startDate='2019-12-31'
        LogManager('BenchmarkPerformance')

    def updateToDataBase(self,sql, updateRecords):
        if updateRecords:
            self.cursor.executemany(sql, updateRecords)

    def selectFromDataBase(self,sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def getBenchmarkTeams(self,year):
        sql = 'select distinct [PortfolioCode] from [RiskDb].[bench].[BenchmarkPortfolioWeight]'
        data = self.selectFromDataBase(sql)
        return data

    def getBenchmarkPortfolioConstData(self, dateStr):
        sql = 'SELECT [PortfolioCode],[BbgTicker],[TradeDate],[CloseValue]  FROM [RiskDb].[bench].[BenchmarkPortfolioConstEodPrice] where TradeDate between \''+self.startDate+' and \''+dateStr+'\''
        data = self.selectFromDataBase(sql)
        return data

    def getMonthEndDataOnly(self,dataframe):
        dataframe['YearTemp'] = dataframe['TradeDate'].dt.year
        dataframe['MonthTemp'] = dataframe['TradeDate'].dt.month
        years = list(dataframe['YearTemp'].unique())
        months = list(dataframe['MonthTemp'].unique())
        dataframe.sort_values('TradeDate', ascending=True, inplace=True)
        total_result = dataframe.iloc[[0]].copy()
        for year in years:
            year_data = dataframe[dataframe['YearTemp'] == year].copy()
            for m in months:
                month_data = year_data[year_data['MonthTemp']==m].copy()
                if not month_data.empty:
                    month_data.sort_values('TradeDate', ascending=True, inplace=True)
                    total_result = pd.concat([total_result, month_data.iloc[[-1]]],axis=0)
                else:
                    logging.warning(str(year)+'-'+str(m)+', no data')
        total_result.drop_duplicates(subset='TradeDate', inplace=True)
        total_result.sort_values('TradeDate', ascending=True, inplace=True)

        del dataframe['YearTemp']
        del dataframe['MonthTemp']
        return total_result

    def run(self):
        data = self.getBenchmarkPortfolioConstData('2020-02-28')
        porfolios = list(data['PortfolioCode'].unique())

        for porfolio_code in porfolios:
            porfolio_data = data[data['PortfolioCode']==porfolio_code].copy()
            tickers = list(porfolio_data['BbgTicker'].unique())
            for ticker in tickers:
                ticker_data = porfolio_data[porfolio_data['BbgTicker']==ticker].copy()
                ticker_data['TradeDate'] = pd.to_datetime(ticker_data['TradeDate'])
                ticker_data.sort_values('TradeDate', ascending=True, inplace=True)
                ticker_data = self.getMonthEndDataOnly(ticker_data)
                ticker_data['MTD'] = (ticker_data['CloseValue'] / ticker_data['CloseValue'].shift(1)) - 1
                ticker_data['YTD'] = (ticker_data['CloseValue'] / ticker_data['CloseValue'].iloc[0]) - 1





if __name__ == '__main__':
    ####勿提交改动，
    env = 'prod'
    benchmarkPerformance = BenchmarkPerformance(env)
    print 'test'

