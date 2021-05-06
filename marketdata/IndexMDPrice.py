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


class IndexMDPrice(Base):
    def __init__(self, env):
        self.env = env
        LogManager('IndexMDPrice')

    def updateToDataBase(self, sql, updateRecords):
        if updateRecords:
            self.cursor.executemany(sql, updateRecords)

    def selectFromDataBase(self, sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def insertToDatabase(self, sql, data):
        if data:
            try:
                self.cursor.executemany(sql, data)
            except pyodbc.IntegrityError, e:
                logging.warning(
                    'insertToDatabase: integrity error while saving record, will ignore: ' + e.message + e.args[1])
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def cleanMDData(self, tickers,startDateStr,endDateStr):
        if tickers:
            self.initSqlServer('prod')
            sql = 'delete from RiskDb.bench.BenchmarkIndexEodPrice where IndexCode in (\'' + ('\',\'').join(tickers)+'\') and TradeDate between ? and ?'
            self.cursor.execute(sql, (startDateStr, endDateStr))
            self.closeSqlServerConnection()


    def refreshAndDownloadMarketData(self, startDateStr, endDateStr, indexTickers):
        mdService = MarketDataDownloader(self.env, bbgUrl='http://192.168.203.40:9090')
        mdService.initSqlServer(self.env)
        self.cleanMDData(indexTickers,startDateStr,endDateStr)
        mdService.getIndexMdFromBbgNILVALUE(startDateStr, endDateStr, indexTickers)
        mdService.closeSqlServerConnection()


    def run(self):
        self.initSqlServer(self.env)
        sql = 'select distinct(BenchmarkBB_TCM) from TradeRule.swap.SwapTradeParameter where BenchmarkBB_TCM is not null'
        idx_data = self.selectFromDataBase(sql)
        self.closeSqlServerConnection()
        indexTickers = list(idx_data['BenchmarkBB_TCM'].unique())
        indexList = ['US00O/N Index', 'US0003M Index', 'SHIF3M Index', 'FEDL01 Index']
        indexTickers += indexList
        currentDate = datetime.datetime.now()
        weekDay = currentDate.date().weekday()
        diff = 15
        if (weekDay == 5 or weekDay == 6):
            return
        runPre1Days = currentDate - datetime.timedelta(days=1)
        if (weekDay == 0):
            runPre1Days = currentDate - datetime.timedelta(days=3)
        runPre2Days = currentDate - datetime.timedelta(days=diff)
        runPre2DaysStr = runPre2Days.strftime('%Y-%m-%d')
        runPre1DaysStr = runPre1Days.strftime('%Y-%m-%d')
        self.refreshAndDownloadMarketData(runPre2DaysStr,runPre1DaysStr,indexTickers)
        #self.refreshAndDownloadMarketData('2019-09-01','2019-10-15',indexTickers)


if __name__ == '__main__':
    ####勿提交改动，
    env = 'prod'
    indexMDPrice = IndexMDPrice(env)
    #indexMDPrice.run()
    indexTickers = ['MXAS Index','MXCN Index']
    indexMDPrice.refreshAndDownloadMarketData('2018-01-03','2020-09-01',indexTickers)
