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
import sqlite3
import sys

class CorrAnalysis(Base):
    def __init__(self,env):
        self.bbgAddr = ''
        self.env = env
        LogManager('AlphaBetaCalc')
        now = datetime.datetime.now()
        self.inceptionDate = str(now.year)+'-01-01'
        self.calcYear = str(now.year)

    def insertToDatabase(self,sql,data):
        if data:
            self.cursor.executemany(sql, data)
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def selectFromDataBase(self,sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def getTeamReturn(self,startDayStr,stopDayStr):
        sql = 'select FundBookCode,Date,YtdGrossReturn,a.FundId,a.BookId from Portfolio.perf.Nav as a join ReferenceData.ref.FundBook as b on a.FundId=b.FundId and a.BookId=b.BookId where FundBookCode not in (\'PMSF-T24\',\'PMSF-T33\',\'PMSF-T36\',\'PMSF-W07\') and Date between \''+startDayStr+'\' and \''+stopDayStr+'\' order by Date'
        teamReturnData = self.selectFromDataBase(sql)
        teamReturnData['Date'] = pd.to_datetime(teamReturnData['Date'])
        return teamReturnData

    def getFundReturn(self,startDayStr,stopDayStr):
        sql = 'select Date,YtdGrossReturn,a.FundId,b.FundCode,b.FundDesc from Portfolio.perf.Nav as a join RiskDb.ref.Fund as b on a.FundId=b.FundId where a.BookId is null and a.Date between \''+startDayStr+'\' and \''+stopDayStr+'\' order by Date'
        fundReturnData = self.selectFromDataBase(sql)
        return fundReturnData

    def saveCorrValues(self,corrRecords):
        if corrRecords:
            sql = 'insert into RiskDb.risk.Correlation(BeginDate, EndDate, FundCode, SourceFundBookCode, TargetFundBookCode, Correlation) values(?,?,?,?,?,?)'
            self.insertToDatabase(sql,corrRecords)
        else:
            logging.warn('saveCorrValues: empty record')

    def cleanCorrValues(self,dateStr, fundList):
        sql = 'delete FROM RiskDb.risk.Correlation where EndDate=\'' + dateStr + '\''
        if fundList:
            sql += ' and FundCode in (\'' + ('\',\'').join(fundList) + '\')'
        self.cursor.execute(sql)


    def calcPCTCHG(self,teamReturnData):
        fundBookCodeDataDict = dict()
        for fundBookCode, data in teamReturnData.groupby('FundBookCode'):
            fundCode = fundBookCode.split('-')[0]
            bookCode = fundBookCode.split('-')[1]
            if fundCode=='PMSF' and not (bookCode.startswith('T') or bookCode.startswith('W')):
                #PMSF只对比T,W book
                continue

            if fundCode == 'SLHL' and bookCode == 'W00':
                continue

            if fundCode == 'CVF' and bookCode == 'GTJA':
                continue

            copiedData = data.copy()
            copiedData.loc[:,('Date')] = pd.to_datetime(copiedData['Date'])
            copiedData.sort_values('Date', ascending=True, inplace=True)
            copiedData.drop_duplicates(subset='Date', inplace=True)

            sumYTDReturn = copiedData['YtdGrossReturn'].sum()
            if sumYTDReturn == 0:
                #skipped if all YTD is 0
                continue

            copiedData.loc[:,(fundBookCode)] = ((copiedData['YtdGrossReturn'] - copiedData['YtdGrossReturn'].shift(1)) / ( 1 + data['YtdGrossReturn'].shift(1))).astype(float)
            if fundBookCodeDataDict.has_key(fundCode):
                existingData = fundBookCodeDataDict.get(fundCode)
                newData = pd.merge(existingData, copiedData[['Date', fundBookCode]], how='left', on=['Date'])
                fundBookCodeDataDict[fundCode] = newData
            else:
                fundBookCodeDataDict[fundCode] = copiedData[['Date', fundBookCode]]
        return fundBookCodeDataDict

    def corrCalc(self,dateStr, fundList):
        self.initSqlServer(self.env)
        teamReturnData = self.getTeamReturn(self.inceptionDate, dateStr)
        corrResult = []
        fundBookCodeDataDict = self.calcPCTCHG(teamReturnData)
        for fundCode,data in fundBookCodeDataDict.items():
            if fundList:
                if fundCode in fundList:
                    logging.info('calc corr for fund:'+fundCode)
                else:
                    continue
            data.set_index('Date', inplace=True)
            data = data.fillna(0)
            corr = data.corr()
            colsList = corr.columns.values
            indexList = corr.index.values
            corr = corr.fillna(0)
            for col in colsList:
                for index in indexList:
                    correlationValue = corr.loc[index,col]
                    corrResult.append((self.inceptionDate, dateStr, fundCode, col, index, correlationValue))

        self.cleanCorrValues(dateStr, fundList)
        self.saveCorrValues(corrResult)
        self.closeSqlServerConnection()

    def run(self):
        currentDate = datetime.datetime.now()
        currentDateStr = currentDate.strftime('%Y-%m-%d')
        self.corrCalc(currentDateStr,[])



if __name__ == '__main__':
    env = 'prod'
    corrAnalysis = CorrAnalysis(env)
    dateStr = sys.argv[1]
    fundCodeArg = sys.argv[2]
    if not dateStr:
        logging.error('date str can not be empty')
        raise Exception('date str can not be empty')
        exit(1)

    teamList = []
    fundList = []
    if fundCodeArg != 'NULL':
        fundList = fundCodeArg.split('_')

    corrAnalysis.corrCalc(dateStr, fundList)

