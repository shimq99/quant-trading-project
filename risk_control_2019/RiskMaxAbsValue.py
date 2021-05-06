# encoding:UTF-8
from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import pandas as pd
import logging
from benchmark.marketdata.MarketDataDownloader import *
from benchmark.tools import PandasDBUtils as pdUtil
import numpy as np
import datetime
import json
from decimal import *
getcontext().prec = 6

class RiskMaxAbsValue(Base):

    def __init__(self, env):
        self.env = env
        LogManager('RiskMaxAbsValue')

    def selectFromDB(self,sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def selectFromDataBase(self, sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def insertToDatabase(self, sql, data):
        if data:
            self.cursor.executemany(sql, data)
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def cleanDataMaxMVData(self,dateStr):
        sql = ' delete from RiskDb.risk.Exposure where BookId is null and Instrument=\'MaxAbsMv\' and AsOfDate=\''+dateStr+'\''
        self.cursor.execute(sql)

    def getPositionData(self, dateStr): #db_position
        sql = 'EXEC RiskDb.risk.usp_GetRiskPosition @Date = \''+dateStr+'\''
        return self.selectFromDB(sql)

    def getFund(self):
        sql = 'SELECT FundId, FundCode FROM RiskDb.ref.Fund'
        data = self.selectFromDataBase(sql)
        return data

    def getBook(self):
        sql = 'SELECT BookId, BookCode FROM RiskDb.ref.Book'
        data = self.selectFromDataBase(sql)
        return data

    def prepareData(self, dateStr):
        position_data = self.getPositionData(dateStr)
        inst_cls = ['REITS', 'NON_REITS', 'PRFD', 'CFD_EQUITY', 'DR', 'EQTY_FT']
        valid_position_data = position_data[position_data['Inst Class (Code)'].isin(inst_cls)].copy()
        valid_position_data['AbsNotnlMVUSDDeltaAdjOption'] = valid_position_data['Notnl MV (USD)-delta adj-Option'].abs()
        valid_position_data['LongNotnlMVUSDDeltaAdjOption'] = np.where(valid_position_data['Notnl MV (USD)-delta adj-Option']>=0,valid_position_data['Notnl MV (USD)-delta adj-Option'],0)
        valid_position_data['ShortNotnlMVUSDDeltaAdjOption'] = np.where(valid_position_data['Notnl MV (USD)-delta adj-Option']<0,valid_position_data['Notnl MV (USD)-delta adj-Option'],0)
        summary_pos_data = valid_position_data.groupby(['Fund (shrt)', 'Identifier(Agg)']).agg({'AbsNotnlMVUSDDeltaAdjOption': 'sum','LongNotnlMVUSDDeltaAdjOption':'sum','ShortNotnlMVUSDDeltaAdjOption':'sum'})
        summary_pos_data = summary_pos_data.reset_index()
        summary_pos_data_w_books = valid_position_data[ (valid_position_data['Fund (shrt)'].isin(['ZJNF'])) & (valid_position_data['Book'].isin(['ZJNF-T34'])==False)].groupby(['Fund (shrt)', 'Identifier(Agg)']).agg({'AbsNotnlMVUSDDeltaAdjOption': 'sum','LongNotnlMVUSDDeltaAdjOption':'sum','ShortNotnlMVUSDDeltaAdjOption':'sum'})
        summary_pos_data_w_books = summary_pos_data_w_books.reset_index()

        return (summary_pos_data[['Fund (shrt)','AbsNotnlMVUSDDeltaAdjOption', 'LongNotnlMVUSDDeltaAdjOption','ShortNotnlMVUSDDeltaAdjOption']], summary_pos_data_w_books[['Fund (shrt)','AbsNotnlMVUSDDeltaAdjOption', 'LongNotnlMVUSDDeltaAdjOption','ShortNotnlMVUSDDeltaAdjOption']])

    def calcAndSaveMaxMVData(self, dateStr, pos_data, pos_data_w_books):
        fund_data = self.getFund()
        pos_data_abs_max_mv = pos_data.copy()
        pos_data_abs_max_mv = pos_data_abs_max_mv.groupby(['Fund (shrt)']).agg({'AbsNotnlMVUSDDeltaAdjOption': 'max', 'LongNotnlMVUSDDeltaAdjOption':'max','ShortNotnlMVUSDDeltaAdjOption':'min'})
        summary_pos_data = pos_data_abs_max_mv.reset_index()
        summary_pos_data['Date'] = dateStr
        summary_pos_data['FundCode'] = summary_pos_data['Fund (shrt)']
        summary_pos_data['Instrument'] = 'MaxAbsMv'
        summary_pos_data = pd.merge(summary_pos_data, fund_data, how='left', on=['FundCode'])
        records = pdUtil.dataFrameToSavableRecords(summary_pos_data, ['Date', 'FundId','Instrument','AbsNotnlMVUSDDeltaAdjOption', 'LongNotnlMVUSDDeltaAdjOption','ShortNotnlMVUSDDeltaAdjOption'])
        if records:
            self.cleanDataMaxMVData(dateStr)
            sql = 'insert into RiskDb.risk.Exposure(AsOfDate, FundId, Instrument,GrossNav, LongNavBeta, ShortNavBeta) values(?, ?, ?, ?, ?, ?)'
            self.insertToDatabase(sql,records)
        '''
        EXTRA: for ZJNF W books only(W01,W02,W03...., Mr Wang's book) , which is exclude T34 
        '''
        book_data = self.getBook()
        pos_data_abs_max_mv_w_books = pos_data_w_books.copy()
        pos_data_abs_max_mv_w_books = pos_data_abs_max_mv_w_books.groupby(['Fund (shrt)']).agg({'AbsNotnlMVUSDDeltaAdjOption': 'max', 'LongNotnlMVUSDDeltaAdjOption':'max','ShortNotnlMVUSDDeltaAdjOption':'min'})
        summary_pos_data_w_books = pos_data_abs_max_mv_w_books.reset_index()
        summary_pos_data_w_books['Date'] = dateStr
        summary_pos_data_w_books['FundCode'] = summary_pos_data_w_books['Fund (shrt)']
        summary_pos_data_w_books['BookCode'] = 'W'
        summary_pos_data_w_books['Instrument'] = 'MaxAbsMv'
        summary_pos_data_w_books = pd.merge(summary_pos_data_w_books, fund_data, how='left', on=['FundCode'])
        summary_pos_data_w_books = pd.merge(summary_pos_data_w_books, book_data, how='left', on=['BookCode'])
        w_books_records = pdUtil.dataFrameToSavableRecords(summary_pos_data_w_books, ['Date', 'FundId','BookId','Instrument','AbsNotnlMVUSDDeltaAdjOption', 'LongNotnlMVUSDDeltaAdjOption','ShortNotnlMVUSDDeltaAdjOption'])
        if w_books_records:
            sql = 'insert into RiskDb.risk.Exposure(AsOfDate, FundId, BookId, Instrument,GrossNav, LongNavBeta, ShortNavBeta) values(?, ?, ?, ?, ?, ?)'
            self.insertToDatabase(sql,w_books_records)


    def runWithDateRange(self,dateStr):
        self.initSqlServer(self.env)
        summary_pos_data,summary_pos_data_w_books = self.prepareData(dateStr)
        self.calcAndSaveMaxMVData(dateStr, summary_pos_data, summary_pos_data_w_books)
        self.closeSqlServerConnection()

    def run(self):
        currentDate = datetime.datetime.now()
        weekDay = currentDate.date().weekday()
        diff = 1
        if (weekDay == 5 or weekDay == 6):
            return
        elif (weekDay == 0):
            diff += 2
        runYesterDay = currentDate - datetime.timedelta(days=diff)
        runYesterDayStr = runYesterDay.strftime('%Y-%m-%d')
        self.runWithDateRange(runYesterDayStr)
if __name__ == '__main__':
    env = 'prod'
    riskMaxAbsValue = RiskMaxAbsValue(env)
    riskMaxAbsValue.runWithDateRange('2019-05-08')
