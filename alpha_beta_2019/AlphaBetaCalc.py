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

class AlphaBetaCalc(Base):
    def __init__(self, env):
        self.bbgAddr = ''
        self.env = env
        LogManager('AlphaBetaCalc')
        self.inceptionDate='2019-12-31'
        self.alphaBeginDate = '2020-01-01'
        self.calcYear = '2020'
        # self.inceptionDate = '2018-12-31'
        # self.alphaBeginDate = '2019-01-01'
        # self.calcYear = '2019'
        self.initPortfolioValue = 100000000

    def updateToDataBase(self, sql, updateRecords):
        if updateRecords:
            self.cursor.executemany(sql, updateRecords)

    def selectFromDataBase(self,sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def insertToDatabase(self,sql,data):
        if data:
            try:
                self.cursor.executemany(sql, data)
            except pyodbc.IntegrityError, e:
                logging.warning(
                    'insertToDatabase: integrity error while saving record, will ignore: ' + e.message + e.args[1])
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def savePortfolioConstValues(self,portfolioConstValueRecords):
        if portfolioConstValueRecords:
            sql = 'insert into RiskDb.bench.BenchmarkPortfolioConstEodPrice(PortfolioCode,BbgTicker,TradeDate,CloseValue,PctChange,Memo) values(?,?,?,?,?,?)'
            self.insertToDatabase(sql,portfolioConstValueRecords)
        else:
            logging.warn('savePortfolioConstValues: empty record')

    def savePortfolioValues(self,portfolioValueRecords):
        if portfolioValueRecords:
            sql = 'insert into RiskDb.bench.BenchmarkPortfolioEodPrice(PortfolioCode,TradeDate,CloseValue) values(?,?,?)'
            self.insertToDatabase(sql, portfolioValueRecords)
        else:
            logging.warn('savePortfolioValues: empty record')

    def saveAndCalcYTDReturnPortfolioValues(self, portfolioValueRecords, portfolioCodeList):
        sql= 'select PortfolioCode,CloseValue as InitCloseValue from RiskDb.bench.BenchmarkPortfolioEodPrice where TradeDate=\''+self.inceptionDate+'\' and PortfolioCode in (\'' + ('\',\'').join(portfolioCodeList) + '\')'
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        portfolioValueRecordData = pd.DataFrame(portfolioValueRecords, columns=['PortfolioCode', 'TradeDate', 'CloseValue'])
        joinedData = pd.merge(portfolioValueRecordData, resultDataFrame, how='left', on=['PortfolioCode'])
        joinedData['YTDReturn'] = joinedData['CloseValue'].astype(float) / joinedData['InitCloseValue'].astype(float) - 1
        records = pdUtil.dataFrameToSavableRecords(joinedData, ['PortfolioCode', 'TradeDate', 'CloseValue', 'YTDReturn'])
        self.savePortfolioValues(records)

    def getAbnormalInceptionDateTeamInfo(self):
        sql = 'SELECT distinct PortfolioCode, InceptionDate FROM RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and  InceptionDate !=\''+self.inceptionDate+'\''
        data = self.selectFromDataBase(sql)
        data['TeamCode'] = data['PortfolioCode'].str.replace(self.calcYear,'')
        data['InceptionDate'] = pd.to_datetime(data['InceptionDate'])
        data['InceptionDateStr'] = data['InceptionDate'].dt.strftime('%Y-%m-%d')
        del data['InceptionDate']
        del data['PortfolioCode']
        dict_data = data.set_index('TeamCode').T.to_dict('list')
        return dict_data

    def removeAlphaBeta(self, dateStr, teamIdList, fundIdList, valueType, reRun=False):
        if dateStr:
            logging.info('removing alpha beta for date:'+dateStr)
            try:
                sql = 'delete from RiskDb.bench.AlphaBeta where EndDate=\''+dateStr+'\' and ValueType='+str(valueType)
                if teamIdList and reRun:
                    sql +=' and BookId in (' + (',').join(teamIdList) + ')'
                    if fundIdList:
                        sql +=' and FundId in (' + (',').join(fundIdList) + ')'
                self.cursor.execute(sql)
            except Exception,e:
                logging.error('removeAlphaBeta error:'+e.args[1])
        else:
            logging.warn('removeAlphaBeta: date is empty')

    def removeFundAlphaBeta(self, dateStr, fundIdList, valueType):
        if dateStr:
            logging.info('removing fund alpha beta for date:'+dateStr)
            try:
                sql = 'delete from RiskDb.bench.AlphaBeta where BookId is null and EndDate=\''+dateStr+'\' and ValueType='+str(valueType)
                if fundIdList:
                    sql +=' and FundId in (' + (',').join(fundIdList) + ')'
                self.cursor.execute(sql)
            except Exception,e:
                logging.error('removeFundAlphaBeta error:'+e.args[1])
        else:
            logging.warn('removeFundAlphaBeta: date is empty')

    def getAlphaBeta(self, dateStr, fundId, bookId):
        date = datetime.datetime.strptime(dateStr, '%Y-%m-%d')
        startDate = str(date.year)+'-01-01'
        sql = 'select * from RiskDb.bench.AlphaBeta where EndDate between \''+startDate+'\' and \''+dateStr+'\' and FundId='+str(fundId) + ' and BookId='+str(bookId)
        if bookId==0:
            sql = 'select * from RiskDb.bench.AlphaBeta where EndDate between \'' + startDate + '\' and \'' + dateStr + '\' and FundId=' + str(fundId) + ' and BookId is null'
        data = self.selectFromDataBase(sql)
        return data

    def saveAlphaBeta(self, alphaBetaRecords):
        if alphaBetaRecords:
            logging.info('saving alpha beta data')
            sql = 'insert into RiskDb.bench.AlphaBeta (BeginDate, EndDate, FundId, BookId, BechmarkCode, Alpha, Beta, RSquared, BenchmarkYTDAdjusted, ValueType, RelReturn, ' \
                  'RelReturnMaxDD, RelReturnRecovery, RelReturnMaxDDFrom, RelReturnMaxDDTo, RelReturnCurDD, RelReturnHistHigh) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, alphaBetaRecords)
        else:
            logging.warn('saveAlphaBeta: empty record')

    def saveToBenchmarkIndexEodPrice(self, indexPctRecords):
        if indexPctRecords:
            logging.info('saving saveToBenchmarkIndexEodPrice')
            sql = 'insert into RiskDb.bench.BenchmarkIndexEodPrice(IndexCode, TradeDate,ClosePrice) values (?,?,?)'
            self.insertToDatabase(sql, indexPctRecords)
        else:
            logging.warn('saveToBenchmarkIndexEodPrice: empty record')

    def saveIndexPctChange(self, indexPctRecords):
        if indexPctRecords:
            logging.info('saving saveIndexPctChange')
            sql = 'insert into RiskDb.bench.BenchmarkIndexEodPrice(IndexCode, TradeDate, PctChange,ClosePrice) values (?,?,?,?)'
            self.insertToDatabase(sql,indexPctRecords)
        else:
            logging.warn('saveIndexPctChange: empty record')

    def saveFundAlphaBeta(self, alphaBetaRecords):
        if alphaBetaRecords:
            logging.info('saving alpha beta data for fund')
            sql = 'insert into RiskDb.bench.AlphaBeta (BeginDate, EndDate, FundId, BechmarkCode, Alpha, Beta, RSquared, BenchmarkYTDAdjusted, ValueType, RelReturn,' \
                  'RelReturnMaxDD,RelReturnRecovery,RelReturnMaxDDFrom,RelReturnMaxDDTo, RelReturnCurDD, RelReturnHistHigh) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, alphaBetaRecords)
        else:
            logging.warn('saveAlphaBeta: empty record')

    def initBenchmarkPortfolioWeight(self, filePath, team, inceptionDateStr):
        data = pd.read_excel(filePath,sheet_name='Worksheet')
        data['TeamCode'] = team
        data['PortfolioCode'] = self.calcYear+team
        data['Weight'] = data['% Wgt (P)']
        data['ConstituentTicker'] = data['Ticker']+' Equity'
        data['InceptionDate'] = pd.to_datetime(inceptionDateStr,format='%Y-%m-%d')
        data['HoldingShares'] = data['Pos (Disp) (P)']
        data['InceptionDatePrice'] = data['Px Close (P)']
        data['Currency'] = data['Crncy']
        data['PortfolioType'] = PortfolioType.BOOK.value
        data['EffectiveYear'] = self.calcYear

        self.initSqlServer(self.env)
        #BenchmarkPortfolioWeight
        records = pdUtil.dataFrameToSavableRecords(data,['PortfolioCode','ConstituentTicker','Weight','HoldingShares','InceptionDate','InceptionDatePrice','TeamCode','Currency','PortfolioType','EffectiveYear'])
        sql = 'insert into RiskDb.bench.BenchmarkPortfolioWeight(PortfolioCode,ConstituentTicker,Weight,HoldingShares,InceptionDate,InceptionDatePrice,TeamCode,Currency,PortfolioType,EffectiveYear) values(?,?,?,?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, records)


        ##BenchmarkPortfolioConstEodPrice
        data['CloseValue'] = data['Mkt Val (P)']
        data['Memo'] = 'Init'
        sql2 = 'insert into RiskDb.bench.BenchmarkPortfolioConstEodPrice(PortfolioCode,BbgTicker,TradeDate,CloseValue,Memo) values(?, ?, ?, ?, ?)'
        self.insertToDatabase(sql2, data[['PortfolioCode','ConstituentTicker','InceptionDate','CloseValue','Memo']].values.tolist())

        ##BenchmarkPortfolioEodPrice
        records=[]
        sql3 = 'insert into RiskDb.bench.BenchmarkPortfolioEodPrice(PortfolioCode,TradeDate,CloseValue,Memo) values(?, ?, ?, ?)'
        records.append((self.calcYear+team,inceptionDateStr,self.initPortfolioValue,'Init'))
        self.insertToDatabase(sql3, records)

        ##[BenchmarkIndexEodPrice]
        records = []
        sql4 = 'insert into RiskDb.bench.BenchmarkIndexEodPrice(IndexCode,TradeDate,ClosePrice) values(?, ?, ?)'
        records.append((self.calcYear + team, inceptionDateStr, self.initPortfolioValue))
        self.insertToDatabase(sql4, records)
        self.closeSqlServerConnection()

    def initFundBenchmarkPortfolioWeight(self,filePath):
        data = pd.read_excel(filePath)
        data['Memo'] = 'Init'
        data['PortfolioType'] = PortfolioType.FUND.value
        data['InceptionDate'] = pd.to_datetime(self.inceptionDate, format='%Y-%m-%d')
        records = pdUtil.dataFrameToSavableRecords(data,['Portfolio', 'Ticker', 'Weight', 'InceptionDate', 'Fund', 'FundCode', 'Currency', 'Memo', 'PortfolioType'])
        sql = 'insert into RiskDb.bench.BenchmarkPortfolioWeight(PortfolioCode, ConstituentTicker, Weight, InceptionDate, FundId, TeamCode, Currency, Memo,PortfolioType) values(?,?,?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, records)


    def getTickers(self,teamList):
        sql = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and PortfolioType=' + str(PortfolioType.BOOK.value)
        if teamList:
            sql += ' and TeamCode in (\'' + ('\',\'').join(teamList)+'\')'
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return list(resultDataFrame['ConstituentTicker'].unique())

    def getIndexTickers(self,fundList):
        sql = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and PortfolioType=' + str(PortfolioType.FUND.value)
        if fundList:
            sql += ' and TeamCode in (\'' + ('\',\'').join(fundList)+'\')'
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return list(resultDataFrame['ConstituentTicker'].unique())

    def getIndexTickersFromTeam(self):
        sql = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and PortfolioType=' + str(PortfolioType.BOOK_BENCHMAR_WITH_INDEX.value)
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return list(resultDataFrame['ConstituentTicker'].unique())
    #
    # def calcInitHolding(self):
    #     sql = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where TeamCode=\'T09\''
    #     data = self.selectFromDataBase(sql)
    #     data['HoldingShares'] = (self.initPortfolioValue*data['Weight'])/data['InceptionDatePrice']
    #     data['StockValues'] = data['HoldingShares'] * data['InceptionDatePrice']
    #     data['HoldingShares'] = data['HoldingShares'].astype('float')
    #     records = pdUtil.dataFrameToSavableRecords(data,['HoldingShares', 'ConstituentTicker', 'TeamCode'])
    #     sql = 'update RiskDb.bench.BenchmarkPortfolioWeight set HoldingShares=? where ConstituentTicker=? and TeamCode=?'
    #     self.updateToDataBase(sql, records)

    def getStockCashDividentFromDb(self, startDayStr, stopDayStr):
        stockCashDividentDict = dict()
        sql = 'select BbgTicker, PayableDate, DividentAmount, PayDayClosePrice, ExDate from RiskDb.bench.BbgCashDividentInfo where ExDate between ? and ?'
        self.cursor.execute(sql, (startDayStr, stopDayStr))

        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        stockCashDividentDataframe = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        for row in allResult:
            try:
                key = row[0] + '_' + row[4].strftime('%Y-%m-%d')
                if stockCashDividentDict.has_key(key):
                    stockCashDividentDict[key] += 999.0
                else:
                    stockCashDividentDict[key] = 999.0
            except Exception, e:
                logging.error('error while calculate Cash Dividend, pls data for ticker_date:' + key)
                raise Exception('error while calculate Cash Dividend, pls data for ticker_date:' + key)

        return stockCashDividentDict,stockCashDividentDataframe

    def getStockDvdSplitInfo(self,startDayStr):
        sql = 'select BbgTicker, AdjustmentDate, AdjustmentFactor, OperatorType, Flag from RiskDb.bench.BbgTickerDvdSplitInfo \
                       where AdjustmentDate >= ?'
        self.cursor.execute(sql, startDayStr)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        stockDvdSplitInfo = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        stockDvdSplitInfo['AdjustmentDate'] = pd.to_datetime(stockDvdSplitInfo['AdjustmentDate'])
        stockDvdSplitInfo['Key'] = stockDvdSplitInfo['BbgTicker'] + '_' + stockDvdSplitInfo['AdjustmentDate'].dt.strftime('%Y-%m-%d')
        stockDvdSplitInfo['AdjustmentFactor'] = stockDvdSplitInfo['AdjustmentFactor'].astype(Decimal)
        return stockDvdSplitInfo

    def getTickersDataWithDate(self, tickers, startDayStr,endDayStr):
        sql = 'select * from RiskDb.bench.BbgTickerEodPrice where BbgTicker in (\'' + ('\',\'').join(tickers) + '\') and TradeDate between \''+startDayStr+'\' and \''+endDayStr+'\''
        tickersData = self.selectFromDataBase(sql)
        tickersData['TradeDate'] = pd.to_datetime(tickersData['TradeDate'])
        tickersData['TradeDateStr'] = tickersData['TradeDate'].dt.strftime('%Y-%m-%d')
        return tickersData
    '''
        getTickersFullMarketData: in case any equity lack of PX data from bloomberg, will fullfill with most recent price
    '''
    def getTickersFullMarketData(self,tickers, dateStr):
        sql = 'select BbgTicker,TradeDate,ClosePrice,FloatVolume from RiskDb.bench.BbgTickerEodPrice where BbgTicker in (\'' + ('\',\'').join(tickers) + '\') and TradeDate between \'' + self.inceptionDate + '\' and \'' + dateStr + '\''
        tickersData = self.selectFromDataBase(sql)
        tickersData['TradeDate'] = pd.to_datetime(tickersData['TradeDate'])
        tickersData['ClosePrice'] = tickersData['ClosePrice'].astype(Decimal)
        for ticker in tickers:
            fullTradeDateData = pd.DataFrame(pd.date_range(self.inceptionDate, dateStr, freq='B'),
                                             index=[pd.date_range(self.inceptionDate, dateStr, freq='B')],
                                             columns=['TradeDate']
                                             )
            tickerFullTradeDateData = pd.merge(fullTradeDateData, tickersData[tickersData['BbgTicker'] == ticker], how='left', on=['TradeDate'])
            tickerFullTradeDateData.sort_index(inplace=True)
            tickerFullTradeDateData['BbgTicker'] = ticker
            tickerFullTradeDateData['ClosePrice'] = tickerFullTradeDateData['ClosePrice'].fillna(method='ffill')
            tickerFullTradeDateData['FloatVolume'] = tickerFullTradeDateData['FloatVolume'].fillna(method='ffill')
            tickerFullTradeDateData['TradeDateStr'] = tickerFullTradeDateData['TradeDate'].dt.strftime('%Y-%m-%d')
            tickersData = pd.concat([tickersData[tickersData['BbgTicker'] != ticker], tickerFullTradeDateData], axis=0, sort=True)
        return tickersData


    def getBenchmarkPortforlioWegithInfoData(self, teamList):
        sql2 = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and TeamCode in (\'' + ('\',\'').join(teamList) + '\') and PortfolioType='+str(PortfolioType.BOOK.value)
        teamsData = self.selectFromDataBase(sql2)
        teamsData['BbgTicker'] = teamsData['ConstituentTicker']
        teamsData['HoldingShares'] = teamsData['HoldingShares'].astype(Decimal)
        teamsData['InceptionDatePrice'] = teamsData['InceptionDatePrice'].astype(Decimal)
        teamsData['Weight'] = teamsData['Weight'].astype(Decimal)
        return teamsData

    def getFundBenchmarkPortforlioWegithInfoData(self, fundList):
        if fundList:
            sql2 = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and TeamCode in (\'' + ('\',\'').join(fundList) + '\') and PortfolioType='+str(PortfolioType.FUND.value)
        else:
            sql2 = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and PortfolioType='+str(PortfolioType.FUND.value)
        fundWeightingData = self.selectFromDataBase(sql2)
        fundWeightingData['IndexCode'] = fundWeightingData['ConstituentTicker']
        fundWeightingData['FundCode'] = fundWeightingData['TeamCode']
        fundWeightingData['InceptionDatePrice'] = fundWeightingData['InceptionDatePrice'].astype(Decimal)
        fundWeightingData['Weight'] = fundWeightingData['Weight'].astype(Decimal)
        return fundWeightingData

    def getTeamBenchmarkTypeIndexWegithInfoData(self, teamList):
        if teamList:
            sql2 = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and TeamCode in (\'' + ('\',\'').join(teamList) + '\') and PortfolioType='+str(PortfolioType.BOOK_BENCHMAR_WITH_INDEX.value)
        else:
            sql2 = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and PortfolioType='+str(PortfolioType.BOOK_BENCHMAR_WITH_INDEX.value)
        fundWeightingData = self.selectFromDataBase(sql2)
        fundWeightingData['IndexCode'] = fundWeightingData['ConstituentTicker']
        fundWeightingData['BookCode'] = fundWeightingData['TeamCode']
        fundWeightingData['InceptionDatePrice'] = fundWeightingData['InceptionDatePrice'].astype(Decimal)
        fundWeightingData['Weight'] = fundWeightingData['Weight'].astype(Decimal)
        return fundWeightingData


    def updateBenchmarkPortfolioWeightInfoData(self,updateRecords):
        sql = 'update RiskDb.bench.BenchmarkPortfolioWeight set HoldingShares=?,Memo=? where PortfolioCode=? and ConstituentTicker=?'
        self.updateToDataBase(sql, updateRecords)

    def getCurrencyInfoData(self, dateStr):
        sql2 = 'SELECT a.CurrencyCode,a.TradeDate,a.ClosePrice,b.PriceSize FROM RiskDb.bench.CurrencyEodPrice a left join RiskDb.ref.CurrencyInfo b on a.CurrencyCode=b.CurrencyCode where a.TradeDate between \''+dateStr+'\' and \''+dateStr+'\' and a.CurrencyCode like \'%USD Curncy\''
        currencyData = self.selectFromDataBase(sql2)
        if currencyData.empty:
            raise Exception('getCurrencyInfoData: there is not currency data for date:'+dateStr)
        currencyData['CurrencyClosePrice'] = currencyData['ClosePrice'].astype(Decimal) / currencyData['PriceSize']
        currencyData['CurrencyTradeDate'] = currencyData['TradeDate']
        del currencyData['ClosePrice']
        del currencyData['TradeDate']
        return currencyData

    def getAllCurrencyInfoData(self, dateStr):
        sql2 = 'SELECT a.CurrencyCode,a.TradeDate,a.ClosePrice,b.PriceSize FROM RiskDb.bench.CurrencyEodPrice a left join RiskDb.ref.CurrencyInfo b on a.CurrencyCode=b.CurrencyCode where a.TradeDate between \'' + dateStr + '\' and \'' + dateStr + '\''
        currencyDataForCheck = self.selectFromDataBase(sql2)
        if currencyDataForCheck.empty:
            raise Exception('getCurrencyInfoData: there is not currency data for date:' + dateStr)
        checkNA = currencyDataForCheck[np.isnan(currencyDataForCheck['PriceSize'].astype('float'))]
        if not checkNA.empty:
            tickers = list(checkNA['CurrencyCode'].unique())
            logging.info('download Quotation Factor for tickers:'+(',').join(tickers))
            mdService = MarketDataDownloader(self.env)
            mdService.initSqlServer(self.env)
            mdService.getFXQuoteFactorFromBBG(tickers)
            mdService.closeSqlServerConnection()
        currencyData = self.selectFromDataBase(sql2)
        if currencyData.empty:
            raise Exception('getCurrencyInfoData: there is not currency data for date:' + dateStr)
        currencyData['CurrencyClosePrice'] = currencyData['ClosePrice'].astype(Decimal) / currencyData['PriceSize']
        currencyData['CurrencyTradeDate'] = currencyData['TradeDate']
        del currencyData['ClosePrice']
        del currencyData['TradeDate']
        return currencyData

    def getBenchmarkPortfolioEODPriceData(self, endDayStr):
        sql2 = 'SELECT PortfolioCode,TradeDate,CloseValue,Memo FROM RiskDb.bench.BenchmarkPortfolioEodPrice where TradeDate between \''+self.inceptionDate+'\' and \''+endDayStr+'\''
        teamsData = self.selectFromDataBase(sql2)
        teamsData['CloseValue'] = teamsData['CloseValue'].astype(Decimal)
        return teamsData

    def getIndexEODPriceData(self, indexList, endDayStr):
        sql2 = 'SELECT IndexCode,TradeDate,ClosePrice FROM RiskDb.bench.BenchmarkIndexEodPrice where TradeDate between \''+self.inceptionDate+'\' and \''+endDayStr+'\' and IndexCode in (\'' + ('\',\'').join(indexList) + '\')'
        indexData = self.selectFromDataBase(sql2)
        return indexData

    def getBenchmarkPortfolioConstEODValueData(self, startDayStr,endDayStr):
        sql = 'SELECT PortfolioCode,BbgTicker,TradeDate,CloseValue,PctChange,Memo,LastUpdatedOn,LastUpdatedBy FROM RiskDb.bench.BenchmarkPortfolioConstEodPrice where TradeDate between \''+startDayStr+'\' and \''+endDayStr+'\''
        teamsData = self.selectFromDataBase(sql)
        teamsData['TradeDate'] = pd.to_datetime(teamsData['TradeDate'])
        teamsData['TradeDateStr'] = teamsData['TradeDate'].dt.strftime('%Y-%m-%d')
        return teamsData

    def filterWeekendData(self, teamReturnData):
        teamReturnData['Date'] = pd.to_datetime(teamReturnData['Date'])
        teamReturnData['Weekday'] = teamReturnData['Date'].dt.dayofweek
        teamReturnData = teamReturnData[(teamReturnData['Weekday']!=5) & (teamReturnData['Weekday']!=6)]
        del teamReturnData['Weekday']
        return teamReturnData

    def getTeamReturn(self,startDayStr,stopDayStr):
        #sql = 'select FundBookCode,Date,YtdGrossReturn,a.FundId,a.BookId from Portfolio.perf.Nav as a join ReferenceData.ref.FundBook as b on a.FundId=b.FundId and a.BookId=b.BookId where Date between \''+startDayStr+'\' and \''+stopDayStr+'\' order by Date'
        sql = 'SELECT Fund,FundId,Book,BookId,Date,YtdGrossReturn FROM Portfolio.perf.NavView where Date between \'' + startDayStr + '\' and \'' + stopDayStr + '\' order by Date'
        teamReturnData = self.selectFromDataBase(sql)
        teamReturnData = teamReturnData[teamReturnData['Fund']!=teamReturnData['Book']]
        teamReturnData['FundBookCode'] = teamReturnData['Fund']+'-'+teamReturnData['Book']
        teamReturnData = self.filterWeekendData(teamReturnData)
        return teamReturnData

    def getFundReturn(self,startDayStr,stopDayStr):
        sql = 'select Date,YtdGrossReturn,a.FundId,b.FundCode,b.FundDesc from Portfolio.perf.Nav as a join RiskDb.ref.Fund as b on a.FundId=b.FundId where a.BookId is null and a.Date between \''+startDayStr+'\' and \''+stopDayStr+'\' order by Date'
        fundReturnData = self.selectFromDataBase(sql)
        return fundReturnData

    def getTickersCurrencyInfo(self):
        sql = 'SELECT BbgTicker,TickerCurrencyID,B.CurrencyCode as TickerCurrencyCode,DvdCurrencyID,C.CurrencyCode as DvdCurrencyCode FROM RiskDb.bench.BbgTickerCurrencyInfo A left join RiskDb.ref.Currency B on B.CurrencyId=A.TickerCurrencyID left join RiskDb.ref.Currency C on C.CurrencyId=A.DvdCurrencyID'
        tickersCurrencyInfo = self.selectFromDataBase(sql)
        return tickersCurrencyInfo

    def calcPortfolioValueAndPctChange(self, teamList, tickers, dateStr):
        allTickersCurrencyInfoData = self.getTickersCurrencyInfo()
        tickersFullMarketData = self.getTickersFullMarketData(tickers, dateStr)
        currecnyInfoData = self.getCurrencyInfoData(dateStr)
        teamsWeightData = self.getBenchmarkPortforlioWegithInfoData(teamList)
        stocksDvdSplitInfoData = self.getStockDvdSplitInfo(dateStr)
        (stockCashDividentDict, stockCashDividentDataframe) = self.getStockCashDividentFromDb(dateStr,dateStr) #get same day data
        portfolioConstPriceData = self.getBenchmarkPortfolioConstEODValueData(self.inceptionDate,dateStr)
        portfolioValueRecords=[]
        portfolioCodeList = []
        updatePortfolioWeightInfoRecords = []  ##records for updates like: stock split, need to change init holding shares
        for team in teamList:
            logging.info('calculating Portfolio Value for team:'+team)
            #allCurrecnyInfoData = self.getAllCurrencyInfoData(dateStr)
            indTeamData = teamsWeightData[teamsWeightData['TeamCode'] == team]
            tickers = list(indTeamData['BbgTicker'].unique())
            portfolioConstValueRecords = []
            portfolioCode = self.calcYear + team
            teamPortfolioConstPriceData = portfolioConstPriceData[portfolioConstPriceData['PortfolioCode']==portfolioCode]
            for ticker in tickers:
                memo = dict()
                allCurrecnyInfoData = self.getAllCurrencyInfoData(dateStr)
                tickerDvdSplitData = stocksDvdSplitInfoData[stocksDvdSplitInfoData['Key']==ticker+'_'+dateStr]
                tickerData = tickersFullMarketData[tickersFullMarketData['BbgTicker'] == ticker]
                tickerData['PreClosePrice'] = tickerData['ClosePrice'].shift(1)
                tickerData = pd.merge(indTeamData[indTeamData['BbgTicker']==ticker],tickerData, how='left', on=['BbgTicker']).copy()
                tickerData['CurrencyCode'] = tickerData['Currency']+'USD Curncy'
                tickerData = pd.merge(tickerData, currecnyInfoData, how='left',on=['CurrencyCode']).copy()
                tickerData = pd.merge(tickerData, teamPortfolioConstPriceData[['BbgTicker','TradeDateStr','CloseValue']], how='left',on=['BbgTicker','TradeDateStr']).copy()
                tickerData['ClosePriceLocalCurrency'] = tickerData['ClosePrice'].copy()
                tickerData['ClosePrice'] = np.where(tickerData['Currency'] == 'USD',tickerData['ClosePrice'],tickerData['ClosePrice']*tickerData['CurrencyClosePrice'])
                currenClosePrice = tickerData[tickerData['TradeDateStr'] == dateStr]['ClosePrice'].iloc[-1]
                currenHolding = tickerData[tickerData['TradeDateStr'] == dateStr]['HoldingShares'].iloc[-1]
                currentCloseValue = tickerData[tickerData['TradeDateStr'] == dateStr]['ClosePrice'].iloc[-1] * tickerData[tickerData['TradeDateStr'] == dateStr]['HoldingShares'].iloc[-1]
                currentTickerPctChange = currentCloseValue/tickerData['CloseValue'].astype(Decimal).iloc[-2] - Decimal('1')
                if not tickerDvdSplitData.empty:
                    splitInfoMemo = ''
                    '''
                    Operator Type (1=div, 2=mult, 3=add, 4=sub. Opposite for Volume)
                    Flag (1=prices only, 2=volumes only, 3=prices and volumes)
                    '''
                    adjFactor = tickerDvdSplitData['AdjustmentFactor'].iloc[0]
                    operatorType = tickerDvdSplitData['OperatorType'].iloc[0]
                    flag = tickerDvdSplitData['Flag'].iloc[0]

                    if flag == 1: ##只调整price,说明当天拿到价格已经是调整过的，因为需把前一天的价格也按照adjfactor进行调整，再算出前一天的总value（adjustedPreviousCurrentValue）
                        adjustedPreClosePrice = self.adjustPriceForSplit(adjFactor, dateStr, operatorType, tickerData)
                        adjustedPreviousCurrentValue = adjustedPreClosePrice * tickerData['HoldingShares'].iloc[-1]
                        currentTickerPctChange = currentCloseValue / adjustedPreviousCurrentValue - 1
                        memo['DvdSplitEvent'] = 'StockSplitPrice:'+ticker+',AdjustedPreClosePrice:' + str(adjustedPreClosePrice) + ',AdjustedPreviousCurrentValue:' +str(adjustedPreviousCurrentValue)+', Date:'+ dateStr
                    elif flag == 2: ##只调整volumes,即当前在数据库中的volumes(holdingshares)还未调整，故计算不用改变，只需在算完pct change后，更新holdingshares(表：BenchmarkPortfolioWeight)
                        adjustHoldingShares = (tickerData[tickerData['TradeDateStr'] == dateStr]['HoldingShares'].iloc[-1])
                        if operatorType == 1:   ###Opposite for Volume
                            adjustHoldingShares *= adjFactor
                        elif operatorType == 2: ###Opposite for Volume
                            adjustHoldingShares /= adjFactor
                        elif operatorType == 3: ###Opposite for Volume
                            adjustHoldingShares -= adjFactor
                        elif operatorType == 4: ###Opposite for Volume
                            adjustHoldingShares += adjFactor
                        beforeHolding = tickerData[tickerData['TradeDateStr'] == dateStr]['HoldingShares'].iloc[-1]
                        portfolioWeightMemo = 'StockSplitVolumes:'+ticker+', AdjustedHoldingShares:' + str(adjustHoldingShares) + ',BeforeHoldingShares:' +str(beforeHolding)+', Date:'+ dateStr
                        memo['DvdSplitEvent'] = portfolioWeightMemo
                        updatePortfolioWeightInfoRecords.append((str(adjustHoldingShares.quantize(Decimal('0.00000000'))),portfolioWeightMemo, portfolioCode,ticker))
                    elif flag == 3:##同时调整price和volumes,即总market value(因为price和volumes的调整是相反的，故相乘后的value还是保持不变的）。故用计算currentValue = 当天获取的price(即是调整后的）* 新的holdingshares
                        adjustHoldingShares = (tickerData[tickerData['TradeDateStr'] == dateStr]['HoldingShares'].iloc[-1])
                        if operatorType == 1:  ###Opposite for Volume
                            adjustHoldingShares *= adjFactor
                        elif operatorType == 2:  ###Opposite for Volume
                            adjustHoldingShares /= adjFactor
                        elif operatorType == 3:  ###Opposite for Volume
                            adjustHoldingShares -= adjFactor
                        elif operatorType == 4:  ###Opposite for Volume
                            adjustHoldingShares += adjFactor

                        currentCloseValue = tickerData[tickerData['TradeDateStr'] == dateStr]['ClosePrice'].iloc[-1] * adjustHoldingShares
                        currentTickerPctChange = currentCloseValue / tickerData['CloseValue'].astype(Decimal).iloc[-2] - Decimal('1')

                        beforeHolding = tickerData[tickerData['TradeDateStr'] == dateStr]['HoldingShares'].iloc[-1]
                        portfolioWeightMemo = 'StockSplitPriceAndVolumes:'+ticker+', AdjustedHoldingShares:' + str(adjustHoldingShares) + ',BeforeHoldingShares:' + str(beforeHolding) + ', Date:' + dateStr
                        memo['DvdSplitEvent'] = portfolioWeightMemo
                        updatePortfolioWeightInfoRecords.append((str(adjustHoldingShares.quantize(Decimal('0.00000000'))), portfolioWeightMemo, portfolioCode, ticker))

                if stockCashDividentDict.has_key(ticker+'_'+dateStr):
                    '''
                       当有cash div时，默认是reinvest, 所以reinvest时应注意dvd currency 和stock currency是否一致
                       有些equity的dvd currency跟stock currency不一致时，计算时需验证。
                       如： THBEV SP Equity, dvd currency=THB  stock currency = SGD
                       
                       同时需注意，因BBG返回DVD HIST时，只返回Net Amount数据，但BBG PORT的自动 REINVEST是根据Gross Amount来计算新增pos，
                       所以会看到本程序的结果与BBG的结果有一些差距。 如：ADVANC TB Equity 在2019-02-20的cash div中就与BBG有800shares差距，其原因就在于gross amount=3.3,但net amound=2.97
                    '''
                    tickerCurrncyInfo = allTickersCurrencyInfoData[(allTickersCurrencyInfoData['BbgTicker'] == ticker)
                                                                   &(allTickersCurrencyInfoData['TickerCurrencyCode'] != allTickersCurrencyInfoData['DvdCurrencyCode']) ].copy()
                    dvdToStockCurrencyFxRate = 1
                    if not tickerCurrncyInfo.empty:
                        dvdToStockCurrncy = tickerCurrncyInfo['DvdCurrencyCode'].iloc[0] +tickerCurrncyInfo['TickerCurrencyCode'].iloc[0] +' Curncy'
                        allCurrecnyInfoData['CurrencyTradeDate'] = pd.to_datetime(allCurrecnyInfoData['CurrencyTradeDate'])
                        allCurrecnyInfoData['CurrencyTradeDateStr'] = allCurrecnyInfoData['CurrencyTradeDate'].dt.strftime('%Y-%m-%d')
                        dvdToStockCurrncyFxData = allCurrecnyInfoData[(allCurrecnyInfoData['CurrencyCode']==dvdToStockCurrncy)
                                                                   & (allCurrecnyInfoData['CurrencyTradeDateStr']==dateStr)]
                        if dvdToStockCurrncyFxData.empty:
                            mdService = MarketDataDownloader(self.env)
                            mdService.initSqlServer(self.env)
                            fxRecord = mdService.getCurrencEodPrice(dateStr, dateStr,[dvdToStockCurrncy])
                            mdService.closeSqlServerConnection()
                            dvdToStockCurrencyFxRate = float(fxRecord[0][2])
                        else:
                            dvdToStockCurrencyFxRate = dvdToStockCurrncyFxData['CurrencyClosePrice'].astype(float).iloc[0]

                    tickerCashDvdData = stockCashDividentDataframe[(stockCashDividentDataframe['BbgTicker'] == ticker) & (stockCashDividentDataframe['ExDate'] == dateStr)]
                    cashDvdTotal = tickerCashDvdData['DividentAmount'].astype(float).sum()
                    holding = tickerData[tickerData['TradeDateStr'] == dateStr]['HoldingShares'].astype(float).iloc[-1]

                    cashDvdReinvestTotalValue = cashDvdTotal * holding * dvdToStockCurrencyFxRate
                    deltaPos = cashDvdReinvestTotalValue / float(tickerData[tickerData['TradeDateStr'] == dateStr]['ClosePriceLocalCurrency'].iloc[-1])
                    newHolding = Decimal(holding + deltaPos)
                    portfolioWeightMemo = 'StockCashDivEvent:' + ticker + ', AdjustedHoldingShares:' + str(newHolding) + ',BeforeHoldingShares:' + str(holding) + ', Date:' + dateStr
                    updatePortfolioWeightInfoRecords.append((str(newHolding.quantize(Decimal('0.00000000'))), portfolioWeightMemo, portfolioCode, ticker))

                    preCurrentCloseValue = str(currentCloseValue)
                    tickerCurrency = tickerData[tickerData['TradeDateStr'] == dateStr]['Currency'].iloc[-1]

                    currentCloseValue = newHolding * tickerData[tickerData['TradeDateStr'] == dateStr]['ClosePrice'].iloc[-1]

                    currentTickerPctChangeOld = str(currentTickerPctChange)
                    currentTickerPctChange = currentCloseValue / tickerData['CloseValue'].astype(Decimal).iloc[-2] - Decimal('1')
                    deltaPctChange =  currentTickerPctChange - Decimal(currentTickerPctChangeOld)
                    memo['DvdCashEvent'] = 'PctChange add with' + str(deltaPctChange) + ' currentCloseValue old:' + preCurrentCloseValue + ' new:' + str(currentCloseValue)
                currentDate = pd.to_datetime(dateStr, format='%Y-%m-%d')
                if memo:
                    portfolioConstValueRecords.append((portfolioCode, ticker, currentDate, str(currentCloseValue.quantize(Decimal('0.00000000'))), str(currentTickerPctChange.quantize(Decimal('0.000000'))), json.dumps(memo)))
                else:
                    portfolioConstValueRecords.append((portfolioCode, ticker, currentDate, str(currentCloseValue.quantize(Decimal('0.00000000'))), str(currentTickerPctChange.quantize(Decimal('0.000000'))), ''))

            self.savePortfolioConstValues(portfolioConstValueRecords)
            columns = ['Team', 'BbgTicker', 'TradeDate', 'CloseValue', 'PctChange', 'Memo']
            summaryDataFrame = pd.DataFrame(portfolioConstValueRecords, columns=columns)
            summaryDataFrame = pd.merge(indTeamData, summaryDataFrame, how='left', on=['BbgTicker'])

            portfolioCloseValue = summaryDataFrame['CloseValue'].astype(float).sum()
            portfolioValueRecords.append((portfolioCode, currentDate, portfolioCloseValue))
            portfolioCodeList.append(portfolioCode)

        self.savePortfolioValues(portfolioValueRecords)
        '''
            saveToBenchmarkIndexEodPrice: only because reporting.pinpointfund.com is rely on here
        '''
        self.saveToBenchmarkIndexEodPrice(portfolioValueRecords)
        if updatePortfolioWeightInfoRecords:
            self.updateBenchmarkPortfolioWeightInfoData(updatePortfolioWeightInfoRecords)

    def adjustPriceForSplit(self, adjFactor, dateStr, operatorType, tickerData):
        if operatorType == 1:
            tickerData['AdjustedPreClosePrice'] = np.where(tickerData['TradeDateStr'] == dateStr,
                                                           tickerData['PreClosePrice'] / adjFactor,
                                                           tickerData['PreClosePrice'])
        elif operatorType == 2:
            tickerData['AdjustedPreClosePrice'] = np.where(tickerData['TradeDateStr'] == dateStr,
                                                           tickerData['PreClosePrice'] * adjFactor,
                                                           tickerData['PreClosePrice'])
        elif operatorType == 3:
            tickerData['AdjustedPreClosePrice'] = np.where(tickerData['TradeDateStr'] == dateStr,
                                                           tickerData['PreClosePrice'] + adjFactor,
                                                           tickerData['PreClosePrice'])
        elif operatorType == 4:
            tickerData['AdjustedPreClosePrice'] = np.where(tickerData['TradeDateStr'] == dateStr,
                                                           tickerData['PreClosePrice'] - adjFactor,
                                                           tickerData['PreClosePrice'])
        return tickerData['AdjustedPreClosePrice'].astype(Decimal).iloc[-1]

    def calcAlphaBetaIndexTypeTeamLevel(self, dateStr, teamList ,reRun=False):
        allFundWeightData = self.getTeamBenchmarkTypeIndexWegithInfoData(teamList)
        if allFundWeightData.empty:
            logging.warn('calcAlphaBetaIndexTypeTeamLevel got empty data, will return')
            return
        indexList = list(allFundWeightData['ConstituentTicker'].unique())
        indexData = self.getIndexEODPriceData(indexList,dateStr)
        teamReturnData = self.getTeamReturn(self.alphaBeginDate, dateStr)
        teamReturnData['FundCode'] = teamReturnData['FundBookCode'].str.split(pat='-').str[0]
        teamReturnData['BookCode'] = teamReturnData['FundBookCode'].str.split(pat='-').str[1]

        teamCodeList = list(allFundWeightData['BookCode'].unique())

        alphaBetaRecords = []
        indexPctRecordsDict = dict()
        indexPctRecords =[]
        fundIdList = []
        bookIdList = []
        for teamCode in teamCodeList:
            teamAllData = teamReturnData[teamReturnData['BookCode'] == teamCode].copy()
            teamDataFundCodeList = list(teamAllData['FundCode'].unique())
            for teamDataFundCode in teamDataFundCodeList:
                teamData = teamAllData[teamAllData['FundCode'] == teamDataFundCode].copy()
                teamData.drop_duplicates(subset='Date', inplace=True)
                fundId = teamData['FundId'].iloc[-1]
                bookId = teamData['BookId'].iloc[-1]
                fundIdList.append(str(fundId))
                bookIdList.append(str(bookId))
                teamData.set_index('Date', inplace=True)
                teamData.index = pd.to_datetime(teamData.index)
                teamData['TradeDate'] = teamData.index
                teamData.sort_index(ascending=True, inplace=True)
                teamData['YtdGrossReturn'] = teamData['YtdGrossReturn'].fillna(0)
                teamData['TeamPctChange'] = (teamData['YtdGrossReturn'] - teamData['YtdGrossReturn'].shift(1)) / (1 + teamData['YtdGrossReturn'].shift(1))
                actualReturnDays = teamData.shape[0]
                actualPortfolioYTDYield = 0.06 / 365 * actualReturnDays

                fundWeightData = allFundWeightData[allFundWeightData['BookCode'] == teamCode]
                if fundWeightData.empty:
                    continue

                indexList = list(fundWeightData['IndexCode'].unique())
                joinedData = pd.merge(indexData, fundWeightData, how='left', on=['IndexCode'])

                columns = ['BookCode', 'IndexCode', 'TradeDate', 'Weight', 'WeightedPctChange']
                fundIndexsummaryData = pd.DataFrame(columns=columns)
                for indexCode in indexList:
                    indexJoinedData = joinedData[joinedData['IndexCode'] == indexCode]
                    indexJoinedData['TradeDate'] = pd.to_datetime(indexJoinedData['TradeDate'])
                    indexJoinedData.sort_values('TradeDate', ascending=True, inplace=True)
                    indexJoinedData['PctChange'] = ((indexJoinedData['ClosePrice'] / indexJoinedData['ClosePrice'].shift(1)) - 1)*100
                    ##PctChange 带符号 % ，需除100
                    indexJoinedData['IndexWeightedPctChange'] = indexJoinedData['PctChange'] / 100 * indexJoinedData['Weight']
                    indexJoinedData['IndexWeightedClose'] = indexJoinedData['ClosePrice'] * indexJoinedData['Weight']
                    fundIndexsummaryData = pd.concat([fundIndexsummaryData, indexJoinedData[['BookCode','IndexCode', 'TradeDate', 'Weight', 'IndexWeightedPctChange','IndexWeightedClose']]], axis=0, sort=True)

                #data.groupby(['FundCode', 'BookCode', 'Country']).agg({'NetNav': 'sum', 'GrossNav': 'sum'})
                fundBenchmarkSummaryData = fundIndexsummaryData.groupby(['BookCode', 'TradeDate']).agg({'IndexWeightedPctChange': 'sum', 'IndexWeightedClose': 'sum'})
                fundBenchmarkSummaryData = fundBenchmarkSummaryData.reset_index()
                fundBenchmarkSummaryData.index = pd.to_datetime(fundBenchmarkSummaryData['TradeDate'])
                fundBenchmarkSummaryData.sort_index(ascending=True, inplace=True)
                #fundBenchmarkSummaryData['IndexYTDReturn'] = (1 + fundBenchmarkSummaryData['IndexWeightedPctChange']).astype(float).cumprod() - 1

                joinedData = pd.merge(teamData, fundBenchmarkSummaryData, how='left', on=['TradeDate'])
                joinedData.dropna(subset=['TeamPctChange'], how='all', inplace=True)
                joinedData.sort_values('TradeDate', ascending=True, inplace=True)
                joinedData['IndexYTDReturn'] = (1 + joinedData['IndexWeightedPctChange']).astype(float).cumprod() - 1
                checkNA = joinedData[np.isnan(joinedData['IndexWeightedPctChange'].astype('float'))]
                if not checkNA.empty:
                    logging.error(
                        'CRITICAL: ' + teamCode + ' - ' + dateStr + ' Index pct change has NA value, pls check data!')
                    continue
                if not joinedData.empty:
                    actualPortfolioYTDYield = joinedData['IndexYTDReturn'].astype('float').iloc[-1]
                    relReturn = teamData['YtdGrossReturn'].astype('float').iloc[-1] - actualPortfolioYTDYield
                    (relReturnCurDD, relReturnMaxDD, maxDDStartDateStr, maxDDDateStr, recovered, relReturnHistHigh) = self.calcRelReturnMaxDD(dateStr,fundId,int(bookId), relReturn)
                    key= self.calcYear+'-'+teamCode+'-'+dateStr
                    if indexPctRecordsDict.has_key(key) == False:
                        indexPctRecordsDict[key] = 'saved'
                        indexPctRecords.append((self.calcYear+teamCode, dateStr, joinedData['IndexWeightedPctChange'].astype('float').iloc[-1], joinedData['IndexWeightedClose'].astype('float').iloc[-1]))

                    (alpha, beta, r_squared) = self.regressionCalc(
                        np.asarray(joinedData['IndexWeightedPctChange'].astype('float')),
                        np.asarray(joinedData['TeamPctChange'].astype('float')),
                        teamData['YtdGrossReturn'].astype('float').iloc[-1], actualPortfolioYTDYield)
                    if alpha == 0 and beta == 0 and r_squared == 0:
                        logging.warning(teamCode + ' ref data not enough to calculate Alpha/Beta')
                    else:
                        logging.info(
                            teamCode + ' alpha: ' + str(alpha) + ', beta: ' + str(beta) + ', date:' + dateStr)
                        alphaBetaRecords.append((self.alphaBeginDate, dateStr, int(fundId),int(bookId),
                                                 self.calcYear + teamCode, alpha, beta, r_squared, actualPortfolioYTDYield,
                                                 AlphaBetaType.TEAM_WITH_INDEX_BENCHMARK.value, relReturn,
                                                 relReturnMaxDD, recovered, maxDDStartDateStr, maxDDDateStr,relReturnCurDD,relReturnHistHigh))
                else:
                    logging.error(
                        'CRITICAL: Alpha/Beta calculation aborted for Fund:' + teamCode + ' - ' + dateStr + ', due to miss market data for Portfolio')

        '''
            saveIndexPctChange: only because reporting.pinpointfund.com is rely on here
        '''
        if not reRun:
            self.saveIndexPctChange(indexPctRecords)
        self.removeAlphaBeta(dateStr, bookIdList, fundIdList, AlphaBetaType.TEAM_WITH_INDEX_BENCHMARK.value, reRun)
        self.saveAlphaBeta(alphaBetaRecords)

    def calcAlphaBetaFundLevel(self, dateStr, fundList):
        allFundWeightData = self.getFundBenchmarkPortforlioWegithInfoData(fundList)
        if allFundWeightData.empty:
            logging.warn('calcAlphaBetaFundLevel got empty data, will return')
            return
        indexList = list(allFundWeightData['ConstituentTicker'].unique())
        indexData = self.getIndexEODPriceData(indexList,dateStr)
        fundReturnData = self.getFundReturn(self.alphaBeginDate, dateStr)
        fundCodeList = fundList
        if allFundWeightData.empty:
            fundCodeList = fundList

        alphaBetaRecords = []
        fundIdList = []
        for fundCode in fundCodeList:
            teamCode = fundCode
            teamData = fundReturnData[fundReturnData['FundCode'] == fundCode].copy()
            teamData.drop_duplicates(subset='Date', inplace=True)
            if teamData.empty:
                logging.warning('fund return is empty,'+fundCode)
                continue
            fundId = teamData['FundId'].iloc[-1]
            bookId = 0
            fundIdList.append(str(fundId))
            teamData.set_index('Date', inplace=True)
            teamData.index = pd.to_datetime(teamData.index)
            teamData['TradeDate'] = teamData.index
            teamData.sort_index(ascending=True, inplace=True)
            teamData['YtdGrossReturn'] = teamData['YtdGrossReturn'].fillna(0)
            teamData['TeamPctChange'] = (teamData['YtdGrossReturn'] - teamData['YtdGrossReturn'].shift(1)) / (1 + teamData['YtdGrossReturn'].shift(1))
            actualReturnDays = teamData.shape[0]
            actualPortfolioYTDYield = 0.06 / 365 * actualReturnDays

            fundWeightData = allFundWeightData[allFundWeightData['FundCode'] == fundCode]
            if fundWeightData.empty and fundCode=='SLHL':
                relReturn = teamData['YtdGrossReturn'].astype('float').iloc[-1] - actualPortfolioYTDYield
                (relReturnCurDD, relReturnMaxDD, maxDDStartDateStr, maxDDDateStr, recovered, relReturnHistHigh) = self.calcRelReturnMaxDD(dateStr,fundId,int(bookId),relReturn)
                alpha = teamData['YtdGrossReturn'].astype('float').iloc[-1]
                logging.info(fundCode + ' alpha: ' + str(alpha) + ', beta: 0' + ',date:' + dateStr)
                alphaBetaRecords.append((self.alphaBeginDate, dateStr, int(fundId), 'NO_BENCHMARK', alpha,
                                         0, 0, actualPortfolioYTDYield, AlphaBetaType.FUND_WITH_INDEX_BENCHMARK.value, relReturn,
                                             relReturnMaxDD, recovered, maxDDStartDateStr, maxDDDateStr,relReturnCurDD,relReturnHistHigh))
                continue

            indexList = list(fundWeightData['IndexCode'].unique())
            joinedData = pd.merge(indexData, fundWeightData, how='left', on=['IndexCode'])

            columns = ['FundCode', 'IndexCode', 'TradeDate', 'Weight', 'WeightedPctChange']
            fundIndexsummaryData = pd.DataFrame(columns=columns)
            for indexCode in indexList:
                indexJoinedData = joinedData[joinedData['IndexCode'] == indexCode]
                indexJoinedData['TradeDate'] = pd.to_datetime(indexJoinedData['TradeDate'])
                indexJoinedData.sort_values('TradeDate', ascending=True, inplace=True)
                indexJoinedData['PctChange'] = ((indexJoinedData['ClosePrice'] / indexJoinedData['ClosePrice'].shift(1)) - 1)*100
                ##PctChange 带符号 % ，需除100
                indexJoinedData['IndexWeightedPctChange'] = indexJoinedData['PctChange'] / 100 * indexJoinedData['Weight']
                fundIndexsummaryData = pd.concat([fundIndexsummaryData, indexJoinedData[['FundCode','IndexCode', 'TradeDate', 'Weight', 'IndexWeightedPctChange']]], axis=0, sort=True)

            fundBenchmarkSummaryData = fundIndexsummaryData.groupby(['FundCode', 'TradeDate'])['IndexWeightedPctChange'].sum()
            fundBenchmarkSummaryData = fundBenchmarkSummaryData.reset_index()
            fundBenchmarkSummaryData.index = pd.to_datetime(fundBenchmarkSummaryData['TradeDate'])
            fundBenchmarkSummaryData.sort_index(ascending=True, inplace=True)
            #fundBenchmarkSummaryData['IndexYTDReturn'] = (1 + fundBenchmarkSummaryData['IndexWeightedPctChange']).astype(float).cumprod() - 1

            joinedData = pd.merge(teamData, fundBenchmarkSummaryData, how='left', on=['TradeDate'])
            joinedData.dropna(subset=['TeamPctChange'], how='all', inplace=True)
            joinedData.sort_values('TradeDate', ascending=True, inplace=True)
            joinedData['IndexYTDReturn'] = (1 + joinedData['IndexWeightedPctChange']).astype(float).cumprod() - 1
            joinedData['WeekDay'] = joinedData['TradeDate'].dt.dayofweek
            joinedData = joinedData[joinedData['WeekDay'] != 6]
            joinedData = joinedData[joinedData['WeekDay'] != 5]
            checkNA = joinedData[np.isnan(joinedData['IndexWeightedPctChange'].astype('float'))]
            if not checkNA.empty:
                logging.error(
                    'CRITICAL: ' + fundCode + ' - ' + dateStr + ' Index pct change has NA value, pls check data!')
                continue
            if not joinedData.empty:
                actualPortfolioYTDYield = joinedData['IndexYTDReturn'].astype('float').iloc[-1]
                relReturn = teamData['YtdGrossReturn'].astype('float').iloc[-1] - actualPortfolioYTDYield
                (relReturnCurDD, relReturnMaxDD, maxDDStartDateStr, maxDDDateStr, recovered, relReturnHistHigh) = self.calcRelReturnMaxDD(dateStr,fundId,int(bookId),relReturn)
                (alpha, beta, r_squared) = self.regressionCalc(
                    np.asarray(joinedData['IndexWeightedPctChange'].astype('float')),
                    np.asarray(joinedData['TeamPctChange'].astype('float')),
                    teamData['YtdGrossReturn'].astype('float').iloc[-1], actualPortfolioYTDYield)
                if alpha == 0 and beta == 0 and r_squared == 0:
                    logging.warning(fundCode + ' ref data not enough to calculate Alpha/Beta')
                else:
                    logging.info(
                        fundCode + ' alpha: ' + str(alpha) + ', beta: ' + str(beta) + ', date:' + dateStr)
                    alphaBetaRecords.append((self.alphaBeginDate, dateStr, int(fundId),
                                             self.calcYear + teamCode, alpha, beta, r_squared, actualPortfolioYTDYield,
                                             AlphaBetaType.FUND_WITH_INDEX_BENCHMARK.value, relReturn,
                                             relReturnMaxDD, recovered, maxDDStartDateStr, maxDDDateStr,relReturnCurDD,relReturnHistHigh))
            else:
                logging.error(
                    'CRITICAL: Alpha/Beta calculation aborted for Fund:' + fundCode + ' - ' + dateStr + ', due to miss market data for Portfolio')


        self.removeFundAlphaBeta(dateStr, fundIdList, AlphaBetaType.FUND_WITH_INDEX_BENCHMARK.value)
        self.saveFundAlphaBeta(alphaBetaRecords)

    def calcAlphaBeta(self, dateStr, teamList, indexAsBenchTeamList, calcNoBenchmarkTeams=False, reRun=False, reRunFundList=[]):
        indexAsBenchTeamLWeightData = self.getTeamBenchmarkTypeIndexWegithInfoData(indexAsBenchTeamList)
        indexAsBenchTeamList = list(indexAsBenchTeamLWeightData['BookCode'].unique())
        benchmarkPortfolioEODPriceData = self.getBenchmarkPortfolioEODPriceData(dateStr)
        teamReturnData = self.getTeamReturn(self.alphaBeginDate, dateStr)
        fundBookCodeList = list(teamReturnData['FundBookCode'].unique())
        fundAndFundBookCodeDict = dict((k,'FundBook') for k in fundBookCodeList)
        abnormal_inception_date_team_dict = self.getAbnormalInceptionDateTeamInfo()
        noBenchmarkFundOrTeamList = []
        alphaBetaRecords = []
        teamIdList = []
        fundIdList = []
        for fundOrFundBookCode, type in fundAndFundBookCodeDict.items():
            if type == 'FundBook':
                fundCode = fundOrFundBookCode.split('-')[0]
                teamCode = fundOrFundBookCode.split('-')[1]
                teamData = teamReturnData[teamReturnData['FundBookCode'] == fundOrFundBookCode].copy()
                if abnormal_inception_date_team_dict.has_key(teamCode):
                    inceptionDateStr = abnormal_inception_date_team_dict[teamCode][0]
                    inceptionDate = datetime.datetime.strptime(inceptionDateStr, '%Y-%m-%d')
                    teamData['Date'] = pd.to_datetime(teamData['Date'])
                    teamData = teamData[teamData['Date'] >= inceptionDate]
                    if teamData.empty:
                        logging.warn(dateStr+' '+fundOrFundBookCode+', return data empty, skipping')
                        continue

                fundId = teamData['FundId'].iloc[-1]
                bookId = teamData['BookId'].iloc[-1]

            teamData.drop_duplicates(subset='Date', inplace=True)
            teamData.set_index('Date', inplace=True)
            teamData.index = pd.to_datetime(teamData.index)
            teamData['TradeDate'] = teamData.index
            teamData.sort_index(ascending=True, inplace=True)
            teamData['YtdGrossReturn'] = teamData['YtdGrossReturn'].fillna(0)
            teamData['TeamPctChange'] = (teamData['YtdGrossReturn'] - teamData['YtdGrossReturn'].shift(1)) / (1 + teamData['YtdGrossReturn'].shift(1))
            actualReturnDays = teamData.shape[0]
            ytd_no_benchmark = 0.06
            if teamCode in ['T45','T34','T05','T22','T16','T12','P190401']:
                ytd_no_benchmark = 0.08

            actualPortfolioYTDYield = ytd_no_benchmark / 365 * actualReturnDays

            portfolioData = benchmarkPortfolioEODPriceData[benchmarkPortfolioEODPriceData['PortfolioCode'] == self.calcYear+teamCode].copy()
            portfolioData.set_index('TradeDate', inplace=True)
            portfolioData.index = pd.to_datetime(portfolioData.index)
            portfolioData['TradeDate'] = portfolioData.index
            portfolioData.sort_index(ascending=True, inplace=True)
            #portfolioData['PortfolioPctChange'] = (portfolioData['CloseValue']/portfolioData['CloseValue'].shift(1))-1
            relReturn = teamData['YtdGrossReturn'].astype('float').iloc[-1] - actualPortfolioYTDYield

            if portfolioData.empty and calcNoBenchmarkTeams and teamCode not in indexAsBenchTeamList:
                if (reRun and (teamCode in teamList) and (fundCode in reRunFundList)) or reRun==False:
                   teamIdList.append(bookId)
                   logging.warn('no portfolio data for '+fundOrFundBookCode+' and date='+dateStr)
                   #没有benchmark的team,alpha是YTDReturn, relative return = return  - 6%(默认benchmark return)
                   #alpha = teamData['YtdGrossReturn'].astype('float').iloc[-1] - actualPortfolioYTDYield
                   (relReturnCurDD, relReturnMaxDD, maxDDStartDateStr, maxDDDateStr, recovered, relReturnHistHigh) = self.calcRelReturnMaxDD(dateStr,fundId,int(bookId),relReturn)
                   alpha = teamData['YtdGrossReturn'].astype('float').iloc[-1]
                   logging.info(fundOrFundBookCode + ' alpha: ' + str(alpha) + ', beta: 0'+',date:'+dateStr)
                   alphaBetaRecords.append((self.alphaBeginDate, dateStr, int(fundId), int(bookId), 'NO_BENCHMARK', alpha, 0, 0, actualPortfolioYTDYield, AlphaBetaType.TEAM_WITHOUT_BENCHMARK.value,relReturn,
                                             relReturnMaxDD, recovered, maxDDStartDateStr, maxDDDateStr, relReturnCurDD, relReturnHistHigh))
                   continue


            if teamCode not in teamList:
                continue

            if reRun and fundCode not in reRunFundList:
                continue

            teamIdList.append(str(bookId))
            fundIdList.append(str(fundId))
            logging.warn('fundbook::::'+fundCode+'-'+teamCode)
            portfolioData['PortfolioPctChange'] = (portfolioData['CloseValue'] / portfolioData['CloseValue'].shift(1)) - 1
            portfolioData['PortfolioYTDReturn'] = (1 + portfolioData['PortfolioPctChange']).astype(float).cumprod() - 1
            joinedData = pd.merge(teamData, portfolioData, how='left',on=['TradeDate'])
            joinedData.sort_index(ascending=True, inplace=True)
            #joinedData['PortfolioPctChange'] = (joinedData['CloseValue'] / joinedData['CloseValue'].shift(1)) - 1
            #joinedData['PortfolioYTDReturn'] = (1+joinedData['PortfolioPctChange'])
            #joinedData['PortfolioYTDReturn'] = (1 + joinedData['PortfolioPctChange']).astype(float).cumprod() - 1
            joinedData.dropna(subset=['TeamPctChange'], how='all', inplace=True)
            checkNA = joinedData[np.isnan(joinedData['PortfolioPctChange'].astype('float'))]
            if not checkNA.empty:
                logging.error('CRITICAL: '+fundOrFundBookCode+' - '+dateStr+' Portfolio pct change has NA value, pls check data!')
                continue
            if not joinedData.empty:
                actualPortfolioYTDYield = joinedData['PortfolioYTDReturn'].astype('float').iloc[-1]
                relReturn = teamData['YtdGrossReturn'].astype('float').iloc[-1] - actualPortfolioYTDYield
                (relReturnCurDD, relReturnMaxDD, maxDDStartDateStr, maxDDDateStr, recovered,relReturnHistHigh) = self.calcRelReturnMaxDD(dateStr,fundId,int(bookId),relReturn)
                (alpha, beta, r_squared) = self.regressionCalc(np.asarray(joinedData['PortfolioPctChange'].astype('float')),
                                                           np.asarray(joinedData['TeamPctChange'].astype('float')),
                                                           teamData['YtdGrossReturn'].astype('float').iloc[-1],
                                                           actualPortfolioYTDYield)
                if alpha == 0 and beta == 0 and r_squared == 0:
                    logging.warning(fundOrFundBookCode + ' ref data not enough to calculate Alpha/Beta')
                else:
                    logging.info(fundOrFundBookCode + ' alpha: ' + str(alpha) + ', beta: ' + str(beta)+', date:'+dateStr)
                    alphaBetaRecords.append((self.alphaBeginDate, dateStr, int(fundId), int(bookId),  self.calcYear+teamCode, alpha, beta, r_squared, actualPortfolioYTDYield,
                                             AlphaBetaType.TEAM_WITH_STOCK_BENCHMARK.value,relReturn,
                                             relReturnMaxDD,recovered, maxDDStartDateStr, maxDDDateStr,relReturnCurDD,relReturnHistHigh))
            else:
                logging.error('CRITICAL: Alpha/Beta calculation aborted for ' + fundOrFundBookCode +' - '+ dateStr + ', due to miss market data for Portfolio')

        if calcNoBenchmarkTeams:
            self.removeAlphaBeta(dateStr, [], [], AlphaBetaType.TEAM_WITHOUT_BENCHMARK.value, reRun)
        self.removeAlphaBeta(dateStr, teamIdList, fundIdList, AlphaBetaType.TEAM_WITH_STOCK_BENCHMARK.value, reRun)
        self.saveAlphaBeta(alphaBetaRecords)

    def calcRelReturnMaxDD(self, dateStr, fundId, bookId, relReturn):
        rel_return_data = self.getAlphaBeta(dateStr, fundId,bookId)
        rel_return_data = rel_return_data[['EndDate','FundId','BookId','RelReturn']]
        rel_return_data['BookId'] = bookId
        date = datetime.datetime.strptime(dateStr, '%Y-%m-%d')
        current_data = pd.DataFrame(columns=['EndDate', 'BookId', 'FundId','RelReturn'])
        current_data = current_data.append({'EndDate': date, 'BookId': bookId, 'FundId': fundId,'RelReturn':relReturn},ignore_index=True)
        rel_return_data = pd.concat([rel_return_data, current_data], axis=0)
        rel_return_data['EndDate'] = pd.to_datetime(rel_return_data['EndDate'])
        rel_return_data.sort_values('EndDate', ascending=True, inplace=True)
        rel_return_data.drop_duplicates(subset='EndDate', inplace=True, keep='first')
        rel_return_data['RelReturn'] = rel_return_data['RelReturn'].astype(float)
        rel_return_data['PCT_CHG'] = (rel_return_data['RelReturn'] - rel_return_data['RelReturn'].shift(1))/(1+rel_return_data['RelReturn'].shift(1))
        rel_return_data['Date'] = rel_return_data['EndDate']
        relReturnHistHigh = rel_return_data['RelReturn'].max()
        (another_maxDD, maxDDStartDateStr, maxDDDateStr, recovered, annualRtn, currentDD, annualVol, annualSharpe) = IndicatorCalculation.calculateRecoveryWithPct(rel_return_data,fundId,bookId)
        return (currentDD, another_maxDD, maxDDStartDateStr, maxDDDateStr, recovered,relReturnHistHigh)

    def get_alphabeta_view(self, date_str, funds):
        sql= 'select BeginDate,EndDate, FundId, FundCode, BookId, BookCode, RelReturn, RelReturnCurDD, RelReturnMaxDD, RelReturnHistHigh, RelReturnRecovery, RelReturnMaxDDFrom, RelReturnMaxDDTo  ' \
             'FROM [RiskDb].[risk].[AlphaBetaView] where FundCode in (\'' + ('\',\'').join(funds) + '\') and EndDate=\''+date_str+'\''
        return self.selectFromDataBase(sql)

    def alphacapture_calc(self, date_str):
        ''' abs return between Max DD period '''
        maxdd_data = self.get_alphabeta_view(date_str,['ZJNF','CACF'])
        maxdd_data['RelReturnMaxDDFrom'] = pd.to_datetime(maxdd_data['RelReturnMaxDDFrom'])
        maxdd_data['RelReturnMaxDDTo'] = pd.to_datetime(maxdd_data['RelReturnMaxDDTo'])
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        startDate = str(date.year)+'-01-01'
        team_data = self.getTeamReturn(startDate, date_str)
        team_data['Date'] = pd.to_datetime(team_data['Date'])
        team_data['RelReturnMaxDDFrom'] = team_data['Date']
        team_data['RelReturnMaxDDTo'] = team_data['Date']

        maxdd_data = pd.merge(maxdd_data, team_data[['FundId','BookId','RelReturnMaxDDFrom', 'YtdGrossReturn']], how='left', on=['FundId','BookId','RelReturnMaxDDFrom'])
        maxdd_data['YtdMaxDDFrom'] = maxdd_data['YtdGrossReturn']
        del maxdd_data['YtdGrossReturn']
        maxdd_data = pd.merge(maxdd_data, team_data[['FundId','BookId','RelReturnMaxDDTo', 'YtdGrossReturn']], how='left', on=['FundId','BookId','RelReturnMaxDDTo'])
        maxdd_data['YtdMaxDDTo'] = maxdd_data['YtdGrossReturn']
        del maxdd_data['YtdGrossReturn']
        maxdd_data['ReturnDuringMaxDD'] = (maxdd_data['YtdMaxDDTo']+1)/(maxdd_data['YtdMaxDDFrom']+1) - 1

        if not maxdd_data.empty:
            maxdd_data.dropna(subset=['BookId'], how='all', inplace=True)
            maxdd_data['ReturnDuringMaxDD'] = maxdd_data['ReturnDuringMaxDD'].astype(float).round(6)
            sql = 'update [RiskDb].[bench].[AlphaBeta] set RetAtMaxDDPeriod=? where EndDate=? and FundId=? and BookId=?'
            updateRecords = pdUtil.dataFrameToSavableRecords(maxdd_data, ['ReturnDuringMaxDD','EndDate','FundId','BookId'])
            self.updateToDataBase(sql, updateRecords)

    def regressionCalc(self, portfolioPctChg, pmPctChg, teamYTDYield, portfolioYTDYield):
        if len(portfolioPctChg) <= 1 or len(pmPctChg) <= 1:
            return 0, 0, 0
        benchmarkPctChg = sm.add_constant(portfolioPctChg)
        model = sm.OLS(pmPctChg, benchmarkPctChg).fit()
        if len(model.params)>1:
            beta = model.params[1]
            #portfolioActualYTDYield = self.calcPortfolioYTDYield(portfolioPctChg)
            alpha = teamYTDYield - beta * portfolioYTDYield
            r_squared = model.rsquared
            return alpha, beta, r_squared
        else:
            logging.warning('Beta is none value, pls check pct chg')
            return 0, 0, 0

    # def calcPortfolioYTDYield(self, portfolioPctChg):
    #     portfolioYtdYield = 1.0
    #     for dtdPctChg in portfolioPctChg:
    #         portfolioYtdYield *= (1 + dtdPctChg)
    #     portfolioYtdYield -= 1
    #     return portfolioYtdYield

    def refreshAndDownloadMarketData(self,refreshMarketData,startDateStr, endDateStr,tickers,teamList,indexTickers):
        if refreshMarketData:
            mdService = MarketDataDownloader(self.env)
            mdService.initSqlServer(self.env)
            mdService.cleanupMarketData(startDateStr, endDateStr, tickers, teamList, self.calcYear,indexTickers)
            mdService.getEquityDvdSplitInfoFromBbg(tickers)
            mdService.getStockCashDividentEventsFromBbg(tickers)
            ##mdService.addStockDividentDayClosePrice(startDateStr, endDateStr)
            mdService.getIndexMdFromBbg(startDateStr,endDateStr,indexTickers)
            mdService.getCurrencEodPrice(startDateStr, endDateStr,
                                         ['HKDUSD Curncy', 'CNYUSD Curncy', 'IDRUSD Curncy', 'PHPUSD Curncy',
                                          'SGDUSD Curncy', 'JPYUSD Curncy', 'VNDUSD Curncy', 'KRWUSD Curncy',
                                          'THBUSD Curncy', 'MYRUSD Curncy', 'EURUSD Curncy', 'INRUSD Curncy',
                                          'TWDUSD Curncy', 'AUDUSD Curncy', 'CHFUSD Curncy'])
            mdService.getBbgTickerEodMd(startDateStr, endDateStr, tickers)
            mdService.closeSqlServerConnection()
        else:
            logging.info('market data download- skipped')

    def checkTickerDvdCcyInfo(self, tickers):
        sql = 'SELECT distinct ConstituentTicker FROM RiskDb.bench.BenchmarkPortfolioWeight where EffectiveYear=\''+self.calcYear+'\' and  PortfolioType=0 and ConstituentTicker not in( SELECT  BbgTicker FROM RiskDb.bench.BbgTickerCurrencyInfo)'
        if tickers:
            sql += ' and ConstituentTicker in (\'' + ('\',\'').join(tickers)+'\')'
        data = self.selectFromDataBase(sql)
        if not data.empty:
            tickers = list(data['ConstituentTicker'].unique())
            mdService = MarketDataDownloader(self.env)
            mdService.initSqlServer(self.env)
            mdService.getCurrencyInfo(tickers)
            mdService.closeSqlServerConnection()


    def runWithDateRange(self, startDateStr, stopDateStr, teamList, fundList, indexAsBenchTeamList, refreshMarketData = False, calcNoBenchmarkTeams = False, calcAlphaBetaOnly=False):
        self.initSqlServer(self.env)
        tickers = self.getTickers(teamList)
        indexTickers = self.getIndexTickers(fundList)
        indexTickersFromBook = self.getIndexTickersFromTeam()
        extraIndexTickers = ['KOSPI Index', 'MXAU Index', 'MXJP0IT index', 'MXKR0IT index', 'NKY Index', 'STI Index', 'SXXP Index','SHCOMP INDEX']
        indexTickers = list(set(indexTickers + indexTickersFromBook + extraIndexTickers))
        startDate = datetime.datetime.strptime(startDateStr, '%Y-%m-%d')
        stopDate = datetime.datetime.strptime(stopDateStr, '%Y-%m-%d')
        self.checkTickerDvdCcyInfo(tickers)
        while (startDate <= stopDate):
            if (startDate.weekday() >= 0 and startDate.weekday() <= 4):
                dateStr = startDate.strftime('%Y-%m-%d')
                logging.info('run date='+dateStr)
                self.refreshAndDownloadMarketData(refreshMarketData, startDateStr, stopDateStr, tickers, teamList, indexTickers)
                if not calcAlphaBetaOnly:
                    self.calcPortfolioValueAndPctChange(teamList, tickers, dateStr)
                self.calcAlphaBeta(dateStr, teamList, indexAsBenchTeamList, calcNoBenchmarkTeams = calcNoBenchmarkTeams)
                self.calcAlphaBetaFundLevel(dateStr, fundList)
                self.calcAlphaBetaIndexTypeTeamLevel(dateStr, indexAsBenchTeamList)
                self.alphacapture_calc(dateStr)
            startDate = startDate + datetime.timedelta(days=1)
        self.closeSqlServerConnection()


    def getTeamBenchmarkInfoData(self, teamList):
        sql2 = 'select * from RiskDb.bench.BenchmarkPortfolioWeight where  EffectiveYear=\''+self.calcYear+'\' and TeamCode in (\'' + ('\',\'').join(teamList) + '\') and (PortfolioType='+str(PortfolioType.BOOK.value)+' or PortfolioType='+str(PortfolioType.BOOK_BENCHMAR_WITH_INDEX.value)
        teamsData = self.selectFromDataBase(sql2)
        teamsData['BbgTicker'] = teamsData['ConstituentTicker']
        teamsData['HoldingShares'] = teamsData['HoldingShares'].astype(Decimal)
        teamsData['InceptionDatePrice'] = teamsData['InceptionDatePrice'].astype(Decimal)
        teamsData['Weight'] = teamsData['Weight'].astype(Decimal)
        return teamsData


    def reRunWithDateRange(self, teamList, startDateStr, stopDateStr, fundList, calcNoBenchmarkTeams = False):
        self.initSqlServer(self.env)
        startDate = datetime.datetime.strptime(startDateStr, '%Y-%m-%d')
        stopDate = datetime.datetime.strptime(stopDateStr, '%Y-%m-%d')
        reRunFlag = True
        while (startDate <= stopDate):
            if (startDate.weekday() >= 0 and startDate.weekday() <= 4):
                dateStr = startDate.strftime('%Y-%m-%d')
                logging.info('re-run date='+dateStr)
                if teamList:
                    self.calcAlphaBeta(dateStr, teamList, [], calcNoBenchmarkTeams, reRun = reRunFlag, reRunFundList=fundList)
                else:
                    self.calcAlphaBetaFundLevel(dateStr, fundList)
            startDate = startDate + datetime.timedelta(days=1)
        self.closeSqlServerConnection()

    def copyPreDateData(self, preDateStr, endDateStr):
        self.initSqlServer(self.env)
        sql = 'SELECT BeginDate,EndDate,FundId,BookId,BechmarkCode,Alpha,Beta,RSquared,BenchmarkYTDAdjusted,ValueType,RelReturn,Memo FROM RiskDb.bench.AlphaBeta where EndDate=\'' + preDateStr+'\' and BookId is not null'
        data = self.selectFromDataBase(sql)
        data['EndDate'] = endDateStr
        data['Memo'] = 'initForDay'
        data[['Alpha','Beta','RSquared','BenchmarkYTDAdjusted','RelReturn']] = data[['Alpha','Beta','RSquared','BenchmarkYTDAdjusted','RelReturn']].astype(float).round(8)
        records = pdUtil.dataFrameToSavableRecords(data, ['BeginDate', 'EndDate', 'FundId','BookId','BechmarkCode','Alpha','Beta','RSquared','BenchmarkYTDAdjusted','ValueType','RelReturn','Memo'])
        if records:
            logging.info('copying alpha beta data')
            sql = 'insert into RiskDb.bench.AlphaBeta (BeginDate, EndDate, FundId, BookId, BechmarkCode, Alpha, Beta, RSquared, BenchmarkYTDAdjusted, ValueType, RelReturn, Memo) values (?,?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, records)
        else:
            logging.warn('copyAlphaBeta: empty record')
        self.closeSqlServerConnection()

    def run(self, refreshMarketData = False, calcNoBenchmarkTeams=False):
        teamList = ['T09','T10','T11','T13','T14','T17','T23','T28','T31','T35','T37','T38','T43','T46','T47','T48','T49','T50','T20','T27','T39','T41','T42','T44','PJH191211','PKT200203','PPDX191230','PTA200122','PYU190916','PRL200401','PWC200407','PJW200716','PSZ200810','PTY200716','EDISON200713','YUSHENG200713','T52']
        fundList = ['CVF', 'PCF', 'PLUS', 'PMSF', 'ZJNF', 'SLHL', 'PTF', 'DCL', 'CPTF']
        indexAsBenchTeamList = []
        currentDate = datetime.datetime.now()
        weekDay = currentDate.date().weekday()
        diff = 1
        if (weekDay == 5 or weekDay == 6):
        #if (weekDay == 6):  ### ad-hoc change , as ren fei needs the data at Sat 03/22
            return
        elif (weekDay == 0):
            diff += 2
        runYesterDay = currentDate - datetime.timedelta(days=diff)
        runYesterDayStr = runYesterDay.strftime('%Y-%m-%d')

        currentHour = currentDate.hour
        currentMin = currentDate.minute
        calcAlphaBetaOnly = False
        logging.info(currentDate)
        if currentHour==7:
            days_diff = 1
            if runYesterDay.date().weekday()==0:
                days_diff = 3
            preDate = runYesterDay - datetime.timedelta(days=days_diff)
            preDateStr = preDate.strftime('%Y-%m-%d')
            self.copyPreDateData(preDateStr, runYesterDayStr)
        elif (currentHour == 12) & (currentMin < 10):
            logging.info('re-run at 12PM')
            refreshMarketData = False
            calcAlphaBetaOnly = True
            self.runWithDateRange(runYesterDayStr, runYesterDayStr, teamList, fundList, indexAsBenchTeamList,
                                  refreshMarketData, calcNoBenchmarkTeams, calcAlphaBetaOnly)
        else:
            self.runWithDateRange(runYesterDayStr, runYesterDayStr, teamList, fundList, indexAsBenchTeamList, refreshMarketData, calcNoBenchmarkTeams, calcAlphaBetaOnly)

    def api(self, dateStr, teamList):
        self.calcAlphaBeta(dateStr, teamList, [], calcNoBenchmarkTeams=False, reRun=False)

if __name__ == '__main__':
    ####勿提交改动，
    env = 'prod'
    alphaBetaCalc = AlphaBetaCalc(env)
    #alphaBetaCalc.run(True,True)
    # alphaBetaCalc.initSqlServer(env)
    # alphaBetaCalc.getTeamReturn('2019-01-01','2019-12-23')
    # alphaBetaCalc.closeSqlServerConnection()
    #alphaBetaCalc.runWithDateRange('2020-09-11', '2020-09-11', [],[],[], True, False, False)
    alphaBetaCalc.initSqlServer(env)
    alphaBetaCalc.alphacapture_calc('2020-09-21')
    alphaBetaCalc.closeSqlServerConnection()
    #alphaBetaCalc.initSqlServer(env)
    #alphaBetaCalc.calcRelReturnMaxDD('2020-01-18',11,0,0.1)
    #alphaBetaCalc.closeSqlServerConnection()
    #alphaBetaCalc.run(refreshMarketData = True,calcNoBenchmarkTeams=True)
    #alphaBetaCalc.initBenchmarkPortfolioWeight('C:\\devel\\2020benchmark\\ok_version\\bbg\\EDISON200713_2020_BBG.xlsx','EDISON200713','2020-07-10')
    #alphaBetaCalc.initBenchmarkPortfolioWeight('C:\\devel\\2019benchmark\\bbg\\PJH191211_2019_bbg.xlsx','PJH191211','2019-12-10')
    #dict = alphaBetaCalc.getAbnormalInceptionDateTeamInfo()
    print 'test'

