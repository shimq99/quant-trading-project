# encoding:UTF-8
from benchmark.base.Base import Base
import datetime
from benchmark.log.LogManager import LogManager
import numpy as np
import pandas as pd
import pyodbc
import logging
import smtplib
import xlwt
import sys
from benchmark.tools import PandasDBUtils as pdUtil
from benchmark.base.CommonEnums import RiskFundStatReportPositionType
from benchmark.base.CommonEnums import RiskFundStatReportType


class FundStatistics(object):

    def __init__(self,aum):
        self.positionList = []
        self.tickerPositionInfo = dict()
        self.pctStyle = xlwt.XFStyle()
        self.AUM_USD=aum
        self.pctStyle.num_format_str = '0.00%'
        self.useNewDb = 1

    def initSqlServer(self):
        server = '192.168.200.9\DevServer'
        database = 'Portfolio'
        username = 'Dev'
        password = 'Dev123'
        self.conn = pyodbc.connect(
            'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        self.cursor = self.conn.cursor()

    def initSqlserver22(self):
        server = '192.168.200.22\prod'
        database = 'RiskDb'
        username = 'Dev'
        password = 'Dev@123'
        self.conn = pyodbc.connect(
            'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password,
            autocommit=True)
        self.cursor = self.conn.cursor()
        self.cursor.fast_executemany = True

    def initLogger(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            filename='log.log',
                            filemode='w')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)-8s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)

    def closeSqlServerConnection(self):
        self.conn.commit()
        self.conn.close()

    def getPositionsFromDb(self, asOfDateStr):
        if (self.useNewDb == 1):
            sql = "RiskDb.risk.usp_GetRiskPosition @Date='%s'" % asOfDateStr
        else:
            sql = u"DI.excel.usp_get_risk_holding @as_of_date='%s'" % asOfDateStr
        self.cursor.execute(sql)
        self.positionList = self.cursor.fetchall()

    def getUsdRmbExchangeRate(self):
        for r in self.positionList:
            if (self.useNewDb != 1):
                lsf = r[4]
                if (lsf == 'Flat In Month'):
                    continue

            fund = r[0]
            if (self.useNewDb == 1):
                ccy = r[10]
            else:
                ccy = r[8]
            if ((fund == 'PMSF' or fund == 'PCF' or fund == 'PLUS') and ccy == 'CNY'):
                if (self.useNewDb == 1):
                    rate = 1 / r[20]
                else:
                    rate = r[13]
                return rate

    def getTickerPositionInfo(self, rate):
        for r in self.positionList:
            if (self.useNewDb != 1):
                lsf = r[4]
                if (lsf == 'Flat In Month'):
                    continue
            instrumentCode = r[2]
            if (
                    instrumentCode == 'REITS' or instrumentCode == 'NON_REITS' or instrumentCode == 'PRFD' or instrumentCode == 'CFD_EQUITY' \
                    or instrumentCode == 'DR'):

                fund = r[0]
                book = r[3]
                if (self.useNewDb == 1):
                    ticker = r[6]
                    mv = r[13]
                    ulyTicker = r[27]
                else:
                    ticker = r[5]
                    mv = r[10]
                    ulyTicker = r[23]

                if (fund == 'CVF' or fund == 'SLHL' or fund == 'ZJNF'):
                    mv /= rate

                if (ulyTicker != None):
                    ulyTicker = ulyTicker.replace('C1 Equity', 'CH Equity')
                    ulyTicker = ulyTicker.replace('C2 Equity', 'CH Equity')
                if (self.tickerPositionInfo.has_key(ticker)):
                    self.tickerPositionInfo[ticker].append((fund, book, mv, ulyTicker))
                else:
                    newTickerFlag = True
                    dueTicker = ''
                    # check underlying ticker
                    for k, v in self.tickerPositionInfo.items():
                        try:
                            for r in v:
                                if (r[3] == ulyTicker):
                                    newTickerFlag = False
                                    self.tickerPositionInfo[k].append((fund, book, mv, ulyTicker))
                                    # dueTicker = k
                                    break

                            if (not newTickerFlag):
                                break

                        except Exception, e:
                            pass

                    if (newTickerFlag):
                        a = []
                        a.append((fund, book, mv, ulyTicker))
                        self.tickerPositionInfo[ticker] = a

    def exportTickerTeamInfoToExcel(self, tickerInfoList, sheet, row, col, sortFlag, pctFlag=False):
        # remove same team
        tickerTeamDict = dict()
        for t in tickerInfoList:
            # key = t[0] + ' ' + t[1]
            key = t[1]
            if (tickerTeamDict.has_key(key)):
                tickerTeamDict[key] += t[2]
            else:
                tickerTeamDict[key] = t[2]

        sortedList = sorted(tickerTeamDict.iteritems(), key=lambda d: d[1], reverse=sortFlag)
        startCol = col
        for r in sortedList:
            # desc = r[0] + ' ' + str(r[1])
            sheet.write(row, startCol, r[0])
            sheet.write(row, startCol + 1, r[1])
            if (pctFlag):
                command = str(r[1]) + '/A2'
                sheet.write(row, startCol + 2, xlwt.Formula(command), self.pctStyle)
                startCol += 3
            else:
                startCol += 2

    def exportResultToExcel(self, tickerList, mvList):
        rowNum = 4
        colNum = 0
        # workbook = xlwt.Workbook()
        # sheet = self.workbook.add_sheet('TopMarketValueStocks', cell_overwrite_ok=True)
        sheet = self.topStockSheet
        for i in range(10):
            sheet.write(rowNum, colNum, tickerList[-1 - i])
            sheet.write(rowNum, colNum + 1, mvList[-1 - i])
            command = str(mvList[-1 - i]) + '/A2'
            sheet.write(rowNum, colNum + 2, xlwt.Formula(command), self.pctStyle)
            startCol = colNum + 3
            self.exportTickerTeamInfoToExcel(self.tickerPositionInfo[tickerList[-1 - i]], sheet, rowNum, startCol, True,
                                             True)

            sheet.write(rowNum + 12, colNum, tickerList[i])
            sheet.write(rowNum + 12, colNum + 1, mvList[i])
            command = str(mvList[i]) + '/A2'
            sheet.write(rowNum + 12, colNum + 2, xlwt.Formula(command), self.pctStyle)
            startCol = colNum + 3
            self.exportTickerTeamInfoToExcel(self.tickerPositionInfo[tickerList[i]], sheet, rowNum + 12, startCol,
                                             False, True)
            rowNum += 1

        # workbook.save('fundstatistics.xls')

    def exportLongShortStocksToExcel(self, stockList):
        rowNum = 0
        colNum = 0
        # workbook = xlwt.Workbook()
        sheet = self.workbook.add_sheet('LongShortStocks', cell_overwrite_ok=True)
        for s in stockList:
            sheet.write(rowNum, 0, s)
            startCol = 1
            self.exportTickerTeamInfoToExcel(self.tickerPositionInfo[s], sheet, rowNum, startCol, True)
            rowNum += 1

        # workbook.save('longshortstocks.xls')

    def rankTickerPosition(self, asOfDateStr):
        tickerList = []
        mvList = []
        tickerMVList = []
        pos_raw_data = pd.DataFrame(tickerMVList, columns=['Ticker', 'FundCode','FundBookCode','MVInFundBookCode','UnderlyingTicker'])
        for k, v in self.tickerPositionInfo.items():
            tickerList.append(k)
            marketValue = 0
            for p in v:
                marketValue += p[2]
                data = [k] + list(p)
                pos_raw_data = pd.concat([pos_raw_data, pd.DataFrame([(data)],columns=['Ticker', 'FundCode', 'FundBookCode', 'MVInFundBookCode', 'UnderlyingTicker'])],axis=0, sort=True)
            mvList.append(marketValue)
            tickerMVList.append([k,marketValue])
        ticker_mv_data = pd.DataFrame(tickerMVList,columns=['Ticker', 'MV'])
        ticker_mv_data['MV'] = ticker_mv_data['MV'].astype(float).round(3)
        ticker_mv_data['Pct'] = (ticker_mv_data['MV']/self.AUM_USD).astype(float)
        ticker_mv_data['AsOfDate'] = asOfDateStr
        ticker_mv_data['PositionType'] = RiskFundStatReportPositionType.TICKER_SUM_MV.value
        ticker_mv_data['ReportType'] = RiskFundStatReportType.TOP_MARKET_VALUE_STOCKS.value
        ticker_mv_data.sort_values('MV', ascending=True, inplace=True)

        bottom_10_ticker_data = ticker_mv_data.iloc[:10].copy()
        top_10_ticker_data = ticker_mv_data.iloc[-10:].copy()
        bottom10Records = pdUtil.dataFrameToSavableRecords(bottom_10_ticker_data,['AsOfDate', 'MV', 'PositionType', 'Ticker', 'ReportType','Pct'])
        sql = 'insert into RiskDb.risk.RiskFundStatReport(AsOfDate,Position,PositionType,Ticker,ReportType,Percentage) values(?,?,?,?,?,?)'
        self.insertToDatabase(sql, bottom10Records)

        top10Records = pdUtil.dataFrameToSavableRecords(top_10_ticker_data,['AsOfDate', 'MV', 'PositionType', 'Ticker', 'ReportType','Pct'])
        sql = 'insert into RiskDb.risk.RiskFundStatReport(AsOfDate,Position,PositionType,Ticker,ReportType,Percentage) values(?,?,?,?,?,?)'
        self.insertToDatabase(sql, top10Records)


        top_bottom_10_tickers = list(top_10_ticker_data['Ticker'].unique()) + list(bottom_10_ticker_data['Ticker'].unique())
        pos_raw_data = pos_raw_data[pos_raw_data['Ticker'].isin(top_bottom_10_tickers)]
        pos_raw_data['AsOfDate'] = asOfDateStr
        pos_raw_data['BookCode'] = pos_raw_data['FundBookCode'].str.split(pat='-').str[1]
        pos_raw_data['MVInFundBookCode'] = pos_raw_data['MVInFundBookCode'].astype(float).round(3)
        pos_raw_data['Pct'] = (pos_raw_data['MVInFundBookCode']/self.AUM_USD).astype(float)
        pos_raw_data['PositionType'] = RiskFundStatReportPositionType.TICKER_MV_PER_FUNDBOOK.value
        pos_raw_data['ReportType'] = RiskFundStatReportType.TOP_MARKET_VALUE_STOCKS.value
        records = pdUtil.dataFrameToSavableRecords(pos_raw_data, ['AsOfDate', 'MVInFundBookCode', 'FundCode', 'BookCode', 'PositionType', 'Ticker','ReportType','Pct','FundBookCode'])

        sql = 'insert into RiskDb.risk.RiskFundStatReport(AsOfDate,Position,FundCode,BookCode,PositionType,Ticker,ReportType, Percentage,FundBookCode) values(?,?,?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, records)
        ##self.exportLongShortStocksToExcel(stockList)

    def findLongShortEquity(self, asOfDateStr):
        stockList = []
        stock_pos_df = pd.DataFrame(columns=['Ticker','FundCode', 'FundBookCode', 'MVInFundBookCode', 'UnderlyingTicker'])
        for k, v in self.tickerPositionInfo.items():
            posNum = 0
            negNum = 0
            for r in v:
                if (r[2] > 0):
                    posNum += 1
                elif (r[2] < 0):
                    negNum += 1

            if (posNum > 0 and negNum > 0):
                stockList.append(k)
                stock_pos_tmp = pd.DataFrame(self.tickerPositionInfo[k], columns=['FundCode', 'FundBookCode', 'MVInFundBookCode', 'UnderlyingTicker'])
                stock_pos_tmp['Ticker'] = k
                stock_pos_df = pd.concat([stock_pos_df, stock_pos_tmp],axis=0, sort=True)

        stock_pos_df['MVInFundBookCode'] = stock_pos_df['MVInFundBookCode'].astype(float).round(3)
        stock_pos_df['AsOfDate'] = asOfDateStr
        stock_pos_df['BookCode'] = stock_pos_df['FundBookCode'].str.split(pat='-').str[1]
        stock_pos_df['ReportType'] = RiskFundStatReportType.LONG_SHORT_STOCKS.value
        records = pdUtil.dataFrameToSavableRecords(stock_pos_df, ['AsOfDate', 'MVInFundBookCode', 'FundCode', 'BookCode', 'ReportType', 'Ticker','FundBookCode'])

        sql = 'insert into RiskDb.risk.RiskFundStatReport(AsOfDate,Position,FundCode,BookCode,ReportType,Ticker,FundBookCode) values(?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, records)
        ##self.exportLongShortStocksToExcel(stockList)

    def cleanGivenDateFundStatRecords(self, dateStr):
        sql='delete from RiskDb.risk.RiskFundStatReport where AsOfDate=\''+dateStr+'\''
        self.cursor.execute(sql)

    def insertToDatabase(self,sql,data):
        if data:
            self.cursor.executemany(sql, data)
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def run(self, asOfDateStr, useNewDb):
        self.useNewDb = useNewDb
        if (self.useNewDb == 1):
            self.initSqlserver22()
        else:
            self.initSqlServer()
        self.initLogger()
        self.getPositionsFromDb(asOfDateStr)
        rate = self.getUsdRmbExchangeRate()
        self.getTickerPositionInfo(rate)
        self.cleanGivenDateFundStatRecords(asOfDateStr)
        self.rankTickerPosition(asOfDateStr)
        self.findLongShortEquity(asOfDateStr)
        self.closeSqlServerConnection()


if __name__ == '__main__':
    f = FundStatistics(2600000000)
    currentDay = datetime.datetime.now()
    runDay = currentDay - datetime.timedelta(days=3)
    runDayStr = runDay.strftime('%Y%m%d')
    print runDayStr
    f.run(runDayStr, 1)

