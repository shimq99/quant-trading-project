#encoding:UTF-8
import datetime
import logging
from benchmark.base.Base import Base
import pandas as pd
pd.set_option('display.max_columns', 10)
import decimal
decimal.getcontext().prec = 6
from  decimal import Decimal
import numpy as np
import math
import pyodbc
from IndicatorCalculation import IndicatorCalculation
from datetime import timedelta
import benchmark.tools.PandasDBUtils as pdUtil

class HistoricalPerformance(Base):
    def __init__(self):
        '''
        T5 AUM-鲁贻: 只用从16年开始的数据
        T13 AUM - 因更换PM，故只用从2017-7-13后的数据
        '''
        self.cols_2014=['Date','T1','T2','T3','T4','T8','T9','T10','T11','T12','T14']
        self.cols_2015 = ['Date','T2','T3','T4','T7','T8','T9','T10','T11','T12','T13','T14','T15','T16','T17','T18']
        self.cols_2016 = ['Date','T1','T2','T3','T5','T7','T9','T10','T11','T12','T13','T14','T15','T16','T17','T18','T19','T20','T21','T22','T23','T24','T25']
        self.cols_2017 = ['Date','T3','T5','T9','T10','T11','T12','T13','T14','T15','T16','T17','T19','T20','T21','T22','T23','T24','T25','T26','T27','T28']
        self.cols_2018 = ['Date','T3','T5','T9','T10','T11','T12','T13','T14','T16','T17','T20','T22','T23','T24','T27','T28','T30','T33','T35','T36','T37','T38','T39','W06','W07']

    def insertToDatabase(self,sql,data):
        if data:
            try:
                #self.cursor.executemany(sql, data)
                for record in data:
                    try:
                        self.cursor.execute(sql, (record))
                    except Exception, e:
                        logging.error(record)
                        logging.error('data:'+e.args[1])
                        raise Exception('error')
            except pyodbc.IntegrityError, e:
                '''Integrity Error most likely is duplicate record which could be ignored'''
                logging.warning( 'insertToDatabase: integrity error while saving record, will ignore the error: ' + e.message + e.args[1])
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')


    def saveHistoricalPerformance(self, records):
        if records:
            sql4 = 'insert into RiskDb.risk.HistoricalPerformance (FundId,BookId,BookCode,AsOfDate,' \
                   'MaxDd, Recovery, maxDDFrom, maxDDTo, StartDate, AnnRet, CurrDd, PCT,AnnVol,Sharpe,Aum) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql4,records)


    def saveHistoricalAum(self, records):
        if records:
            sql = 'insert into RiskDb.bench.HistoricalReturn(TradeDate,TeamCode,Aum,Pct) values(?,?,?,?)'
            self.insertToDatabase(sql,records)
        else:
            logging.warn('savePortfolioConstValues: empty record')

    def getAllHistoricalReturnBefore2018(self,teamList):
        sql = 'SELECT TeamCode,TradeDate,Aum,PreAumReplace FROM RiskDb.bench.HistoricalReturn where TeamCode in (\''+('\',\'').join(teamList)+'\')'
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def getBookInfo(self):
        sql = 'SELECT BookId,BookCode,BookDesc FROM RiskDb.ref.Book'
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def loadAumAdjustmentFromExcel(self):
        data = pd.read_excel('C:\\Deployments\\prs-scripts\\2019MDD\\AUM_REPLACE_AFTER_2018.xlsx')
        #data = pd.read_excel('C:\\devel\\2019MDD\\AUM_REPLACE_AFTER_2018.xlsx')
        data['Date'] = pd.to_datetime(data['Date'])
        return data

    def getAllHistoricalReturn(self, fundCode,teamList):
        cols= ['TeamCode','TradeDate','Aum','PreAumReplace', 'YtdGrossReturnInYear','NextDayStartAum']
        return_before_2018 = self.getAllHistoricalReturnBefore2018(teamList)
        return_before_2018['YtdGrossReturnInYear'] = np.nan
        return_after_2018 = self.getAllReturnAfter2018(fundCode,teamList)
        return_after_2018['TeamCode'] = return_after_2018['Book']
        return_after_2018['TradeDate'] = return_after_2018['Date']
        #return_after_2018['PreAumReplace'] = np.nan
        return_after_2018['YtdGrossReturnInYear'] = return_after_2018['YtdGrossReturn']
        return_after_2018.dropna(subset=['YtdGrossReturnInYear'], how='all', inplace=True)
        return_before_2018['NextDayStartAum'] = None
        all_data = pd.concat([return_before_2018[cols], return_after_2018[cols]], axis=0, sort=True)
        book_info = self.getBookInfo()
        book_info.drop_duplicates(subset='BookCode', inplace=True)
        book_info['TeamCode'] = book_info['BookCode']
        all_data = pd.merge(all_data, book_info[['TeamCode','BookId']], how='left', on=['TeamCode'])
        return all_data

    def getAllReturnAfter2018(self, fundCode, teamList):
        sql = 'SELECT NavId,Fund,FundId,Book,BookId,Date,Source,Aum,QdAum,MgmtFee,Currency,DtdGrossReturn,MtdGrossReturn,YtdGrossReturn,DtdNetReturn,MtdNetReturn,YtdNetReturn,NextDayStartAum FROM Portfolio.perf.NavView where Date>=\'2018-01-01\' and BookId is not null  and Book in (\''+('\',\'').join(teamList)+'\') and Fund=\''+fundCode+'\' order by Date'
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        aum_data_adjusted = self.loadAumAdjustmentFromExcel()

        resultDataFrame['Date'] =  pd.to_datetime(resultDataFrame['Date'])
        all_data = pd.merge(resultDataFrame, aum_data_adjusted[['Book','Date','PreAumReplace']], how='left', on=['Book','Date'])
        all_data = all_data[~all_data['Date'].isin(['2019-01-01','2018-01-01'])]

        '''
            del all 2018-01-01 and 2019-01-01 data
        '''
        ##all_data['CurrentDateStartAum'] =all_data['NextDayStartAum'].shift(1)
        return all_data

    def getAllAum(self, data):
        teamCodes = list(data['TeamCode'].unique())
        excel_writer = pd.ExcelWriter('C:\\temp\\AUMs_all.xlsx', engine='openpyxl')
        data['PreAumReplace'] =data['PreAumReplace'].astype(float)
        data['Aum'] = data['Aum'].astype(float)
        for team in teamCodes:
            teamData = data[data['TeamCode']==team].copy()
            teamData['TradeDate'] = pd.to_datetime(teamData['TradeDate'])
            teamData.sort_values('TradeDate', ascending=True, inplace=True)
            teamData['PctChange'] = np.where(teamData['PreAumReplace'].isna(), teamData['Aum'].pct_change(),
                                                     (teamData['Aum'] / teamData['PreAumReplace']) - 1)
            #teamData['PctChange'] = teamData['Aum'].pct_change()
            #unusualPctteamData = teamData[teamData['PctChange']>=0.05]
            #teamData.to_excel('C:\\temp\\'+team+'.xlsx', sheet_name=team)
            teamData[['BookId','TeamCode','TradeDate','Aum','PreAumReplace','YtdGrossReturnInYear','PctChange']].to_excel(excel_writer, sheet_name=team, index=False)
            #unusualPctteamData.to_excel('C:\\temp\\unusual_' + team + '.xlsx', sheet_name=team)
        excel_writer.save()
        excel_writer.close()

    def getHistoricalAUMs(self,filePath):
        try:
            histReturnDataDict = dict()
            xls = pd.ExcelFile(filePath, encoding='utf-8')
            sheetsInfo = xls.sheet_names
            for sheet in sheetsInfo:
                aumDataByYear = pd.read_excel(filePath, sheet_name=sheet)
                histReturnDataDict[sheet] = aumDataByYear
        except IOError, e:
            logging.warn('no file found')
            exit(0)
        return histReturnDataDict

    def filterHistoricalData(self, histReturnData, yearStr):
        if yearStr == '2014':
            columns = self.cols_2014
            histReturnData['TradeDate'] = pd.to_datetime(histReturnData['Date'], format='%m/%d/%Y')
            histReturnData.sort_values('TradeDate', ascending=True, inplace=True)
        elif yearStr == '2015':
            histReturnData = pd.read_excel('C:\\Deployments\\prs-scripts\\2019MDD\\2015 PMSF UPDATED-prod.xlsx',sheet_name='Daily summary')
            #histReturnData = pd.read_excel('C:\\devel\\2019MDD\\2015 PMSF UPDATED-prod.xlsx',sheet_name='Daily summary')
            columns = self.cols_2015
            histReturnData['TradeDate'] = pd.to_datetime(histReturnData['Date'], format='%d/%m/%Y')
            histReturnData.sort_values('TradeDate', ascending=True, inplace=True)
        elif yearStr == '2016':
            columns = self.cols_2016
            histReturnData['TradeDate'] = pd.to_datetime(histReturnData['Date'], format='%m/%d/%Y')
            histReturnData.sort_values('TradeDate', ascending=True, inplace=True)
        elif yearStr == '2017':
            columns = self.cols_2017
            histReturnData['TradeDate'] = pd.to_datetime(histReturnData['Date'], format='%m/%d/%Y')
            histReturnData.sort_values('TradeDate', ascending=True, inplace=True)
        elif yearStr == '2018':
            columns = self.cols_2018
            histReturnData['TradeDate'] = pd.to_datetime(histReturnData['Date'], format='%m/%d/%Y')
            histReturnData.sort_values('TradeDate', ascending=True, inplace=True)
        for columnName in columns:
            if columnName == 'T10':
                teamData = histReturnData[[columnName, 'TradeDate']].copy()
                teamData.dropna(inplace=True)
                teamData['Pct'] = teamData[columnName].astype('float').pct_change()
                teamData['Pct'] = teamData['Pct'].fillna(0)
                teamData['Pct'] = teamData['Pct'].astype(float).round(6)
                teamData['TeamCode'] = columnName
                if yearStr=='2015' and columnName=='T13':
                    teamData = teamData[teamData['TradeDate'] > '2015-07-13']
                    #histReturnData['T13 AUM'] = np.where(histReturnData['TradeDate'] < '2015-07-13', 0, histReturnData['T13 AUM'])
                records = pdUtil.dataFrameToSavableRecords(teamData, ['TradeDate', 'TeamCode', columnName, 'Pct'])
                logging.warn('saving for team:'+columnName+', Year:'+yearStr)
                logging.warn(records)
                self.saveHistoricalAum(records)

    def calcHistPerformance(self, dateStr,fundId, fundCode, teamList):
        hist_data = self.getAllHistoricalReturn(fundCode, teamList)
        hist_data['Date'] = pd.to_datetime(hist_data['TradeDate'])
        hist_data.sort_values('Date', ascending=True, inplace=True)
        teamCodes = list(hist_data['TeamCode'].unique())
        #excel_writer = pd.ExcelWriter('C:\\temp\\HistoricalPerformance2.xlsx', engine='openpyxl')
        for team in teamCodes:
            print team
            # if team not in ['T36']:
            #     continue

            teamData = hist_data[hist_data['TeamCode'] == team].copy()
            teamData['CurrentDateStartAum'] = teamData['NextDayStartAum'].shift(1)
            if team=='T10':
                teamData['Aum'] = np.where(teamData['Date'] == pd.Timestamp(datetime.date(2018, 2, 26)),87591958.41,teamData['Aum'])

            if team=='T03':
                teamData = teamData[teamData['Date']<pd.Timestamp(datetime.date(2018, 10, 18))]

            if team=='T35':
                teamData = teamData[teamData['Date']>=pd.Timestamp(datetime.date(2018,8,15))]

            if team in (['PT02','PT03','PT05']):
                teamData = teamData[teamData['Date']>pd.Timestamp(datetime.date(2018, 11, 12))]

            if team=='T30':
                teamData = teamData[teamData['Date']<pd.Timestamp(datetime.date(2018, 12, 11))]

            if team == 'CS01':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 12, 05))]

            if team == 'CS02':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 11, 15))]

            if team == 'CS05':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 12, 05))]

            if team == 'CS04':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2019, 3, 15))]

            if team == 'T12':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 7, 2))]

            if team == 'T24':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2019, 2, 18))]
            if team == 'T25':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 11, 1))]
            if team == 'T32':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 7,2))]
            if team == 'T36':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2019, 3,25))]
            if team == 'UB01':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 12, 12))]
            if team == 'UB02':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 12, 12))]
            if team == 'UB03':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 11, 28))]
            if team == 'W07':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2019, 03, 18))]
            if team == 'CT01':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 12, 18))]
            if team == 'CT02':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 12, 17))]
            if team == 'CT03':
                teamData = teamData[teamData['Date'] < pd.Timestamp(datetime.date(2018, 11, 14))]

            tradeDateList = list(teamData['Date'].unique())
            columns = ['FundId', 'BookId', 'TeamCode', 'Date', 'MaxDD', 'Recovered', 'MaxDDStartDate', 'MaxDDEndDate', 'StartDate', 'AnnRet', 'CurrDd','PCT','AnnVol','Sharpe','Aum']
            total_result = pd.DataFrame(columns=columns)
            startDate = tradeDateList[0]
            runDateList = list()
            runDateList.append(pd.to_datetime(dateStr, format='%Y-%m-%d'))
            for tradeDate in runDateList:
                valid_data = teamData[teamData['Date'] == tradeDate].copy()
                if not valid_data.empty:
                    date_201900307 = pd.to_datetime('2019-03-07', format='%Y-%m-%d')
                    if tradeDate >= np.datetime64('2018-10-15'):
                        print 'test'
                    teamDataWithDate = teamData[teamData['Date']<=tradeDate].copy()
                    if team == 'PT12':
                        date_201900307 = pd.to_datetime('2019-03-07', format='%Y-%m-%d')
                        teamDataWithDate = teamDataWithDate[
                            ~((teamDataWithDate['Aum'] == 30000000) & (teamDataWithDate['Date'] == date_201900307))]
                    tradeDateStr = str(tradeDate)[:10]
                    if teamDataWithDate.empty:
                        continue
                    teamDataWithDate['Aum'] = teamDataWithDate['Aum'].astype(float).round(5)
                    teamDataWithDate['PreAumReplace'] = teamDataWithDate['PreAumReplace'].astype(float)
                    teamDataWithDate['YtdGrossReturnInYear'] = teamDataWithDate['YtdGrossReturnInYear'].astype(float)
                    teamDataWithDate['StartAum'] = teamDataWithDate['Aum'].iloc[0]
                    #if not teamDataWithDate[teamDataWithDate['Date'] <= '2017-12-31'].empty:
                    if True:
                        '''
                            Due to incomplete data before 2018, this code handle data only Date <= 2017-12-31
                        '''
                        ##YtdGrossReturn = (value at current date / value at last year end) - 1
                        teamDataWithDate['PCT_CHG'] = np.where(teamDataWithDate['PreAumReplace'].isna(),
                                                                 np.where((teamDataWithDate['CurrentDateStartAum'].isna()) | (teamDataWithDate['CurrentDateStartAum']==None),teamDataWithDate['Aum'].pct_change(),(teamDataWithDate['Aum']/teamDataWithDate['CurrentDateStartAum'].astype(float)) - 1),
                                                                 (teamDataWithDate['Aum']/teamDataWithDate['PreAumReplace']) - 1)
                        # teamDataWithDate['PCT_CHG_YTD'] = np.where(teamDataWithDate['YtdGrossReturnInYear'].isna(),
                        #                                           teamDataWithDate['PCT_CHG'],
                        #                                           (teamDataWithDate['YtdGrossReturnInYear']+1) / (teamDataWithDate['YtdGrossReturnInYear'].shift(1)+1) - 1)
                        # teamDataWithDate['PCT_CHG'] = np.where((~teamDataWithDate['PreAumReplace'].isna()) &(~teamDataWithDate['YtdGrossReturnInYear'].isna()),
                        #                                        (teamDataWithDate['Aum'] / teamDataWithDate['PreAumReplace']) - 1,
                        #                                        teamDataWithDate['PCT_CHG'])
                        teamDataWithDate['FundId'] = fundId
                        total_result = self.calcPerformance(fundId, startDate, team, teamDataWithDate, total_result, tradeDate,teamDataWithDate['Aum'].iloc[-1])
            print 'done, saving'
            total_result['MaxDD'] = total_result['MaxDD'].astype(float).round(5)
            #total_result['Current DD'] = total_result['Current DD'].astype(float).round(5)
            #total_result['AnotherMaxDD'] = total_result['AnotherMaxDD'].astype(float).round(5)
            #total_result['AnnVol'] = total_result['AnnVol'].astype(float).round(3)
            total_result['AnnRet'] = total_result['AnnRet'].astype(float).round(3)
            total_result['PCT'] = total_result['PCT'].astype(float).round(3)
            #total_result['Sharpe'] = total_result['Sharpe'].astype(float).round(3)
            #total_result['Aum'] = total_result['Aum'].astype(float).round(3)
            #total_result['Aum'] = np.where(total_result['Aum'].isna(), 0, total_result['Aum'])
            #total_result['YtdGrossReturn'] = total_result['YtdGrossReturn'].astype(float).round(3)
            total_result = total_result.where((pd.notnull(total_result)) | (pd.notna(total_result)), None)
            #savable_result = total_result[total_result['Date'] == pd.to_datetime(dateStr, format='%Y-%m-%d')].copy()
            records = pdUtil.dataFrameToSavableRecords(total_result, columns)
            self.saveHistoricalPerformance(records)
            #total_result.to_excel('C:\\temp\\'+team+'_perf2.xlsx', sheet_name='result')

            #total_result.to_excel(excel_writer, sheet_name=team, index=False)
        #excel_writer.save()
        #excel_writer.close()

    def calcPerformance(self, fundId, startDate, team, teamDataWithDate, total_result, tradeDate, aum):
        (another_maxDD, maxDDStartDateStr, maxDDDateStr, recovered, annualRtn, currentDD, annualVol, annualSharpe) = IndicatorCalculation.calculateRecoveryWithPct(teamDataWithDate, fundId, teamDataWithDate['BookId'].iloc[0])
        pct = teamDataWithDate['PCT_CHG'].iloc[-1]
        bookId = teamDataWithDate[teamDataWithDate['Date'] == tradeDate]['BookId'].iloc[0]
        total_result = total_result.append({'FundId': fundId, 'BookId': bookId, 'TeamCode': team, 'Date': tradeDate,
                                            'MaxDD': another_maxDD,'AnnRet':annualRtn,'CurrDd':currentDD,
                                            'Recovered': recovered, 'MaxDDStartDate': maxDDStartDateStr,
                                            'MaxDDEndDate': maxDDDateStr,
                                            'StartDate': startDate,'PCT':pct,
                                            'AnnVol':annualVol,'Sharpe':annualSharpe,'Aum':aum}, ignore_index=True)
        return total_result


    def toExcel(self):
        aum_sql = 'SELECT Fund,FundId,Book,BookId,Date,Aum FROM Portfolio.perf.NavView where Book IN (\'PT02\',\'PT03\',\'PT05\',\'PT07\',\'PT08\',\'PT11\',\'PT12\',\'PT13\',\'PT14\',\'PT15\') and Fund=\'PTF\''
        sql = 'SELECT BookCode,AsOfDate,PCT FROM RiskDb.risk.HistoricalPerformance where BookCode IN (\'PT02\',\'PT03\',\'PT05\',\'PT07\',\'PT08\',\'PT11\',\'PT12\',\'PT13\',\'PT14\',\'PT15\')'
        index_sql = 'select IndexCode,TradeDate,PctChange from RiskDb.bench.BenchmarkIndexEodPrice where TradeDate >\'2018-01-01\' and IndexCode=\'NKY Index\''
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        bookReturns = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        bookReturns['AsOfDate'] = pd.to_datetime(bookReturns['AsOfDate'])
        bookReturns['Book_PCT'] = bookReturns['PCT'].astype(float)*100
        bookReturns.sort_values('AsOfDate', ascending=True, inplace=True)

        self.cursor.execute(index_sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        indexReturns = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        indexReturns['AsOfDate'] = indexReturns['TradeDate']
        indexReturns['NKYIndex_PCT'] = indexReturns['PctChange'].astype(float)
        indexReturns['AsOfDate'] = pd.to_datetime(indexReturns['AsOfDate'])

        self.cursor.execute(aum_sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        aumData = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        aumData['AsOfDate'] = aumData['Date']
        aumData['AsOfDate'] = pd.to_datetime(aumData['AsOfDate'])
        aumData['BookCode'] = aumData['Book']

        bookList = list(bookReturns['BookCode'].unique())
        excel_writer = pd.ExcelWriter('C:\\temp\\book_pcts.xlsx', engine='openpyxl')
        aum_data_adjusted = self.loadAumAdjustmentFromExcel()
        aum_data_adjusted['AsOfDate'] = aum_data_adjusted['Date']
        aum_data_adjusted['BookCode'] = aum_data_adjusted['Book']



        for book in bookList:
            bookData = bookReturns[bookReturns['BookCode']==book].copy()
            joinedData = pd.merge(bookData[['BookCode','AsOfDate','Book_PCT']], indexReturns[['IndexCode','NKYIndex_PCT','AsOfDate']], how='left', on=['AsOfDate'])
            joinedData = pd.merge(joinedData, aumData[['AsOfDate','BookCode','Aum']], how='left', on=['AsOfDate','BookCode'])
            joinedData = pd.merge(joinedData, aum_data_adjusted[['BookCode', 'AsOfDate', 'PreAumReplace']], how='left', on=['BookCode', 'AsOfDate'])
            joinedData = joinedData.sort_values(['AsOfDate'], ascending=True)
            joinedData['Aum'] = joinedData['Aum'].astype(float)
            joinedData['Book_PCT'] = np.where(joinedData['PreAumReplace'].isna(),
                                              joinedData['Aum'].pct_change(),
                                                   (joinedData['Aum'] / joinedData['PreAumReplace']) - 1)
            joinedData['Book_PCT'] = joinedData['Book_PCT'].astype(float) * 100
            joinedData[['BookCode','AsOfDate','Aum','Book_PCT','IndexCode','NKYIndex_PCT']].to_excel(excel_writer, sheet_name=book, index=False)

        #excel_writer = pd.ExcelWriter('C:\\temp\\stock_weight_test.xlsx', engine='openpyxl')
        excel_writer.save()
        excel_writer.close()

    def runWithDateRange(self, startDateStr, endDateStr):
        startDate = datetime.datetime.strptime(startDateStr, '%Y-%m-%d')
        stopDate = datetime.datetime.strptime(endDateStr, '%Y-%m-%d')
        self.initSqlServer('prod')
        while (startDate <= stopDate):
            if (startDate.weekday() >= 0 and startDate.weekday() <= 4):
                dateStr = startDate.strftime('%Y-%m-%d')
                self.calcHistPerformance(dateStr, 6,'PMSF', ['T05','T09','T10','T11','T13','T14','T16','T17','T20','T22','T23','T27','T28','W06','T33','T35','T37','T38','T39','T40'])
                self.calcHistPerformance(dateStr, 24, 'PTF',
                                                           ['PT02', 'PT03', 'PT05', 'PT07', 'PT08', 'PT11', 'PT12',
                                                            'PT13', 'PT14', 'PT15'])
            startDate = startDate + datetime.timedelta(days=1)
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
        self.runWithDateRange(runYesterDayStr, runYesterDayStr)

if __name__ == '__main__':
    historical_return_data = HistoricalPerformance()

    historical_return_data.runWithDateRange('2019-06-04', '2019-06-12')
    #historical_return_data.run()
    #historical_return_data.toExcel()
    #historical_return_data.getAllAum(historical_return_data.getAllHistoricalReturn())
    # histReturnDataDict = historical_return_data.getHistoricalAUMs('C:\\devel\\2019MDD\\pmsf 2014-2018-prod.xlsx')
    # for yearStr, data in histReturnDataDict.items():
    #     if yearStr == '2014':
    #         historical_return_data.filterHistoricalData(data, '2014')






