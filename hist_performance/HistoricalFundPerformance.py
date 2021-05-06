#encoding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import datetime
import logging
from benchmark.base.Base import Base
import pandas as pd
pd.set_option('display.max_columns', 10)
pd.set_option('precision', 15)
import decimal
decimal.getcontext().prec = 10
from  decimal import Decimal
import benchmark.tools.PandasDBUtils as pdUtil
import tools.PandasExcelUtil as pdExcelUtil
import numpy as np
import math
import pyodbc
from datetime import timedelta
import xlrd
from benchmark.base.CommonEnums import RiskFundMonthlyReportExternalNetValueType
from dateutil import relativedelta
from IndicatorCalculation import IndicatorCalculation
import calendar
import holidays
import shutil


class HistoricalFundPerformance(Base):
    def __init__(self):
        self.HSCEI_CVF_FOUNDED_PX = 13414.83 #2015/6/17 成立时间
        self.SHSZ300_CVF_FOUNDED_PX = 5138.83 #2015/6/17 成立时间
        self.HSCEI_CVF_OPEN_PX = 11850.14 #2015/7/17 运作时间
        self.SHSZ300_CVF_OPEN_PX = 4151.50 #2015/7/17 运作时间

        self.HSCEI_SLHL_FOUNDED_PX = 8756.38 #2016/6/2
        self.SHSZ300_SLHL_FOUNDED_PX = 3167.10  # 2016/6/2
        self.HEDGESTRATEGY_SLHL_FOUNDED_PX = 1816.77#2016/6/2
        self.HSCEI_SLHL_OPEN_PX = 9049.66  #2016/7/15 运作时间
        self.SHSZ300_SLHL_OPEN_PX = 3276.28  #2016/7/15 运作时间
        self.HEDGESTRATEGY_SLHL_OPEN_PX = 1850.51  #2016/7/15 运作时间

        self.HSCEI_ZJNF_FOUNDED_PX = 9790.23 #2016/11/25 成立时间
        self.SHSZ300_ZJNF_FOUNDED_PX = 3521.30  #2016/11/25 成立时间
        self.HSCEI_ZJNF_OPEN_PX = 10297.96  # 2017/2/28 运作时间
        self.SHSZ300_ZJNF_OPEN_PX = 3452.81  # 2017/2/28 运作时间

        self.HSCEI_DCL_FOUNDED_PX = 10604.55 #2019-05-22 成立时间
        self.SHSZ300_DCL_FOUNDED_PX = 3649.38 #2019-05-22 成立时间
        self.HSCEI_DCL_OPEN_PX = 10604.55 #2019-05-22 运作时间
        self.SHSZ300_DCL_OPEN_PX = 3649.38 #2019-05-22 运作时间

        self.PMSF_START_DATE = '2008-03-30'
        self.PMSF_INDEX_START_DATE = '2008-02-29'
        self.PLUS_START_DATE = '2017-10-31'
        self.PLUS_INDEX_START_DATE = '2017-10-16'
        self.PCF_START_DATE = '2005-06-30'
        self.PCF_INDEX_START_DATE = '2005-05-31'

        print 'init'

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

            except Exception, e:
                logging.error('error while insert Data')
                raise
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def selectDataFromDb(self, sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def getFundUnitPrice(self,start_date, end_date, fundCode):
        sql = 'SELECT N.FundId,N.BookId,F.FundCode,B.BookCode,Date,UnitNet as NetUnit FROM Portfolio.perf.Nav N  left join ref.Fund F on F.FundId=N.FundId  left join ref.Book B on B.BookId=N.BookId where Date between \''+start_date+'\' and \''+end_date+'\' and F.FundCode=\''+fundCode+'\' and N.BookId is null'
        result = self.selectDataFromDb(sql)
        result['NetUnit'] = result['NetUnit'].astype(float)
        return result

    def getExchangeClosedInfo(self):
        sql = 'SELECT Id,Date,Market,Country FROM RiskDb.ref.ExchnageClosedInfo'
        data = self.selectDataFromDb(sql)
        data['Date'] = pd.to_datetime(data['Date'])
        return data

    def getIndexPrice(self, start_date, end_date,indexCode):
        sql = 'SELECT IndexCode,TradeDate as Date,ClosePrice FROM RiskDb.bench.BenchmarkIndexEodPrice where IndexCode=\''+indexCode+'\' and TradeDate between \''+start_date+'\' and \''+end_date+'\''
        return self.selectDataFromDb(sql)

    def getHEDGESTRATPrice(self, end_date_str):
        end_date = pd.to_datetime(end_date_str, format='%Y-%m-%d')
        data = pd.read_excel('\\\\192.168.200.3\\ftp\\Quarterly\\ChaoYanIndex\\CHAOYAN_Index1.xls', sheet_name=0, encoding='gb2312', engine='xlrd')
        #data = pd.read_excel('C:\\devel\\2019MDD\\Fund\data\\test.xls', sheet_name=0, encoding='gb2312', engine='xlrd')
        #data['Date'] = data['时间'.decode('utf-8')]
        #data['BenchmarkPrice'] = data['对冲策略精选指数'.decode('utf-8')]
        #data['BenchmarkPrice'].replace('--', np.nan, inplace=True)
        data.dropna(subset=['BenchmarkPrice'], how='all', inplace=True)
        data['BenchmarkPrice'] = data['BenchmarkPrice'].astype(float)
        data.drop(data.columns.difference(['Date', 'BenchmarkPrice']), 1, inplace=True)
        data['Date'] = pd.to_datetime(data['Date'])
        data.sort_values('Date', ascending=True, inplace=True)
        data.index = data['Date']
        start_date = data['Date'].iloc[0]
        data = data[data['Date'] <= end_date]

        fullTradeDateData = pd.DataFrame(pd.date_range(start_date, end_date, freq='B'), index=[pd.date_range(start_date, end_date, freq='B')], columns=['Date'] )
        fullTradeDateData.index.name = None
        data.index.name = None
        fullTradeDateData = pd.merge(fullTradeDateData, data[['Date', 'BenchmarkPrice']], how='left', on=['Date'])

        fullTradeDateData['BenchmarkPrice'] = fullTradeDateData['BenchmarkPrice'].fillna(method='ffill')
        return fullTradeDateData

    def loadAumAdjustmentFromExcel(self, end_date, path, fundCode, benchmarkForSLHL='HEDGESTRAT', ExchangeMarket='China'):
        db_cutoff_date = '2020-09-01'
        content = xlrd.open_workbook(filename=path, encoding_override='gb2312')
        '''Before 2020-09-01: excel'''
        data = pd.read_excel(content, sheet_name=0,encoding='gb2312',engine='xlrd')
        data['Date'] = pd.to_datetime(data['Date'])
        data = data[data['Date'] < pd.to_datetime(db_cutoff_date, format='%Y-%m-%d')]
        '''After 2020-09-01: database'''
        data_from_db = self.getFundUnitPrice(db_cutoff_date, end_date, fundCode)
        data_from_db['Date'] = pd.to_datetime(data_from_db['Date'])
        data_from_db = data_from_db[['Date','NetUnit']]
        data = pd.concat([data, data_from_db], axis=0)
        #data['NetUnit'] = data['单位净值'.decode('utf-8')]
        data['FundCode'] = fundCode
        if fundCode in ['CVF','DCL','ZJNF']:
            '''
            template:C:\\devel\\2019MDD\\Fund\quarterly\\09\\CVF-201910-NEW -template.xlsx
            只需要提供Date和单位净值两列，  benchmark（SHSZ300,HSCEI）数据会从数据库中获取
            '''

            data['Date'] = pd.to_datetime(data['Date'])
            data.index = data['Date']
            data.sort_index(ascending=True, inplace=True)

            shsz300AsOfDate = data['Date'].iloc[0]
            shsz300RunDay = shsz300AsOfDate + datetime.timedelta(days=1)
            if fundCode in ['DCL','CVF','ZJNF']:
                shsz300RunDay = shsz300AsOfDate## + datetime.timedelta(days=1)
            shsz300_run_day_str = shsz300RunDay.strftime('%Y-%m-%d')
            shsz300_rest_data = self.getIndexPrice(shsz300_run_day_str, end_date, 'SHSZ300 Index')
            shsz300_rest_data['SHSZ300_Price'] = shsz300_rest_data['ClosePrice']
            shsz300_rest_data['Date'] = pd.to_datetime(shsz300_rest_data['Date'])
            hsceiAsOfDate = data['Date'].iloc[0]
            hsceiRunDay = hsceiAsOfDate + datetime.timedelta(days=1)
            if fundCode in ['DCL','CVF','ZJNF']:
                hsceiRunDay = hsceiAsOfDate## + datetime.timedelta(days=1)
            hscei_run_day_str = hsceiRunDay.strftime('%Y-%m-%d')
            hscei_rest_data = self.getIndexPrice(hscei_run_day_str, end_date, 'HSCEI Index')
            hscei_rest_data['HSCEI_Price'] = hscei_rest_data['ClosePrice']
            hscei_rest_data['Date'] = pd.to_datetime(hscei_rest_data['Date'])

           #IndexCode,TradeDate as Date,ClosePrice
            data.index.name=None
            shsz300_rest_data.index.name=None
            data = pd.merge(data,shsz300_rest_data[['Date', 'SHSZ300_Price']], how='left', on=['Date'])
            data = pd.merge(data,hscei_rest_data[['Date', 'HSCEI_Price']], how='left', on=['Date'])
            data.index = data['Date']
            data.sort_index(ascending=True, inplace=True)

            data.index.name=None
            data = data[data.index.dayofweek < 5]

        if fundCode=='SLHL' and benchmarkForSLHL=='HEDGESTRAT':

            '''
            template:C:\\devel\\2019MDD\\Fund\quarterly\\09\\SLHL-201909-new -template.xlsx
            只需要提供日期和单位净值两列，  benchmark（HEDGESTRAT）数据会从C:\devel\2019MDD\Fund\data\text.xlsx中获取
            
            数据频率：给每天或月末数据都行，程序会自动按规定的计算频率从每天的数据中萃取月末数据
            '''
            #data['Date'] = data['日期'.decode('utf-8')]
            data.drop(data.columns.difference(['Date','FundCode', 'NetUnit', 'BenchmarkPrice']), 1, inplace=True)
            data['Date'] = pd.to_datetime(data['Date'])
            data.index = data['Date']
            data.sort_index(ascending=True, inplace=True)
            data.index.name = None
            asOfDate = data['Date'].iloc[-1]
            runDay = asOfDate + datetime.timedelta(days=1)
            run_day_str = runDay.strftime('%Y-%m-%d')
            #fund_rest_data = self.getFundUnitPrice(run_day_str, end_date, fundCode)
            #fund_rest_data['Date'] = pd.to_datetime(fund_rest_data['Date'])
            HEDGESTRAT_rest_data = self.getHEDGESTRATPrice(end_date)
            HEDGESTRAT_rest_data['Date'] = pd.to_datetime(HEDGESTRAT_rest_data['Date'])
            HEDGESTRAT_rest_data.index.name = None
            # if not fund_rest_data.empty:
            #     rest_data = pd.merge(HEDGESTRAT_rest_data[['Date', 'BenchmarkPrice']],fund_rest_data[['Date', 'FundCode', 'NetUnit']], how='left', on=['Date'])
            #     rest_data.index = rest_data['Date']
            #     rest_data.sort_index(ascending=True, inplace=True)
            #     data = pd.concat([data, rest_data], axis=0)
            # else:
            data = pd.merge(data,HEDGESTRAT_rest_data[['Date', 'BenchmarkPrice']], how='left', on=['Date'])
            data.index = data['Date']
            data.sort_index(ascending=True, inplace=True)
            data = data[data.index.dayofweek < 5]

        elif fundCode=='SLHL' and benchmarkForSLHL=='SHSZ300':
            data['SHSZ300_Price'] = data['沪深300指数'.decode('utf-8')]
            data.drop(data.columns.difference(['FundCode','NetUnit', 'SHSZ300_Price','HSCEI_Price']), 1, inplace=True)
            data.index = pd.to_datetime(data.index)
            data['Date'] = data.index
            data.sort_index(ascending=True, inplace=True)
            asOfDate = data['Date'].iloc[-1]
            runDay = asOfDate + datetime.timedelta(days=1)
            run_day_str = runDay.strftime('%Y-%m-%d')
            fund_rest_data = self.getFundUnitPrice(run_day_str, end_date, fundCode)
            fund_rest_data['Date'] = pd.to_datetime(fund_rest_data['Date'])
            shsz300_rest_data = self.getIndexPrice(run_day_str, end_date, 'SHSZ300 Index')
            shsz300_rest_data['SHSZ300_Price'] = shsz300_rest_data['ClosePrice']
            shsz300_rest_data['Date'] = pd.to_datetime(shsz300_rest_data['Date'])
            rest_data = pd.merge(fund_rest_data[['Date', 'FundCode', 'NetUnit']],
                                 shsz300_rest_data[['Date', 'SHSZ300_Price']], how='left', on=['Date'])
            rest_data.index = rest_data['Date']
            rest_data.sort_index(ascending=True, inplace=True)
            data = pd.concat([data, rest_data], axis=0)
            data = data[data.index.dayofweek < 5]


        exchange_closed_info=self.getExchangeClosedInfo()
        exchange_closed_info = exchange_closed_info[exchange_closed_info['Country']==ExchangeMarket]
        closed_date = list(exchange_closed_info['Date'].unique())
        data = data[~data['Date'].isin(closed_date)]
        return data

    def loadOnshoreFundData(self, path, fundCode):
        content = xlrd.open_workbook(filename=path, encoding_override='gb2312')
        netunit_data = pd.read_excel(content, sheet_name=fundCode, encoding='gb2312', engine='xlrd')
        netunit_data['NetUnit'] = netunit_data['NetValue_AfterFee']
        netunit_data['FundCode'] = netunit_data['Fund']
        netunit_data['Date'] = pd.to_datetime(netunit_data['Date'])
        netunit_data.index = netunit_data['Date']
        netunit_data.sort_index(ascending=True, inplace=True)
        return netunit_data[['Date','FundCode','NetUnit']]

    def getMonthEndDataOnly(self, dataframe, fundCode):
        dataframe.index.name=None
        dataframe['YearTemp'] = dataframe['Date'].dt.year
        dataframe['MonthTemp'] = dataframe['Date'].dt.month
        years = list(dataframe['YearTemp'].unique())
        months = list(dataframe['MonthTemp'].unique())
        dataframe.sort_values('Date', ascending=True, inplace=True)
        total_result = dataframe.iloc[[0]].copy()
        for year in years:
            year_data = dataframe[dataframe['YearTemp'] == year].copy()
            for m in months:
                month_data = year_data[year_data['MonthTemp']==m].copy()
                if not month_data.empty:
                    month_data.sort_values('Date', ascending=True, inplace=True)

                    # if fundCode=='ZJNF' and year==2016 and m==12:
                    #     total_result = pd.concat([total_result, month_data.iloc[[1]]], axis=0)
                    if fundCode in ['ZJNF'] and year==2018 and m == 12:
                        ##2018-12月应该取 28号的数据
                        total_result = pd.concat([total_result, month_data.iloc[[-2]]], axis=0)
                    else:
                        total_result = pd.concat([total_result,month_data.iloc[[-1]]],axis=0)
                    #total_result = pd.concat([total_result, month_data.iloc[[-1]]], axis=0)
                else:
                    logging.warning(str(year)+'-'+str(m)+', no data')
        total_result.drop_duplicates(subset='Date', inplace=True)
        total_result.sort_values('Date', ascending=True, inplace=True)

        return total_result

    def getWeekEndDataOnly(self, dataframe, fundCode):
        dataframe['YearTemp'] = dataframe['Date'].dt.year
        dataframe['MonthTemp'] = dataframe['Date'].dt.month
        '''0 - Monday 4 - Friday'''
        dataframe['WeekdayTemp'] = dataframe['Date'].dt.dayofweek
        dataframe.sort_values('Date', ascending=True, inplace=True)
        total_result = dataframe[dataframe['WeekdayTemp']==4].copy()
        total_result.drop_duplicates(subset='Date', inplace=True)
        total_result.sort_values('Date', ascending=True, inplace=True)
        return total_result

    def calcOnshoreFundMonthlyFactors(self, dateStr, data, fundCode, netValueType=1,freq='daily'):
        record_date = pd.to_datetime(dateStr, format='%Y-%m-%d')
        data.index = pd.to_datetime(data.index)
        data['Date'] = data.index
        data = data[data['Date'] <= record_date]
        freq_number = 250 ##working day in a year

        if fundCode=='DCL':
            data = data[data['Date'] >= pd.to_datetime('2019-05-22', format='%Y-%m-%d')]
        if fundCode=='SLHL':
            data = data[data['Date'] >= pd.to_datetime('2016-07-15', format='%Y-%m-%d')]
        if fundCode in ['ZJNF', 'CVF','DCL','SLHL']:
            '''
            Sherry: 另外计算的时候要确保价值CVF和石榴SLHL是三位小数净值，紫荆ZJNF和多策略DCL是四位小数,是根据基金合同来的. 四舍五入
            '''
            if fundCode in ['ZJNF','DCL']:
                data['NetUnit'] = data['NetUnit'].astype(float).round(4)
            elif fundCode in ['CVF','SLHL']:
                data['NetUnit'] = data['NetUnit'].astype(float).round(3)

            if fundCode in ['CVF']:
                data = data[data['Date'] >= pd.to_datetime('2015-07-17', format='%Y-%m-%d')]

            if freq == 'daily':
                freq_number = 250  ##working day in a year
            elif freq == 'monthly':
                freq_number = 12
                data = self.getMonthEndDataOnly(data,fundCode)
            data['Fund_PCT'] = data['NetUnit'].astype('float').pct_change()
            data.dropna(subset=['Fund_PCT'], how='all', inplace=True)
            data['Index'] = np.arange(1, len(data))
            data['Fund_AR'] = (1 + data['Fund_PCT']).astype(float).cumprod() - 1  # AR: accumulative return
            data['Fund_Annualized_Return'] = (1 + data['Fund_AR']).pow(1 / (data['Index'])).pow(freq_number) - 1
            data['Fund_PCT'] = data['Fund_PCT'].fillna(0)
            data['Fund_Std'] = data['Fund_PCT'].rolling(len(data), min_periods=2).std() * (freq_number ** 0.5)
            data['Sharpe'] = data['Fund_Annualized_Return'] / data['Fund_Std']
            #data.to_excel('C:\\temp\\SLHL.xlsx')
            #data['SHSZ300_PCT'] = data['SHSZ300_Price'].astype('float').pct_change()
            #data['SHSZ300_AR'] = (1 + data['SHSZ300_PCT']).astype(float).cumprod() - 1  # AR: accumulative return
            #data['SHSZ300_Std'] = data['SHSZ300_PCT'].rolling(len(data), min_periods=2).std() * (250 ** 0.5)

            #data['HSCEI_PCT'] = data['HSCEI_Price'].astype('float').pct_change()
            #data['HSCEI_AR'] = (1 + data['HSCEI_PCT']).astype(float).cumprod() - 1  # AR: accumulative return
            #data['HSCEI_Std'] = data['HSCEI_PCT'].rolling(len(data), min_periods=2).std() * (250 ** 0.5)

            if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                current_year = record_date.year
                current_month = record_date.month
                previous_3m_month = current_month - 2
                previous_3m_data = data[
                    (data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & (
                                data['Date'].dt.month <= current_month)]
                fund_std_3M = previous_3m_data['Fund_PCT'].std() * (freq_number ** 0.5)
                #SHSZ300_std_3M = previous_3m_data['SHSZ300_PCT'].std() * (250 ** 0.5)
                #HSCEI_std_3M = previous_3m_data['HSCEI_PCT'].std() * (250 ** 0.5)

            test_data = data.iloc[::-1].copy()
            #test_data['MIN_HSCEI_AR'] = test_data['HSCEI_AR'].rolling(len(test_data), min_periods=1).min()
            #test_data['MIN_SHSZ300_AR'] = test_data['SHSZ300_AR'].rolling(len(test_data), min_periods=1).min()
            test_data['MIN_Fund_AR'] = test_data['Fund_AR'].rolling(len(test_data), min_periods=1).min()
            data = test_data.iloc[::-1]
            data['DD_Fund'] = -((1 + data['MIN_Fund_AR']) / (1 + data['Fund_AR']) - 1)
            data.loc[:, ('MaxDD_Fund')] = -(data['DD_Fund'].rolling(len(data), min_periods=1).max())
            #data.loc[:, ('DD_SHSZ300')] = -((1 + data['MIN_SHSZ300_AR']) / (1 + data['SHSZ300_AR']) - 1)
            #data.loc[:, ('MaxDD_SHSZ300')] = -(data['DD_SHSZ300'].rolling(len(data), min_periods=1).max())
            #data.loc[:, ('DD_HSCEI')] = -((1 + data['MIN_HSCEI_AR']) / (1 + data['HSCEI_AR']) - 1)
            #data.loc[:, ('MaxDD_HSCEI')] = -(data['DD_SHSZ300'].rolling(len(data), min_periods=1).max())
            data.loc[:, ('PCT_MIN_POWER_2')] = np.where(data['Fund_PCT'] > 0, 0, data['Fund_PCT'].pow(2))
            data.loc[:, ('Annualized_Downside_STD')] = (data['PCT_MIN_POWER_2'].cumsum() / data['Index'] * freq_number) ** 0.5
            data.loc[:, ('Sortino_Ratio')] = np.where(data['Annualized_Downside_STD'] == 0, 0,
                                                      data['Fund_Annualized_Return'] / data[
                                                          'Annualized_Downside_STD'])
            #data['IndexName'] = index_name
            if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                current_year = record_date.year
                current_month = record_date.month
                previous_3m_month = current_month - 2
                previous_3m_data = data[
                    (data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & (
                                data['Date'].dt.month <= current_month)]
                MaxDD_Fund_3M = -(previous_3m_data['DD_Fund'].max())
                #MaxDD_SHSZ300_3M = -(previous_3m_data['DD_SHSZ300'].max())
                #MaxDD_HSCEI_3M = -(previous_3m_data['DD_HSCEI'].max())
            floatCols = ['Fund_PCT', 'Fund_AR', 'Fund_Annualized_Return', 'Fund_Std', 'Sharpe',
                         'DD_Fund', 'Annualized_Downside_STD', 'Sortino_Ratio',]
            savableCols = ['Date', 'FundCode', 'NetUnit', 'Fund_PCT', 'Fund_AR', 'Fund_Annualized_Return',
                           'Fund_Std', 'Sharpe', 'DD_Fund', 'Annualized_Downside_STD', 'Sortino_Ratio', 'MaxDD_Fund']
            data_beta = data.copy()
            data = data.round(6)
            data = self.nanToNone(data, floatCols)
            data = data[data['Date'] == record_date]
            data['NetValueType'] = netValueType
            data['FreqType'] = freq_number
            sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(AsOfDate,FundCode,NetUnit,Pct,CumReturn,AnnReturn,Std,Sharpe,Drawdown,DownsideStd,SortinoRatio,BenchmarkIndexName,BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,Beta,Alpha,AnnAlpha,MaxDrawdown,BenchmarkMaxDrawdown) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            if 'fund_std_3M' in locals():
                data['Fund_Std_3M'] = fund_std_3M
                data['MaxDD_Fund_3M'] = MaxDD_Fund_3M
                savableCols += [ 'Fund_Std_3M', 'MaxDD_Fund_3M']
                sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(AsOfDate,FundCode,NetUnit,Pct,CumReturn,AnnReturn,Std,Sharpe,Drawdown,DownsideStd,SortinoRatio,BenchmarkIndexName,BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,Beta,Alpha,AnnAlpha,MaxDrawdown, BenchmarkMaxDrawdown, Beta3M, Std3M, MaxDrawdown3M, BenchmarkStd3M, Alpha3M, BenchmarkMaxDrawdown3M) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            records = pdUtil.dataFrameToSavableRecords(data, savableCols)
            #self.removeRiskFundBenchmarkReportData(fundCode, index_name, dateStr)
            #self.removeRiskFundMonthlyReportData(fundCode, '', dateStr)
            #self.insertToDatabase(sql, records)
            if not data.empty:
                self.insertIntoMonthlyReport(data)

    def getLastBussinessOnCurrentMonth(self, runMonthStr):
        run_date = pd.to_datetime(runMonthStr, format='%Y-%m')
        run_year = run_date.year
        run_month = run_date.month
        day_range = calendar.monthrange(run_year, run_month)
        start = datetime.date(run_year, run_month, 1)
        end = datetime.date(run_year, run_month, day_range[1])
        #BM::business month end frequency
        business_days_rng = pd.date_range(start, end, freq='BM')
        db = pd.DataFrame()
        db['Date'] = business_days_rng
        return db['Date'][0]

    def next_business_day(self, current_date):
        current_date = datetime.datetime.strptime(current_date, '%Y-%m-%d')
        ONE_DAY = datetime.timedelta(days=1)
        next_day = current_date + ONE_DAY
        while next_day.weekday() in holidays.WEEKEND:
            next_day += ONE_DAY
        return next_day

    def calcFactors(self, dateStr, data, fundCode, benchmarkForSLHL='HEDGESTRAT', freq='daily'):
        record_date = pd.to_datetime(dateStr, format='%Y-%m-%d')
        data.index = pd.to_datetime(data.index)
        data['Date'] = data.index
        data = data[data['Date']<=record_date]
        if fundCode=='ZJNF':
            data_founded = data[data['Date'] >= pd.to_datetime('2016-11-25', format='%Y-%m-%d')].copy()  #成立时间数据
            data = data[data['Date'] >= pd.to_datetime('2017-02-28', format='%Y-%m-%d')]
        if fundCode=='DCL':
            data_founded = data[data['Date'] >= pd.to_datetime('2016-05-22', format='%Y-%m-%d')].copy()  #成立时间数据
            data = data[data['Date'] >= pd.to_datetime('2019-05-22', format='%Y-%m-%d')]
        if fundCode=='SLHL':
            data_founded = data[data['Date'] >= pd.to_datetime('2016-06-02', format='%Y-%m-%d')].copy()  #成立时间数据
            data = data[data['Date'] >= pd.to_datetime('2016-07-15', format='%Y-%m-%d')]
        if fundCode=='CVF':
            data_founded = data[data['Date'] >= pd.to_datetime('2015-06-17', format='%Y-%m-%d')].copy()  #成立时间数据
            data = data[data['Date'] >= pd.to_datetime('2015-07-17', format='%Y-%m-%d')]  #运作时间数据

        if freq == 'daily':
            freq_number = 250  ##working day in a year
        elif freq == 'monthly':
            freq_number = 12
            data = self.getMonthEndDataOnly(data, fundCode)
        elif freq == 'weekly':
            freq_number = 52
            data = self.getWeekEndDataOnly(data, fundCode)

        if fundCode in ['ZJNF', 'CVF', 'DCL']:
            data_copy = data.copy()
            for index_name in ['SHSZ300 Index','HSCEI Index']:
                data = data_copy.copy()
                '''
                Sherry: 另外计算的时候要确保价值CVF和石榴SLHL是三位小数净值，紫荆ZJNF和多策略DCL是四位小数,是根据基金合同来的. 四舍五入
                
                2019-11-06:Yvonne说按实际数据计算，不再截取小数位
                
                Yvonne
                另外根据之前沟通，从1月月报开始，计算麻烦都使用四舍五入后的费后净值，CVF和SLHL保留三位小数，ZJNF和DCL保留四位小数
                1.4545 -> 1.455?  1.4544 -> 1.454?
                '''
                if fundCode in['ZJNF','DCL']:
                    data['NetUnit'] =data['NetUnit'].astype(float).round(4)
                elif fundCode in ['CVF','SLHL']:
                    data['NetUnit'] =data['NetUnit'].astype(float).round(3)

                data['Fund_PCT'] = data['NetUnit'].astype('float').pct_change()
                ##data['NetUnit_MTD'] =

                data['SHSZ300_PCT'] = data['SHSZ300_Price'].astype('float').pct_change()
                data['SHSZ300_AR'] = (1 + data['SHSZ300_PCT']).astype(float).cumprod() - 1 #AR: accumulative return
                data['SHSZ300_Std'] = data['SHSZ300_PCT'].rolling(len(data), min_periods=2).std() * (freq_number ** 0.5)

                data['HSCEI_PCT'] = data['HSCEI_Price'].astype('float').pct_change()
                data['HSCEI_AR'] = (1 + data['HSCEI_PCT']).astype(float).cumprod() - 1 #AR: accumulative return
                data['HSCEI_Std'] = data['HSCEI_PCT'].rolling(len(data), min_periods=2).std() * (freq_number ** 0.5)
                data.dropna(subset=['Fund_PCT'], how='all', inplace=True)
                data['Index'] = np.arange(1, len(data)+1)
                data['Fund_AR'] = (1 + data['Fund_PCT']).astype(float).cumprod() - 1  #AR: accumulative return
                data['HSCEI_Annualized_Return'] = (1+data['HSCEI_AR']).pow(1/(data['Index'])).pow(freq_number) - 1
                data['SHSZ300_Annualized_Return'] = (1+data['SHSZ300_AR']).pow(1/(data['Index'])).pow(freq_number) - 1
                data['Fund_Annualized_Return'] = (1+data['Fund_AR']).pow(1/(data['Index'])).pow(freq_number) - 1
                data['Fund_Std'] = data['Fund_PCT'].dropna().rolling(len(data), min_periods=2).std() * (freq_number ** 0.5)
                data['Sharpe'] = data['Fund_Annualized_Return']/data['Fund_Std']
                data['HSCEI_Sharpe'] = data['HSCEI_Annualized_Return'] / data['HSCEI_Std']
                data['SHSZ300_Sharpe'] = data['SHSZ300_Annualized_Return'] / data['SHSZ300_Std']

                ##ad-hoc: 3,2,1 year annu/sharpe
                year3_data = data[data['Date']>=pd.to_datetime('2017-07-15', format='%Y-%m-%d')].copy()

                year3_data['SHSZ300_Std'] = year3_data['SHSZ300_PCT'].rolling(len(year3_data), min_periods=2).std() * (freq_number ** 0.5)
                year3_data['Index'] = np.arange(1, len(year3_data) + 1)
                year3_data['Fund_AR'] = (1 + year3_data['Fund_PCT']).astype(float).cumprod() - 1  # AR: accumulative return
                year3_data['SHSZ300_Annualized_Return'] = (1 + year3_data['SHSZ300_AR']).pow(1 / (year3_data['Index'])).pow(
                    freq_number) - 1
                year3_data['Fund_Annualized_Return'] = (1 + year3_data['Fund_AR']).pow(1 / (year3_data['Index'])).pow(freq_number) - 1
                year3_data['Fund_Std'] = year3_data['Fund_PCT'].dropna().rolling(len(year3_data), min_periods=2).std() * (freq_number ** 0.5)
                year3_data['Sharpe'] = year3_data['Fund_Annualized_Return'] / year3_data['Fund_Std']
                year3_data['HSCEI_Sharpe'] = year3_data['HSCEI_Annualized_Return'] / year3_data['HSCEI_Std']
                year3_data['SHSZ300_Sharpe'] = year3_data['SHSZ300_Annualized_Return'] / year3_data['SHSZ300_Std']
                year3_data.to_excel('c:\\temp\\year3_data.xlsx')
                year2_data = data[data['Date']>=pd.to_datetime('2018-07-15', format='%Y-%m-%d')].copy()
                year2_data['SHSZ300_Std'] = year2_data['SHSZ300_PCT'].rolling(len(year2_data), min_periods=2).std() * (
                            freq_number ** 0.5)
                year2_data['Index'] = np.arange(1, len(year2_data) + 1)
                year2_data['Fund_AR'] = (1 + year2_data['Fund_PCT']).astype(
                    float).cumprod() - 1  # AR: accumulative return
                year2_data['SHSZ300_Annualized_Return'] = (1 + year2_data['SHSZ300_AR']).pow(
                    1 / (year2_data['Index'])).pow(
                    freq_number) - 1
                year2_data['Fund_Annualized_Return'] = (1 + year2_data['Fund_AR']).pow(1 / (year2_data['Index'])).pow(
                    freq_number) - 1
                year2_data['Fund_Std'] = year2_data['Fund_PCT'].dropna().rolling(len(year2_data),
                                                                                 min_periods=2).std() * (
                                                     freq_number ** 0.5)
                year2_data['Sharpe'] = year2_data['Fund_Annualized_Return'] / year2_data['Fund_Std']
                year2_data['HSCEI_Sharpe'] = year2_data['HSCEI_Annualized_Return'] / year2_data['HSCEI_Std']
                year2_data['SHSZ300_Sharpe'] = year2_data['SHSZ300_Annualized_Return'] / year2_data['SHSZ300_Std']
                year2_data.to_excel('c:\\temp\\year2_data.xlsx')
                year1_data = data[data['Date']>=pd.to_datetime('2019-07-15', format='%Y-%m-%d')].copy()
                year1_data['SHSZ300_Std'] = year1_data['SHSZ300_PCT'].rolling(len(year1_data), min_periods=2).std() * (
                            freq_number ** 0.5)
                year1_data['Index'] = np.arange(1, len(year1_data) + 1)
                year1_data['Fund_AR'] = (1 + year1_data['Fund_PCT']).astype(
                    float).cumprod() - 1  # AR: accumulative return
                year1_data['SHSZ300_Annualized_Return'] = (1 + year1_data['SHSZ300_AR']).pow(
                    1 / (year1_data['Index'])).pow(
                    freq_number) - 1
                year1_data['Fund_Annualized_Return'] = (1 + year1_data['Fund_AR']).pow(1 / (year1_data['Index'])).pow(
                    freq_number) - 1
                year1_data['Fund_Std'] = year1_data['Fund_PCT'].dropna().rolling(len(year1_data),
                                                                                 min_periods=2).std() * (
                                                     freq_number ** 0.5)
                year1_data['Sharpe'] = year1_data['Fund_Annualized_Return'] / year1_data['Fund_Std']
                year1_data['HSCEI_Sharpe'] = year1_data['HSCEI_Annualized_Return'] / year1_data['HSCEI_Std']
                year1_data['SHSZ300_Sharpe'] = year1_data['SHSZ300_Annualized_Return'] / year1_data['SHSZ300_Std']
                year1_data.to_excel('c:\\temp\\year1_data.xlsx')

                if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                    current_year = record_date.year
                    current_month = record_date.month
                    previous_3m_month = current_month-2
                    previous_3m_data = data[(data['Date'].dt.year==current_year) & (data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= current_month)]
                    fund_std_3M = previous_3m_data['Fund_PCT'].std() * (freq_number ** 0.5)
                    SHSZ300_std_3M = previous_3m_data['SHSZ300_PCT'].std() * (freq_number ** 0.5)
                    HSCEI_std_3M = previous_3m_data['HSCEI_PCT'].std() * (freq_number ** 0.5)

                test_data = data.iloc[::-1].copy()
                test_data['MIN_HSCEI_AR'] = test_data['HSCEI_AR'].rolling(len(test_data), min_periods=1).min()
                test_data['MIN_SHSZ300_AR'] = test_data['SHSZ300_AR'].rolling(len(test_data), min_periods=1).min()
                test_data['MIN_Fund_AR'] = test_data['Fund_AR'].rolling(len(test_data), min_periods=1).min()
                data = test_data.iloc[::-1]
                data['DD_Fund'] = -((1 + data['MIN_Fund_AR']) / (1 + data['Fund_AR']) - 1)
                data.loc[:, ('MaxDD_Fund')] = -(data['DD_Fund'].rolling(len(data), min_periods=1).max())
                data.loc[:, ('DD_SHSZ300')] = -((1 + data['MIN_SHSZ300_AR']) / (1 + data['SHSZ300_AR']) - 1)
                data.loc[:, ('MaxDD_SHSZ300')] = -(data['DD_SHSZ300'].rolling(len(data), min_periods=1).max())
                data.loc[:, ('DD_HSCEI')] = -((1 + data['MIN_HSCEI_AR']) / (1 + data['HSCEI_AR']) - 1)
                data.loc[:, ('MaxDD_HSCEI')] = -(data['DD_HSCEI'].rolling(len(data), min_periods=1).max())
                data.loc[:, ('PCT_MIN_POWER_2')] = np.where(data['Fund_PCT'] > 0,0,data['Fund_PCT'].pow(2))
                data.loc[:, ('Annualized_Downside_STD')] = (data['PCT_MIN_POWER_2'].cumsum()/data['Index']*freq_number) ** 0.5
                data.loc[:, ('Sortino_Ratio')] = np.where(data['Annualized_Downside_STD'] == 0, 0, data['Fund_Annualized_Return']/data['Annualized_Downside_STD'])
                data['IndexName'] = index_name
                if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                    current_year = record_date.year
                    current_month = record_date.month
                    previous_3m_month = current_month - 3
                    end_month = current_month - 1
                    if previous_3m_month==0:
                        start_date = pd.to_datetime(str(current_year-1)+'-'+str(12)+'-01')
                        end_date = pd.to_datetime(self.getLastDayOfCurrentMonth(str(current_year)+'-'+str(end_month)))
                        previous_3m_data = data[(data['Date']<=end_date) & (data['Date']>start_date)]
                    else:
                        previous_3m_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= end_month)]
                    MaxDD_Fund_3M = -(previous_3m_data['DD_Fund'].max())
                    MaxDD_SHSZ300_3M = -(previous_3m_data['DD_SHSZ300'].max())
                    MaxDD_HSCEI_3M = -(previous_3m_data['DD_HSCEI'].max())
                floatCols = ['Fund_PCT', 'Fund_AR',  'Fund_Annualized_Return',   'Fund_Std',  'Sharpe',
                                'DD_Fund','Annualized_Downside_STD','Sortino_Ratio','SHSZ300_Price', 'SHSZ300_PCT','SHSZ300_Std','SHSZ300_AR','DD_SHSZ300','SHSZ300_Sharpe','SHSZ300_Annualized_Return']
                savableCols = ['Date', 'FundCode', 'NetUnit', 'Fund_PCT', 'Fund_AR', 'Fund_Annualized_Return',
                               'Fund_Std', 'Sharpe', 'DD_Fund', 'Annualized_Downside_STD', 'Sortino_Ratio',
                               'IndexName', 'SHSZ300_Price', 'SHSZ300_PCT', 'SHSZ300_Std', 'SHSZ300_AR', 'DD_SHSZ300', 'Beta','Alpha','AnnualizedAlpha','MaxDD_Fund','MaxDD_SHSZ300',
                                'SHSZ300_Annualized_Return','SHSZ300_Sharpe',
                                'Fund_MTD', 'Benchmark_MTD', 'Fund_Return_3M', 'Benchmark_Return_3M', 'Fund_Return_1Y', 'Benchmark_Return_1Y', 'Fund_Return_Current1Y', 'Benchmark_Return_Current1Y',
                               'NetUnit','Fund_Return_Since_Open','Benchmark_Return_Since_Open','VolCurrYear',
                               'Fund_MTD_Pre_2M','Benchmark_MTD_Pre_2M','Fund_MTD_Pre_1M','Benchmark_MTD_Pre_1M',
                               'Fund_Return_Since_Founded','Benchmark_Return_Since_Founded',
                               'Fund_Std_Current1Y', 'Benchmark_Std_Current1Y','MaxDD_Fund_Current1Y','MaxDD_Index_Current1Y','Beta_Current1Y',
                               'Fund_Return_Past_3M', 'Benchmark_Return_Past_3M']
                data_beta = data.copy()
                if index_name=='HSCEI Index':
                    floatCols = ['Fund_PCT', 'Fund_AR', 'Fund_Annualized_Return', 'Fund_Std', 'Sharpe',
                                 'DD_Fund', 'Annualized_Downside_STD', 'Sortino_Ratio', 'HSCEI_Price',
                                 'HSCEI_PCT', 'HSCEI_Std', 'HSCEI_AR', 'DD_HSCEI','HSCEI_Sharpe', 'HSCEI_Annualized_Return',]
                    savableCols = ['Date', 'FundCode', 'NetUnit', 'Fund_PCT', 'Fund_AR',  'Fund_Annualized_Return', 'Fund_Std', 'Sharpe',
                                    'DD_Fund', 'Annualized_Downside_STD', 'Sortino_Ratio', 'IndexName',
                                   'HSCEI_Price', 'HSCEI_PCT', 'HSCEI_Std', 'HSCEI_AR', 'DD_HSCEI','Beta', 'Alpha', 'AnnualizedAlpha','MaxDD_Fund','MaxDD_HSCEI',
                                   'HSCEI_Annualized_Return','HSCEI_Sharpe',
                                   'Fund_MTD', 'Benchmark_MTD', 'Fund_Return_3M', 'Benchmark_Return_3M', 'Fund_Return_1Y', 'Benchmark_Return_1Y', 'Fund_Return_Current1Y', 'Benchmark_Return_Current1Y',
                                   'NetUnit','Fund_Return_Since_Open','Benchmark_Return_Since_Open','VolCurrYear',
                                   'Fund_MTD_Pre_2M','Benchmark_MTD_Pre_2M','Fund_MTD_Pre_1M','Benchmark_MTD_Pre_1M',
                                   'Fund_Return_Since_Founded', 'Benchmark_Return_Since_Founded',
                               'Fund_Std_Current1Y', 'Benchmark_Std_Current1Y','MaxDD_Fund_Current1Y','MaxDD_Index_Current1Y','Beta_Current1Y',
                               'Fund_Return_Past_3M', 'Benchmark_Return_Past_3M']
                    data_beta.dropna(subset=['HSCEI_PCT'], how='all', inplace=True)
                    beta, alpha = np.polyfit(data_beta['HSCEI_PCT'].astype(float).tolist(), data_beta['Fund_PCT'].astype(float).tolist(), 1)
                    if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                        current_year = record_date.year
                        current_month = record_date.month
                        previous_3m_month = current_month - 2
                        data_beta_pre_3M = data_beta[(data_beta['Date'].dt.year == current_year) & (data_beta['Date'].dt.month >= previous_3m_month) & (data_beta['Date'].dt.month <= current_month)]
                        HSCEI_PCT_3M = data_beta_pre_3M['HSCEI_PCT'].astype(float).tolist()
                        Fund_PCT_3M = data_beta_pre_3M['Fund_PCT'].astype(float).tolist()
                        beta_3m,alpha_3m= np.polyfit(HSCEI_PCT_3M, Fund_PCT_3M, 1)
                        Index_Std_3M = HSCEI_std_3M
                        MaxDD_Benchmark_3M=MaxDD_HSCEI_3M

                else:
                    data_beta.dropna(subset=['SHSZ300_PCT'], how='all', inplace=True)
                    beta, alpha = np.polyfit(data_beta['SHSZ300_PCT'].astype(float).tolist(), data_beta['Fund_PCT'].astype(float).tolist(), 1)
                    if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                        current_year = record_date.year
                        current_month = record_date.month
                        previous_3m_month = current_month - 2
                        SHSZ300_PCT_3M = data[(data['Date'].dt.year == current_year) & (
                                    data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= current_month)]['SHSZ300_PCT'].astype(float).tolist()
                        Fund_PCT_3M = data[(data['Date'].dt.year == current_year) & (
                                    data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= current_month)]['Fund_PCT'].astype(float).tolist()
                        beta_3m,alpha_3m= np.polyfit(SHSZ300_PCT_3M, Fund_PCT_3M, 1)
                        Index_Std_3M = SHSZ300_std_3M
                        MaxDD_Benchmark_3M=MaxDD_SHSZ300_3M

                annualized_alpha = (alpha + 1) ** freq_number - 1


                current_year = record_date.year
                current_month = record_date.month
                previous_1_year = current_year-1
                previous_3m_month = current_month - 2
                previous_1y_date = record_date + relativedelta.relativedelta(years=-1)

                '''
                与Sherry确认，无论频率是按月按日，按季度，计算前一年的指标时，开始时间都是前一年月末（最后一个工作日） 即2019-11-29，则开始时间是2018-11-30， 而不是2018-11-29
                '''
                ##if freq == 'monthly':
                previous_1y_date = self.next_business_day(self.getLastBussinessOnCurrentMonth(previous_1y_date.strftime('%Y-%m')))

                if (previous_1y_date.strftime('%Y-%m-%d')=='2018-12-31'):
                    previous_1y_date = datetime.datetime.strptime('2018-12-28', '%Y-%m-%d')
                data['DateStr'] = data['Date'].dt.strftime('%Y-%m-%d')

                ''' -2 MTD '''
                pre_2month = current_month-2
                pre_2month_year = current_year
                if current_month==1:
                    pre_2month = 11
                    pre_2month_year = current_year-1
                elif current_month==2:
                    pre_2month = 12
                    pre_2month_year = current_year-1

                pre_2month_data = data[(data['Date'].dt.year == pre_2month_year) & (data['Date'].dt.month == pre_2month)].copy()
                pre_2month_data['Fund_MTD_Pre_2M'] = (1 + pre_2month_data['Fund_PCT']).astype(float).cumprod() - 1
                index_pct_col_name = 'HSCEI_PCT' if index_name == 'HSCEI Index' else 'SHSZ300_PCT'
                pre_2month_data['Benchmark_MTD_Pre_2M'] = (1 + pre_2month_data[index_pct_col_name]).astype(float).cumprod() - 1
                pre_2month_data = pre_2month_data[['Fund_MTD_Pre_2M','Benchmark_MTD_Pre_2M']]
                pre_2month_data['Date'] = record_date

                ''' -1 MTD '''
                pre_1month = current_month-1
                pre_1month_year = current_year
                if current_month==1:
                    pre_1month = 12
                    pre_1month_year = current_year-1
                pre_1month_data = data[(data['Date'].dt.year == pre_1month_year) & (data['Date'].dt.month == pre_1month)].copy()
                pre_1month_data['Fund_MTD_Pre_1M'] = (1 + pre_1month_data['Fund_PCT']).astype(float).cumprod() - 1
                index_pct_col_name = 'HSCEI_PCT' if index_name == 'HSCEI Index' else 'SHSZ300_PCT'
                pre_1month_data['Benchmark_MTD_Pre_1M'] = (1 + pre_1month_data[index_pct_col_name]).astype(float).cumprod() - 1
                pre_1month_data = pre_1month_data[['Fund_MTD_Pre_1M','Benchmark_MTD_Pre_1M']]
                pre_1month_data['Date'] = record_date

                ''' MTD '''
                current_month_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month == current_month)].copy()
                current_month_data['Fund_MTD'] = (1 + current_month_data['Fund_PCT']).astype(float).cumprod() - 1
                index_pct_col_name = 'HSCEI_PCT' if index_name == 'HSCEI Index' else 'SHSZ300_PCT'
                current_month_data['Benchmark_MTD'] = (1 + current_month_data[index_pct_col_name]).astype(float).cumprod() - 1
                current_month_data = current_month_data[['Date','Fund_MTD','Benchmark_MTD']]

                ''' 本年，季度 return - 3m return, '''
                previous_3m_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & ( data['Date'].dt.month <= current_month)].copy()
                previous_3m_data['Fund_Return_3M'] = (1 + previous_3m_data['Fund_PCT']).astype(float).cumprod() - 1
                previous_3m_data['Benchmark_Return_3M'] = (1 + previous_3m_data[index_pct_col_name]).astype(float).cumprod() - 1
                previous_3m_data = previous_3m_data[['Date','Fund_Return_3M','Benchmark_Return_3M']]


                ''' 过去3个月的return'''

                past_3month = current_month-2
                past_3month_year = current_year
                if current_month==1:
                    past_3month = 11
                    past_3month_year = current_year-1
                elif current_month==2:
                    past_3month = 12
                    past_3month_year = current_year-1

                past_3month_date_str = str(past_3month_year)+'-'+str(past_3month)
                past_3month_date = pd.to_datetime(past_3month_date_str)
                past_3month_data = data[data['Date']>=past_3month_date].copy()
                past_3month_data['Fund_Return_Past_3M'] = (1 + past_3month_data['Fund_PCT']).astype(float).cumprod() - 1
                past_3month_data['Benchmark_Return_Past_3M'] = (1 + past_3month_data[index_pct_col_name]).astype(float).cumprod() - 1
                past_3month_data = past_3month_data[['Date','Fund_Return_Past_3M','Benchmark_Return_Past_3M']]

                '''1 year return'''
                previous_1y_data = data[(data['Date'] > previous_1y_date) & (data['Date'] <= record_date)].copy()
                previous_1y_data['Fund_Return_1Y'] = (1 + previous_1y_data['Fund_PCT']).astype(float).cumprod() - 1
                previous_1y_data['Benchmark_Return_1Y'] = (1 + previous_1y_data[index_pct_col_name]).astype(float).cumprod() - 1
                previous_1y_data.to_excel('c:\\temp\\pre.xlsx')
                previous_1y_data = previous_1y_data[['Date', 'Fund_Return_1Y', 'Benchmark_Return_1Y']]

                '''current year return'''
                current_1y_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month <= current_month)].copy()
                current_1y_data['Index'] = np.arange(1, len(current_1y_data) + 1)
                current_1y_data['Fund_Return_Current1Y'] = (1 + current_1y_data['Fund_PCT']).astype(float).cumprod() - 1
                current_1y_data['Benchmark_Return_Current1Y'] = (1 + current_1y_data[index_pct_col_name]).astype(float).cumprod() - 1
                size = current_1y_data.shape[0]
                if size>=2:
                    current_1y_data['Fund_Std_Current1Y'] = current_1y_data['Fund_PCT'].rolling(len(current_1y_data), min_periods=2).std() * (freq_number ** 0.5)
                    current_1y_data['Benchmark_Std_Current1Y'] = current_1y_data[index_pct_col_name].rolling(len(current_1y_data), min_periods=2).std() * (freq_number ** 0.5)

                    current_1y_Index_PCT = current_1y_data[index_pct_col_name].astype(float).tolist()
                    current_1y_Fund_PCT = current_1y_data['Fund_PCT'].astype(float).tolist()
                    current_1y_beta, current_1y_alpha = np.polyfit(current_1y_Index_PCT, current_1y_Fund_PCT, 1)
                    current_1y_data = current_1y_data[['Date','Fund_Return_Current1Y','Benchmark_Return_Current1Y','Fund_Std_Current1Y','Benchmark_Std_Current1Y']]
                else:
                    current_1y_data['Fund_Std_Current1Y'] = 0
                    current_1y_data['Benchmark_Std_Current1Y']=0
                    current_1y_data = current_1y_data[
                        ['Date', 'Fund_Return_Current1Y', 'Benchmark_Return_Current1Y', 'Fund_Std_Current1Y',
                         'Benchmark_Std_Current1Y']]
                    current_1y_beta = 0
                    current_1y_alpha = 0

                '''current year return  maxdd - 范围：从去年12月开始到当前月-1 '''
                current_1y_data2 = data[((data['Date'].dt.year == current_year) & (data['Date'].dt.month <= current_month)) | ((data['Date'].dt.year == current_year-1) & (data['Date'].dt.month == 12))].copy()
                current_1y_data2['Date'] = current_1y_data2['Date'].shift(-1)
                current_1y_data2.dropna(subset=['Date'], how='all', inplace=True)
                current_1y_data2['Index'] = np.arange(1, len(current_1y_data2) + 1)
                size = current_1y_data2.shape[0]
                if size>=2:
                    current_1y_data2.loc[:, ('MaxDD_Fund_Current1Y')] = -(current_1y_data2['DD_Fund'].rolling(len(current_1y_data2), min_periods=1).max())
                    dd_index = 'DD_HSCEI' if index_name == 'HSCEI Index' else 'DD_SHSZ300'
                    current_1y_data2.loc[:, ('MaxDD_Index_Current1Y')] = -(current_1y_data2[dd_index].max())
                    current_1y_data2 = current_1y_data2[['Date','MaxDD_Fund_Current1Y','MaxDD_Index_Current1Y']]
                else:
                    current_1y_data2['MaxDD_Fund_Current1Y']=0
                    current_1y_data2['MaxDD_Index_Current1Y']=0
                    current_1y_data2 = current_1y_data2[['Date', 'MaxDD_Fund_Current1Y', 'MaxDD_Index_Current1Y']]


                ''' return since started'''
                if fundCode in ['ZJNF']: ##运作至今，ZJNF特殊处理。
                    Fund_Return_Since_Open = (data['NetUnit'].iloc[-1] - 0.999902954424024) / 0.999902954424024
                else:
                    Fund_Return_Since_Open = (data['NetUnit'].iloc[-1] -1) / 1

                Fund_Return_Since_FOUNDED = (data_founded['NetUnit'].iloc[-1] -1) / 1

                index_pX_col_name = 'HSCEI_Price' if index_name == 'HSCEI Index' else 'SHSZ300_Price'
                if fundCode=='CVF':
                    if index_name=='HSCEI Index':
                        Benchmark_Return_Since_Open = (data[index_pX_col_name].astype(float).iloc[-1] - self.HSCEI_CVF_OPEN_PX) / self.HSCEI_CVF_OPEN_PX
                        Benchmark_Return_Since_Founded = (data_founded[index_pX_col_name].astype(float).iloc[-1] - self.HSCEI_CVF_FOUNDED_PX) / self.HSCEI_CVF_FOUNDED_PX
                    if index_name=='SHSZ300 Index':
                        Benchmark_Return_Since_Open = (data[index_pX_col_name].astype(float).iloc[-1] - self.SHSZ300_CVF_OPEN_PX) /  self.SHSZ300_CVF_OPEN_PX
                        Benchmark_Return_Since_Founded = (data_founded[index_pX_col_name].astype(float).iloc[-1] - self.SHSZ300_CVF_FOUNDED_PX) /  self.SHSZ300_CVF_FOUNDED_PX

                if fundCode=='ZJNF':
                    if index_name=='HSCEI Index':
                        Benchmark_Return_Since_Open = (data[index_pX_col_name].astype(float).iloc[-1] - self.HSCEI_ZJNF_OPEN_PX) /  self.HSCEI_ZJNF_OPEN_PX
                        Benchmark_Return_Since_Founded = (data_founded[index_pX_col_name].astype(float).iloc[-1] - self.HSCEI_ZJNF_FOUNDED_PX) / self.HSCEI_ZJNF_FOUNDED_PX
                    if index_name=='SHSZ300 Index':
                        Benchmark_Return_Since_Open = (data[index_pX_col_name].astype(float).iloc[-1] - self.SHSZ300_ZJNF_OPEN_PX) /  self.SHSZ300_ZJNF_OPEN_PX
                        Benchmark_Return_Since_Founded = (data_founded[index_pX_col_name].astype(float).iloc[-1] - self.SHSZ300_ZJNF_FOUNDED_PX) /  self.SHSZ300_ZJNF_FOUNDED_PX

                if fundCode=='DCL':
                    if index_name=='HSCEI Index':
                        Benchmark_Return_Since_Open = (data[index_pX_col_name].astype(float).iloc[-1] - self.HSCEI_DCL_OPEN_PX) /  self.HSCEI_DCL_OPEN_PX
                        Benchmark_Return_Since_Founded = (data_founded[index_pX_col_name].astype(float).iloc[-1] - self.HSCEI_DCL_FOUNDED_PX) /  self.HSCEI_DCL_FOUNDED_PX
                    if index_name=='SHSZ300 Index':
                        Benchmark_Return_Since_Open = (data[index_pX_col_name].astype(float).iloc[-1] - self.SHSZ300_DCL_OPEN_PX) /  self.SHSZ300_DCL_OPEN_PX
                        Benchmark_Return_Since_Founded = (data_founded[index_pX_col_name].astype(float).iloc[-1] - self.SHSZ300_DCL_FOUNDED_PX) /  self.SHSZ300_DCL_FOUNDED_PX

                data = data.round(6)
                data = self.nanToNone(data, floatCols)
                data = data[data['Date'] == record_date]

                data = pd.merge(data, pre_1month_data, how='left', on=['Date'])
                data = pd.merge(data, pre_2month_data, how='left', on=['Date'])
                data = pd.merge(data, current_month_data, how='left', on=['Date'])
                data = pd.merge(data, previous_3m_data, how='left', on=['Date'])
                data = pd.merge(data, past_3month_data, how='left', on=['Date'])
                data = pd.merge(data, previous_1y_data, how='left', on=['Date'])
                data = pd.merge(data, current_1y_data, how='left', on=['Date'])
                data = pd.merge(data, current_1y_data2, how='left', on=['Date'])
                data['Fund_Return_Since_Open'] = Fund_Return_Since_Open
                data['Fund_Return_Since_Founded'] = Fund_Return_Since_FOUNDED
                data['Benchmark_Return_Since_Open'] = Benchmark_Return_Since_Open
                data['Benchmark_Return_Since_Founded'] = Benchmark_Return_Since_Founded
                data['Beta'] = beta
                data['Alpha'] = alpha
                data['AnnualizedAlpha'] = annualized_alpha
                data['Beta_Current1Y'] = current_1y_beta
                (annualVol, annualRtn, annualSharpe) = IndicatorCalculation.calculateAnnualVolatilitySharpe(current_1y_data['Fund_Return_Current1Y'].tolist(), tradeDays = freq_number)
                data['VolCurrYear'] = annualVol
                sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(' \
                      'AsOfDate,FundCode,NetUnit,Pct,CumReturn,AnnReturn,Std,Sharpe,' \
                      'Drawdown,DownsideStd,SortinoRatio,BenchmarkIndexName,' \
                      'BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,Beta,Alpha,AnnAlpha,' \
                      'MaxDrawdown,BenchmarkMaxDrawdown,' \
                      'BenchmarkAnnReturn,BenchmarkSharpe, ' \
                      'MTD,MTD_Benchmark,MTD_3M,MTD_3M_Benchmark,YTD_Recent_1Y,YTD_Recent_1Y_Benchmark,YTD_Curr_Year,YTD_Curr_Year_Benchmark,NAV,YTD_Since_Open,' \
                      'YTD_Since_Open_Benchmark,Vol_Curreny_Year,Fund_MTD_Pre_2M,MTD_Pre_2M_Benchmark,Fund_MTD_Pre_1M,MTD_Pre_1M_Benchmark,' \
                      'YTD_Since_Founded,YTD_Since_Founded_Benchmark,' \
                      'Fund_Std_Curr_Year,Bench_Std_Curr_Year,Fund_MaxDD_Curr_Year,Bench_MaxDD_Curr_Year,Beta_Curr_Year,' \
                      'Fund_Return_Past_3M, Benchmark_Return_Past_3M) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                if 'fund_std_3M' in locals():
                    data['Fund_Std_3M'] = fund_std_3M
                    data['Benchmark_Std_3M'] = Index_Std_3M
                    data['Beta_3M'] = beta_3m
                    data['Alpha_3M'] = alpha_3m
                    data['MaxDD_Fund_3M'] = MaxDD_Fund_3M
                    data['MaxDD_Benchmark_3M']=MaxDD_Benchmark_3M
                    savableCols += ['Beta_3M','Fund_Std_3M', 'MaxDD_Fund_3M','Benchmark_Std_3M','Alpha_3M','MaxDD_Benchmark_3M']
                    sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(AsOfDate,FundCode,NetUnit,Pct,CumReturn,AnnReturn,Std,Sharpe,Drawdown,DownsideStd,SortinoRatio,BenchmarkIndexName,' \
                          'BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,Beta,Alpha,AnnAlpha,MaxDrawdown, BenchmarkMaxDrawdown, ' \
                          'BenchmarkAnnReturn,BenchmarkSharpe, ' \
                          'MTD,MTD_Benchmark,MTD_3M,MTD_3M_Benchmark,YTD_Recent_1Y,YTD_Recent_1Y_Benchmark,YTD_Curr_Year,YTD_Curr_Year_Benchmark,NAV,YTD_Since_Open,YTD_Since_Open_Benchmark,Vol_Curreny_Year,Fund_MTD_Pre_2M,MTD_Pre_2M_Benchmark,Fund_MTD_Pre_1M,' \
                          'MTD_Pre_1M_Benchmark,YTD_Since_Founded,YTD_Since_Founded_Benchmark, ' \
                          'Fund_Std_Curr_Year,Bench_Std_Curr_Year,Fund_MaxDD_Curr_Year,Bench_MaxDD_Curr_Year,Beta_Curr_Year,' \
                          'Fund_Return_Past_3M, Benchmark_Return_Past_3M, ' \
                          'Beta3M, Std3M, MaxDrawdown3M, BenchmarkStd3M, ' \
                          'Alpha3M, BenchmarkMaxDrawdown3M) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

                data['NetValueType'] = RiskFundMonthlyReportExternalNetValueType.AFTER_FEE.value
                data['FreqType'] = freq_number
                data = data.fillna(0)
                records = pdUtil.dataFrameToSavableRecords(data, savableCols)
                self.removeRiskFundBenchmarkReportData(fundCode, index_name, dateStr)
                self.insertToDatabase(sql, records)
            self.removeRiskFundMonthlyReportData(fundCode, '', dateStr)
            self.insertIntoMonthlyReport(data)

        if fundCode in ['SLHL'] and benchmarkForSLHL=='HEDGESTRAT':
            '''
                Sherry: 另外计算的时候要确保价值CVF和石榴SLHL是三位小数净值，紫荆ZJNF和多策略DCL是四位小数,是根据基金合同来的. 四舍五入
                
                2019-11-06:Yvonne说按实际数据计算，不再截取小数位
            '''
            index_name=benchmarkForSLHL
            if fundCode=='SLHL':
                data['NetUnit'] = data['NetUnit'].astype(float).round(3)
                data['Fund_PCT'] = data['NetUnit'].astype('float').pct_change()
                #data['Fund_MTD'] =
                # data['Fund_MTD'] =

                data['HEDGESTRAT_PCT'] = data['BenchmarkPrice'].astype('float').pct_change()
                data['HEDGESTRAT_AR'] = (1 + data['HEDGESTRAT_PCT']).astype(float).cumprod() - 1 #AR: accumulative return
                data['HEDGESTRAT_Std'] = data['HEDGESTRAT_PCT'].rolling(len(data), min_periods=2).std() * (freq_number ** 0.5)

                data.dropna(subset=['Fund_PCT'], how='all', inplace=True)
                data['Index'] = np.arange(1, len(data)+1)
                data['Fund_AR'] = (1 + data['Fund_PCT']).astype(float).cumprod() - 1  #AR: accumulative return
                data['Fund_Annualized_Return'] = (1+data['Fund_AR']).pow(1/(data['Index'])).pow(freq_number) - 1
                data['Fund_Std'] = data['Fund_PCT'].dropna().rolling(len(data), min_periods=2).std() * (freq_number ** 0.5)
                data['HEDGESTRAT_Annualized_Return'] = (1+data['HEDGESTRAT_AR']).pow(1/(data['Index'])).pow(freq_number) - 1
                data['Sharpe'] = data['Fund_Annualized_Return']/data['Fund_Std']
                data['HEDGESTRAT_Sharpe'] = data['HEDGESTRAT_Annualized_Return'] / data['HEDGESTRAT_Std']
                if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                    current_year = record_date.year
                    current_month = record_date.month
                    previous_3m_month = current_month-2
                    previous_3m_data = data[(data['Date'].dt.year==current_year) & (data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= current_month)]
                    fund_std_3M = previous_3m_data['Fund_PCT'].std() * (freq_number ** 0.5)
                    HEDGESTRAT_std_3M = previous_3m_data['HEDGESTRAT_PCT'].std() * (freq_number ** 0.5)

                test_data = data.iloc[::-1].copy()
                test_data['MIN_HEDGESTRAT_AR'] = test_data['HEDGESTRAT_AR'].rolling(len(test_data), min_periods=1).min()
                test_data['MIN_Fund_AR'] = test_data['Fund_AR'].rolling(len(test_data), min_periods=1).min()
                data = test_data.iloc[::-1]
                data['DD_Fund'] = -((1 + data['MIN_Fund_AR']) / (1 + data['Fund_AR']) - 1)
                data.loc[:, ('MaxDD_Fund')] = -(data['DD_Fund'].rolling(len(data), min_periods=1).max())
                data.loc[:, ('DD_HEDGESTRAT')] = -((1 + data['MIN_HEDGESTRAT_AR']) / (1 + data['HEDGESTRAT_AR']) - 1)
                data.loc[:, ('MaxDD_HEDGESTRAT')] = -(data['DD_HEDGESTRAT'].rolling(len(data), min_periods=1).max())
                data.loc[:, ('PCT_MIN_POWER_2')] = np.where(data['Fund_PCT'] > 0,0,data['Fund_PCT'].pow(2))
                data.loc[:, ('Annualized_Downside_STD')] = (data['PCT_MIN_POWER_2'].cumsum()/data['Index']*freq_number) ** 0.5
                data.loc[:, ('Sortino_Ratio')] = np.where(data['Annualized_Downside_STD'] == 0, 0, data['Fund_Annualized_Return']/data['Annualized_Downside_STD'])
                data['IndexName'] = index_name
                if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                    current_year = record_date.year
                    current_month = record_date.month
                    previous_3m_month = current_month - 3
                    end_month = current_month - 1
                    if previous_3m_month==0:
                        start_date = pd.to_datetime(str(current_year-1)+'-'+str(12)+'-01')
                        end_date = pd.to_datetime(self.getLastDayOfCurrentMonth(str(current_year)+'-'+str(end_month)))
                        previous_3m_data = data[(data['Date']<=end_date) & (data['Date']>start_date)]
                    else:
                        previous_3m_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= end_month)]
                    MaxDD_Fund_3M = -(previous_3m_data['DD_Fund'].max())
                    MaxDD_HEDGESTRAT_3M = -(previous_3m_data['DD_HEDGESTRAT'].max())
                floatCols = ['Fund_PCT', 'Fund_AR',  'Fund_Annualized_Return',   'Fund_Std',  'Sharpe',
                                'DD_Fund','Annualized_Downside_STD','Sortino_Ratio','BenchmarkPrice', 'HEDGESTRAT_PCT','HEDGESTRAT_Std','HEDGESTRAT_AR','DD_HEDGESTRAT','HEDGESTRAT_Annualized_Return','HEDGESTRAT_Sharpe']
                savableCols = ['Date', 'FundCode', 'NetUnit', 'Fund_PCT', 'Fund_AR', 'Fund_Annualized_Return',
                               'Fund_Std', 'Sharpe', 'DD_Fund', 'Annualized_Downside_STD', 'Sortino_Ratio',
                               'IndexName', 'BenchmarkPrice', 'HEDGESTRAT_PCT', 'HEDGESTRAT_Std', 'HEDGESTRAT_AR', 'DD_HEDGESTRAT', 'Beta','Alpha','AnnualizedAlpha','MaxDD_Fund','MaxDD_HEDGESTRAT',
                               'HEDGESTRAT_Annualized_Return','HEDGESTRAT_Sharpe',
                                'Fund_MTD', 'Benchmark_MTD', 'Fund_Return_3M', 'Benchmark_Return_3M', 'Fund_Return_1Y', 'Benchmark_Return_1Y', 'Fund_Return_Current1Y', 'Benchmark_Return_Current1Y','NetUnit','Fund_Return_Since_Open','Benchmark_Return_Since_Open','VolCurrYear',
                               'Benchmark_MTD_Pre_1M','Benchmark_MTD_Pre_2M','Fund_MTD_Pre_2M','Fund_MTD_Pre_1M',
                               'Fund_Return_Since_Founded', 'Benchmark_Return_Since_Founded',
                               'Fund_Std_Current1Y', 'Benchmark_Std_Current1Y', 'MaxDD_Fund_Current1Y','MaxDD_Index_Current1Y', 'Beta_Current1Y',
                               'Fund_Return_Past_3M', 'Benchmark_Return_Past_3M']
                data_beta = data.copy()
                if index_name=='HSCEI Index':
                    floatCols = ['Fund_PCT', 'Fund_AR', 'Fund_Annualized_Return', 'Fund_Std', 'Sharpe',
                                 'DD_Fund', 'Annualized_Downside_STD', 'Sortino_Ratio', 'HSCEI_Price',
                                 'HSCEI_PCT', 'HSCEI_Std', 'HSCEI_AR', 'DD_HSCEI']
                    savableCols = ['Date', 'FundCode', 'NetUnit', 'Fund_PCT', 'Fund_AR',  'Fund_Annualized_Return', 'Fund_Std', 'Sharpe',
                                    'DD_Fund', 'Annualized_Downside_STD', 'Sortino_Ratio', 'IndexName',
                                   'HSCEI_Price', 'HSCEI_PCT', 'HSCEI_Std', 'HSCEI_AR', 'DD_HSCEI','Beta', 'Alpha', 'AnnualizedAlpha','MaxDD_Fund','MaxDD_HSCEI',
                                   'Fund_MTD', 'Benchmark_MTD', 'Fund_Return_3M', 'Benchmark_Return_3M', 'Fund_Return_1Y', 'Benchmark_Return_1Y', 'Fund_Return_Current1Y', 'Benchmark_Return_Current1Y','NetUnit','Fund_Return_Since_Open','Benchmark_Return_Since_Open','VolCurrYear',
                                   'Benchmark_MTD_Pre_1M','Benchmark_MTD_Pre_2M','Fund_MTD_Pre_2M','Fund_MTD_Pre_1M',
                                   'Fund_Return_Since_Founded', 'Benchmark_Return_Since_Founded',
                               'Fund_Std_Current1Y', 'Benchmark_Std_Current1Y', 'MaxDD_Fund_Current1Y','MaxDD_Index_Current1Y', 'Beta_Current1Y',
                               'Fund_Return_Past_3M', 'Benchmark_Return_Past_3M']
                    data_beta.dropna(subset=['HSCEI_PCT'], how='all', inplace=True)
                    beta, alpha = np.polyfit(data_beta['HSCEI_PCT'].astype(float).tolist(), data_beta['Fund_PCT'].astype(float).tolist(), 1)
                    if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                        current_year = record_date.year
                        current_month = record_date.month
                        previous_3m_month = current_month - 2
                        data_beta_pre_3M = data_beta[(data_beta['Date'].dt.year == current_year) & (data_beta['Date'].dt.month >= previous_3m_month) & (data_beta['Date'].dt.month <= current_month)]
                        HSCEI_PCT_3M = data_beta_pre_3M['HSCEI_PCT'].astype(float).tolist()
                        Fund_PCT_3M = data_beta_pre_3M['Fund_PCT'].astype(float).tolist()
                        beta_3m,alpha_3m= np.polyfit(HSCEI_PCT_3M, Fund_PCT_3M, 1)
                        Index_Std_3M = HSCEI_std_3M
                        MaxDD_Benchmark_3M=MaxDD_HSCEI_3M

                else:
                    data_beta.dropna(subset=['HEDGESTRAT_PCT'], how='all', inplace=True)
                    beta, alpha = np.polyfit(data_beta['HEDGESTRAT_PCT'].astype(float).tolist(), data_beta['Fund_PCT'].astype(float).tolist(), 1)
                    if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                        current_year = record_date.year
                        current_month = record_date.month
                        previous_3m_month = current_month - 2
                        HEDGESTRAT_PCT_3M = data[(data['Date'].dt.year == current_year) & (
                                    data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= current_month)]['HEDGESTRAT_PCT'].astype(float).tolist()
                        Fund_PCT_3M = data[(data['Date'].dt.year == current_year) & (
                                    data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= current_month)]['Fund_PCT'].astype(float).tolist()
                        beta_3m,alpha_3m= np.polyfit(HEDGESTRAT_PCT_3M, Fund_PCT_3M, 1)
                        Index_Std_3M = HEDGESTRAT_std_3M
                        MaxDD_Benchmark_3M=MaxDD_HEDGESTRAT_3M

                annualized_alpha = (alpha + 1) ** freq_number - 1

                current_year = record_date.year
                current_month = record_date.month
                previous_1_year = current_year-1
                previous_3m_month = current_month - 2
                previous_1y_date = record_date + relativedelta.relativedelta(years=-1)



                '''
                与Sherry确认，无论频率是按月按日，按季度，计算前一年的指标时，开始时间都是前一年月末（最后一个工作日） 即2019-11-29，则开始时间是2018-11-30， 而不是 2018-11-29
                '''
                ##if freq == 'monthly':
                previous_1y_date = self.next_business_day(self.getLastBussinessOnCurrentMonth(previous_1y_date.strftime('%Y-%m')))

                if (previous_1y_date.strftime('%Y-%m-%d')=='2018-12-31'):
                    previous_1y_date = datetime.datetime.strptime('2018-12-28', '%Y-%m-%d')
                data['DateStr'] = data['Date'].dt.strftime('%Y-%m-%d')



                ''' -2 MTD '''
                pre_2month = current_month-2
                pre_2month_year = current_year
                if current_month==1:
                    pre_2month = 11
                    pre_2month_year = current_year-1
                elif current_month==2:
                    pre_2month = 12
                    pre_2month_year = current_year-1

                pre_2month_data = data[(data['Date'].dt.year == pre_2month_year) & (data['Date'].dt.month == pre_2month)].copy()
                pre_2month_data['Fund_MTD_Pre_2M'] = (1 + pre_2month_data['Fund_PCT']).astype(float).cumprod() - 1
                pre_2month_data['Benchmark_MTD_Pre_2M'] = (1 + pre_2month_data['HEDGESTRAT_PCT']).astype(float).cumprod() - 1
                pre_2month_data = pre_2month_data[['Fund_MTD_Pre_2M','Benchmark_MTD_Pre_2M']]
                pre_2month_data['Date'] = record_date

                ''' -1 MTD '''
                pre_1month = current_month-1
                pre_1month_year = current_year
                if current_month==1:
                    pre_1month = 12
                    pre_1month_year = current_year-1
                pre_1month_data = data[(data['Date'].dt.year == pre_1month_year) & (data['Date'].dt.month == pre_1month)].copy()
                pre_1month_data['Fund_MTD_Pre_1M'] = (1 + pre_1month_data['Fund_PCT']).astype(float).cumprod() - 1
                pre_1month_data['Benchmark_MTD_Pre_1M'] = (1 + pre_1month_data['HEDGESTRAT_PCT']).astype(float).cumprod() - 1
                pre_1month_data = pre_1month_data[['Date','Fund_MTD_Pre_1M','Benchmark_MTD_Pre_1M']]
                pre_1month_data['Date'] = record_date


                ''' MTD '''
                current_month_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month == current_month)].copy()
                current_month_data['Fund_MTD'] = (1 + current_month_data['Fund_PCT']).astype(float).cumprod() - 1
                current_month_data['Benchmark_MTD'] = (1 + current_month_data['HEDGESTRAT_PCT']).astype(float).cumprod() - 1
                current_month_data = current_month_data[['Date','Fund_MTD','Benchmark_MTD']]

                ''' 3m return'''
                previous_3m_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & ( data['Date'].dt.month <= current_month)].copy()
                previous_3m_data['Fund_Return_3M'] = (1 + previous_3m_data['Fund_PCT']).astype(float).cumprod() - 1
                previous_3m_data['Benchmark_Return_3M'] = (1 + previous_3m_data['HEDGESTRAT_PCT']).astype(float).cumprod() - 1
                previous_3m_data = previous_3m_data[['Date','Fund_Return_3M','Benchmark_Return_3M']]

                ''' 过去3个月的return'''

                past_3month = current_month - 2
                past_3month_year = current_year
                if current_month == 1:
                    past_3month = 11
                    past_3month_year = current_year - 1
                elif current_month == 2:
                    past_3month = 12
                    past_3month_year = current_year - 1

                past_3month_date_str = str(past_3month_year) + '-' + str(past_3month)
                past_3month_date = pd.to_datetime(past_3month_date_str)

                past_3month_data = data[data['Date'] >= past_3month_date].copy()
                past_3month_data['Fund_Return_Past_3M'] = (1 + past_3month_data['Fund_PCT']).astype(float).cumprod() - 1
                past_3month_data['Benchmark_Return_Past_3M'] = (1 + past_3month_data['HEDGESTRAT_PCT']).astype(
                    float).cumprod() - 1
                past_3month_data = past_3month_data[['Date', 'Fund_Return_Past_3M', 'Benchmark_Return_Past_3M']]

                '''1 year return'''
                previous_1y_data = data[(data['Date'] > previous_1y_date) & (data['Date'] <= record_date)].copy()
                previous_1y_data['Fund_Return_1Y'] = (1 + previous_1y_data['Fund_PCT']).astype(float).cumprod() - 1
                previous_1y_data['Benchmark_Return_1Y'] = (1 + previous_1y_data['HEDGESTRAT_PCT']).astype(float).cumprod() - 1
                previous_1y_data = previous_1y_data[['Date','Fund_Return_1Y','Benchmark_Return_1Y']]

                '''current year return'''
                current_1y_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month <= current_month)].copy()
                current_1y_data['Index'] = np.arange(1, len(current_1y_data) + 1)
                current_1y_data['Fund_Return_Current1Y'] = (1 + current_1y_data['Fund_PCT']).astype(float).cumprod() - 1
                current_1y_data['Benchmark_Return_Current1Y'] = (1 + current_1y_data['HEDGESTRAT_PCT']).astype(float).cumprod() - 1
                size = current_1y_data.shape[0]
                if size>=2:
                    current_1y_data['Fund_Std_Current1Y'] = current_1y_data['Fund_PCT'].rolling(len(current_1y_data),min_periods=2).std() * (freq_number ** 0.5)
                    current_1y_data['Benchmark_Std_Current1Y'] = current_1y_data['HEDGESTRAT_PCT'].rolling(len(current_1y_data), min_periods=2).std() * (freq_number ** 0.5)
                    current_1y_Index_PCT = current_1y_data['HEDGESTRAT_PCT'].astype(float).tolist()
                    current_1y_Fund_PCT = current_1y_data['Fund_PCT'].astype(float).tolist()
                    current_1y_beta, current_1y_alpha = np.polyfit(current_1y_Index_PCT, current_1y_Fund_PCT, 1)
                    current_1y_data = current_1y_data[['Date','Fund_Return_Current1Y','Benchmark_Return_Current1Y','Fund_Std_Current1Y','Benchmark_Std_Current1Y']]
                else:
                    current_1y_data['Fund_Std_Current1Y'] = 0
                    current_1y_data['Benchmark_Std_Current1Y']=0
                    current_1y_data = current_1y_data[
                        ['Date', 'Fund_Return_Current1Y', 'Benchmark_Return_Current1Y', 'Fund_Std_Current1Y',
                         'Benchmark_Std_Current1Y']]
                    current_1y_beta = 0
                    current_1y_alpha = 0

                '''current year return  maxdd - 范围：从去年12月开始到当前月-1 '''
                current_1y_data2 = data[((data['Date'].dt.year == current_year) & (data['Date'].dt.month <= current_month)) | ((data['Date'].dt.year == current_year-1) & (data['Date'].dt.month == 12))].copy()
                current_1y_data2['Date'] = current_1y_data2['Date'].shift(-1)
                current_1y_data2.dropna(subset=['Date'], how='all', inplace=True)
                current_1y_data2['Index'] = np.arange(1, len(current_1y_data2) + 1)
                size = current_1y_data2.shape[0]
                if size>=2:
                    current_1y_data2.loc[:, ('MaxDD_Fund_Current1Y')] = -(current_1y_data2['DD_Fund'].rolling(len(current_1y_data2), min_periods=1).max())
                    current_1y_data2.loc[:, ('MaxDD_Index_Current1Y')] = -(current_1y_data2['DD_HEDGESTRAT'].max())
                    current_1y_data2 = current_1y_data2[['Date','MaxDD_Fund_Current1Y','MaxDD_Index_Current1Y']]
                else:
                    current_1y_data2['MaxDD_Fund_Current1Y']=0
                    current_1y_data2['MaxDD_Index_Current1Y']=0
                    current_1y_data2 = current_1y_data2[['Date', 'MaxDD_Fund_Current1Y', 'MaxDD_Index_Current1Y']]


                ''' return since started'''
                Fund_Return_Since_Open = (data['NetUnit'].iloc[-1] -1) / 1
                Fund_Return_Since_Founded = (data_founded['NetUnit'].iloc[-1] -1) / 1
                Benchmark_Return_Since_Open = (data['BenchmarkPrice'].astype(float).iloc[-1] - self.HEDGESTRATEGY_SLHL_OPEN_PX) / self.HEDGESTRATEGY_SLHL_OPEN_PX
                Benchmark_Return_Since_Founded = (data_founded['BenchmarkPrice'].astype(float).iloc[-1] - self.HEDGESTRATEGY_SLHL_FOUNDED_PX) / self.HEDGESTRATEGY_SLHL_FOUNDED_PX


                data = data.round(6)
                data = self.nanToNone(data, floatCols)
                data = data[data['Date'] == record_date]

                data = pd.merge(data, pre_1month_data, how='left', on=['Date'])
                data = pd.merge(data, pre_2month_data, how='left', on=['Date'])
                data = pd.merge(data, current_month_data, how='left', on=['Date'])
                data = pd.merge(data, previous_3m_data, how='left', on=['Date'])
                data = pd.merge(data, past_3month_data, how='left', on=['Date'])
                data = pd.merge(data, previous_1y_data, how='left', on=['Date'])
                data = pd.merge(data, current_1y_data, how='left', on=['Date'])
                data = pd.merge(data, current_1y_data2, how='left', on=['Date'])
                data['Fund_Return_Since_Open'] = Fund_Return_Since_Open
                data['Benchmark_Return_Since_Open'] = Benchmark_Return_Since_Open
                data['Fund_Return_Since_Founded'] = Fund_Return_Since_Founded
                data['Benchmark_Return_Since_Founded'] = Benchmark_Return_Since_Founded
                data['Beta'] = beta
                data['Alpha'] = alpha
                data['Beta_Current1Y'] = current_1y_beta
                (annualVol, annualRtn, annualSharpe) = IndicatorCalculation.calculateAnnualVolatilitySharpe(current_1y_data['Fund_Return_Current1Y'].tolist(), tradeDays = freq_number)
                data['VolCurrYear'] = annualVol
                data['AnnualizedAlpha'] = annualized_alpha
                sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(AsOfDate,FundCode,NetUnit,Pct,CumReturn,AnnReturn,Std,Sharpe,Drawdown,DownsideStd,SortinoRatio,BenchmarkIndexName,' \
                      'BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,Beta,Alpha,AnnAlpha,MaxDrawdown,BenchmarkMaxDrawdown,' \
                      'BenchmarkAnnReturn,BenchmarkSharpe, ' \
                      'MTD,MTD_Benchmark,MTD_3M,MTD_3M_Benchmark,YTD_Recent_1Y,YTD_Recent_1Y_Benchmark,YTD_Curr_Year,YTD_Curr_Year_Benchmark,NAV,YTD_Since_Open,YTD_Since_Open_Benchmark,Vol_Curreny_Year,MTD_Pre_1M_Benchmark,MTD_Pre_2M_Benchmark,Fund_MTD_Pre_2M,Fund_MTD_Pre_1M,YTD_Since_Founded,YTD_Since_Founded_Benchmark,' \
                      'Fund_Std_Curr_Year,Bench_Std_Curr_Year,Fund_MaxDD_Curr_Year,Bench_MaxDD_Curr_Year,Beta_Curr_Year,' \
                      'Fund_Return_Past_3M, Benchmark_Return_Past_3M) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                if 'fund_std_3M' in locals():
                    data['Fund_Std_3M'] = fund_std_3M
                    data['Benchmark_Std_3M'] = Index_Std_3M
                    data['Beta_3M'] = beta_3m
                    data['Alpha_3M'] = alpha_3m
                    data['MaxDD_Fund_3M'] = MaxDD_Fund_3M
                    data['MaxDD_Benchmark_3M']=MaxDD_Benchmark_3M
                    savableCols += ['Beta_3M','Fund_Std_3M', 'MaxDD_Fund_3M','Benchmark_Std_3M','Alpha_3M','MaxDD_Benchmark_3M']
                    sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(AsOfDate,FundCode,NetUnit,Pct,CumReturn,AnnReturn,Std,Sharpe,Drawdown,DownsideStd,SortinoRatio,BenchmarkIndexName,' \
                          'BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,Beta,Alpha,AnnAlpha,MaxDrawdown, BenchmarkMaxDrawdown,' \
                          'BenchmarkAnnReturn,BenchmarkSharpe, ' \
                          'MTD,MTD_Benchmark,MTD_3M,MTD_3M_Benchmark,YTD_Recent_1Y,YTD_Recent_1Y_Benchmark,YTD_Curr_Year,YTD_Curr_Year_Benchmark,NAV,YTD_Since_Open,YTD_Since_Open_Benchmark,Vol_Curreny_Year,MTD_Pre_1M_Benchmark,MTD_Pre_2M_Benchmark,Fund_MTD_Pre_2M,Fund_MTD_Pre_1M,YTD_Since_Founded,YTD_Since_Founded_Benchmark,' \
                          'Fund_Std_Curr_Year,Bench_Std_Curr_Year,Fund_MaxDD_Curr_Year,Bench_MaxDD_Curr_Year,Beta_Curr_Year,' \
                            'Fund_Return_Past_3M,Benchmark_Return_Past_3M,' \
                          'Beta3M, Std3M, MaxDrawdown3M, BenchmarkStd3M, Alpha3M, BenchmarkMaxDrawdown3M) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

                data['NetValueType'] = RiskFundMonthlyReportExternalNetValueType.AFTER_FEE.value
                data['FreqType'] = freq_number
                data = data.fillna(0)
                records = pdUtil.dataFrameToSavableRecords(data, savableCols)
                self.removeRiskFundBenchmarkReportData(fundCode, index_name, dateStr)
                self.removeRiskFundMonthlyReportData(fundCode, '', dateStr)
                self.insertToDatabase(sql, records)
                self.insertIntoMonthlyReport(data)


        if fundCode in ['SLHL'] and benchmarkForSLHL=='SHSZ300': #### 基准：对冲策略精选指数
            data['NetUnit'] = data['NetUnit'].astype(float)
            data['Index'] = np.arange(1, len(data) + 1)
            data['SHSZ300_PCT'] = data['SHSZ300_Price'].astype('float').pct_change()
            data['SHSZ300_AR'] = (1 + data['SHSZ300_PCT']).astype(float).cumprod() - 1 #AR: accumulative return
            data['SHSZ300_Std'] = data['SHSZ300_PCT'].rolling(len(data), min_periods=2).std() * (freq_number ** 0.5)

            data['Fund_PCT'] = data['NetUnit'].astype('float').pct_change()
            data['Fund_PCT'] = data['Fund_PCT'].fillna(0)
            data['Fund_AR'] = (1 + data['Fund_PCT']).astype(float).cumprod() - 1  #AR: accumulative return
            data['Fund_Annualized_Return'] = (1+data['Fund_AR']).pow(1/(data['Index'])).pow(freq_number) - 1
            data['Fund_Std'] = data['Fund_PCT'].rolling(len(data), min_periods=2).std() * (freq_number ** 0.5)
            data['Sharpe'] = data['Fund_Annualized_Return']/data['Fund_Std']

            if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                current_year = record_date.year
                current_month = record_date.month
                previous_3m_month = current_month - 2
                previous_3m_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & (
                                data['Date'].dt.month <= current_month)]
                fund_std_3M = previous_3m_data['Fund_PCT'].std() * (freq_number ** 0.5)
                benchmark_std_3M = previous_3m_data['SHSZ300_PCT'].std() * (freq_number ** 0.5)

            test_data = data.iloc[::-1].copy()
            test_data['MIN_SHSZ300_AR'] = test_data['SHSZ300_AR'].rolling(len(test_data), min_periods=1).min()
            test_data['MIN_Fund_AR'] = test_data['Fund_AR'].rolling(len(test_data), min_periods=1).min()
            data = test_data.iloc[::-1]
            data['DD_Fund'] = -((1 + data['MIN_Fund_AR']) / (1 + data['Fund_AR']) - 1)
            data['MaxDD_Fund'] = -(data['DD_Fund'].rolling(len(data), min_periods=1).max())
            data['DD_SHSZ300'] = -((1 + data['MIN_SHSZ300_AR']) / (1 + data['SHSZ300_AR']) - 1)
            data['MaxDD_SHSZ300'] = -(data['DD_SHSZ300'].rolling(len(data), min_periods=1).max())
            data.loc[:, ('PCT_MIN_POWER_2')] = np.where(data['Fund_PCT'] > 0,0,data['Fund_PCT'].pow(2))
            data.loc[:, ('Annualized_Downside_STD')] = (data['PCT_MIN_POWER_2'].cumsum()/data['Index']*freq_number) ** 0.5
            data.loc[:, ('Sortino_Ratio')] = np.where(data['Annualized_Downside_STD'] == 0, 0, data['Fund_Annualized_Return']/data['Annualized_Downside_STD'])

            data_beta = data.copy()
            data_beta.dropna(subset=['SHSZ300_PCT'], how='all', inplace=True)
            beta, alpha = np.polyfit(data_beta['SHSZ300_PCT'].astype(float).tolist(),data_beta['Fund_PCT'].astype(float).tolist(),1)
            annualized_alpha = (alpha + 1) ** freq_number - 1
            #slope,intercept = np.polyfit(m,n,1).round(2)  #回归 1：代表是1元方程 2：是2元方程
            index_name='SHSZ300 Index'
            data['IndexName'] = index_name

            if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                current_year = record_date.year
                current_month = record_date.month
                previous_3m_month = current_month - 3
                end_month = current_month -1
                previous_3m_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= end_month)]
                data_beta_pre_3M = data_beta[(data_beta['Date'].dt.year == current_year) & (data_beta['Date'].dt.month >= previous_3m_month) & (data_beta['Date'].dt.month <= current_month)]
                MaxDD_Fund_3M = -(previous_3m_data['DD_Fund'].max())
                MaxDD_SHSZ300_3M = -(previous_3m_data['DD_SHSZ300'].max())
                SHSZ300_PCT_3M = data_beta_pre_3M['SHSZ300_PCT'].astype(float).tolist()
                Fund_PCT_3M = data_beta_pre_3M['Fund_PCT'].astype(float).tolist()
                beta_3m, alpha_3m = np.polyfit(SHSZ300_PCT_3M, Fund_PCT_3M, 1)
            floatCols = ['Fund_PCT', 'Fund_AR',  'Fund_Annualized_Return',   'Fund_Std',  'Sharpe',
                            'DD_Fund','Annualized_Downside_STD','Sortino_Ratio','SHSZ300_Price', 'SHSZ300_PCT','SHSZ300_Std','SHSZ300_AR','DD_SHSZ300', 'MaxDD_Fund']
            savableCols = ['Date', 'FundCode', 'NetUnit', 'Fund_PCT', 'Fund_AR',  'Fund_Annualized_Return',   'Fund_Std',  'Sharpe',
                            'DD_Fund','Annualized_Downside_STD','Sortino_Ratio','IndexName', 'SHSZ300_Price',
                           'SHSZ300_PCT','SHSZ300_Std','SHSZ300_AR','DD_SHSZ300','Beta','Alpha','AnnualizedAlpha','MaxDD_Fund','MaxDD_SHSZ300']

            data = data.round(6)
            data = self.nanToNone(data, floatCols)
            data = data[data['Date']==record_date]
            data['Beta'] = beta
            data['Alpha'] = alpha
            data['AnnualizedAlpha'] = annualized_alpha
            sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(AsOfDate,FundCode,NetUnit,Pct,CumReturn,AnnReturn,Std,Sharpe,Drawdown,DownsideStd,SortinoRatio,BenchmarkIndexName,BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,Beta,Alpha,AnnAlpha,MaxDrawdown,BenchmarkMaxDrawdown) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            if 'fund_std_3M' in locals():
                data['Fund_Std_3M'] = fund_std_3M
                data['Benchmark_Std_3M'] = benchmark_std_3M
                data['Beta_3M'] = beta_3m
                data['Alpha_3M'] = alpha_3m
                data['MaxDD_Fund_3M'] = MaxDD_Fund_3M
                data['MaxDD_Benchmark_3M'] = MaxDD_SHSZ300_3M
                savableCols += ['Beta_3M', 'Fund_Std_3M', 'MaxDD_Fund_3M', 'Benchmark_Std_3M', 'Alpha_3M','MaxDD_Benchmark_3M']
                sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(AsOfDate,FundCode,NetUnit,Pct,CumReturn,AnnReturn,Std,Sharpe,Drawdown,DownsideStd,SortinoRatio,BenchmarkIndexName,BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,Beta,Alpha,AnnAlpha,MaxDrawdown, BenchmarkMaxDrawdown, Beta3M, Std3M, MaxDrawdown3M, BenchmarkStd3M, Alpha3M, BenchmarkMaxDrawdown3M) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

            data['NetValueType'] = RiskFundMonthlyReportExternalNetValueType.BEFORE_FEE.value
            records = pdUtil.dataFrameToSavableRecords(data, savableCols)
            self.removeRiskFundBenchmarkReportData(fundCode, index_name, dateStr)
            self.removeRiskFundMonthlyReportData(fundCode, '', dateStr)
            self.insertToDatabase(sql, records)
            self.insertIntoMonthlyReport(data)

        elif fundCode=='SLHL' and benchmarkForSLHL=='HEDGESTRAT1_NOUSED': #### 基准：对冲策略精选指数
            data['NetUnit'] = data['NetUnit'].astype(float).round(3)
            data.sort_index(ascending=True, inplace=True)
            data.index = pd.to_datetime(data.index)
            #data.dropna(subset=['NetUnit'], how='all', inplace=True)
            data['BENCHMARK_WEEKLY_PCT'] = data['BenchmarkPrice'].astype('float').pct_change()
            data['BENCHMARK_WEEKLY_PCT'].iloc[0] = 0
            data['BENCHMARK_YTD'] = (data['BenchmarkPrice'].astype('float')/data[data.index == pd.Timestamp(datetime.date(2016, 07, 15))]['BenchmarkPrice'].astype('float').iloc[0]) - 1
            #data['BENCHMARK_AR'] = (1 + data['BENCHMARK_WEEKLY_PCT']).astype(float).cumprod() - 1 #AR: accumulative return
            data['BENCHMARK_AR'] = (data['BenchmarkPrice'].astype(float)/data['BenchmarkPrice'].astype(float).iloc[0])  - 1 # AR: accumulative return
            data['BENCHMARK_STD'] = data['BENCHMARK_WEEKLY_PCT'].rolling(len(data), min_periods=2).std() * (52 ** 0.5)

            data['Fund_WEEKLY_PCT'] = data['NetUnit'].astype('float').pct_change()
            data['Fund_WEEKLY_PCT'].iloc[0] = 0
            #data['Fund_AR'] = (1 + data['Fund_WEEKLY_PCT']).astype(float).cumprod() - 1 #AR: accumulative return
            data['Fund_AR'] = (1 + data['Fund_WEEKLY_PCT']).astype(float).cumprod() - 1 #AR: accumulative return
            data['Fund_STD'] = data['Fund_WEEKLY_PCT'].rolling(len(data), min_periods=2).std() * (52 ** 0.5)

            if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                current_year = record_date.year
                current_month = record_date.month
                previous_3m_month = current_month - 2
                previous_3m_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= current_month)]
                fund_std_3M = previous_3m_data['Fund_WEEKLY_PCT'].std() * (52 ** 0.5)
                benchmark_std_3M = previous_3m_data['BENCHMARK_WEEKLY_PCT'].std() * (52 ** 0.5)

            test_data = data.iloc[::-1].copy()
            #test_data['BENCHMARK_AR+1'] = test_data['BENCHMARK_AR'].astype(float).shift(1)
            test_data['MIN_BENCHMARK_AR'] = test_data['BENCHMARK_AR'].rolling(len(test_data), min_periods=1).min()

            #test_data['Fund_AR+1'] = test_data['Fund_AR'].astype(float).shift(1)
            test_data['MIN_Fund_AR'] = test_data['Fund_AR'].rolling(len(test_data), min_periods=1).min()
            data = test_data.iloc[::-1]
            data['Benchmark_DD'] = - ((1 + data['MIN_BENCHMARK_AR']).astype(float) / (1 + data['BENCHMARK_AR']).astype(float) - 1)
            data['Fund_DD'] = -((1 + data['MIN_Fund_AR']).astype(float) / (1 + data['Fund_AR']).astype(float) - 1)
            data['MaxDD_Fund'] = -(data['Fund_DD'].rolling(len(data), min_periods=1).max())
            data['MaxDD_Benchmark'] = -(data['Benchmark_DD'].rolling(len(data), min_periods=1).max())
            index_name='HEDGESTRAT Index'
            data['IndexName'] = index_name

            data_beta = data.copy()
            data_beta.dropna(subset=['BENCHMARK_WEEKLY_PCT'], how='all', inplace=True)
            beta, alpha = np.polyfit(data_beta['BENCHMARK_WEEKLY_PCT'].astype(float).tolist(), data_beta['Fund_WEEKLY_PCT'].astype(float).tolist(), 1)
            annualized_alpha = (alpha + 1) ** freq_number - 1

            if dateStr in ['2019-03-29', '2019-06-28', '2019-09-30', '2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30', '2020-12-31']:
                current_year = record_date.year
                current_month = record_date.month
                previous_3m_month = current_month - 3
                end_month = current_month - 1
                previous_3m_data = data[(data['Date'].dt.year == current_year) & (data['Date'].dt.month >= previous_3m_month) & (data['Date'].dt.month <= end_month)]
                data_beta_pre_3M = data_beta[(data_beta['Date'].dt.year == current_year) & (data_beta['Date'].dt.month >= previous_3m_month) & (data_beta['Date'].dt.month <= current_month)]
                MaxDD_Fund_3M = -(previous_3m_data['Fund_DD'].max())
                MaxDD_Benchmark_3M = -(previous_3m_data['Benchmark_DD'].max())
                Benchmark_PCT_3M = data_beta_pre_3M['BENCHMARK_WEEKLY_PCT'].astype(float).tolist()
                Fund_PCT_3M = data_beta_pre_3M['Fund_WEEKLY_PCT'].astype(float).tolist()
                beta_3m, alpha_3m = np.polyfit(Benchmark_PCT_3M, Fund_PCT_3M, 1)

            floatCols = ['Fund_WEEKLY_PCT', 'Fund_AR',   'Fund_STD', 'Fund_DD',
                         'BenchmarkPrice', 'BENCHMARK_WEEKLY_PCT', 'BENCHMARK_STD', 'BENCHMARK_AR', 'Benchmark_DD','MaxDD_Fund','MaxDD_Benchmark']
            savableCols = ['Date', 'FundCode', 'NetUnit','IndexName'] + floatCols
            data = data.round(6)
            data = self.nanToNone(data, floatCols)
            data = data[data['Date'] == record_date]
            if not data.empty:
                sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(AsOfDate,FundCode,NetUnit,BenchmarkIndexName,Pct,CumReturn,Std,Drawdown,BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,MaxDrawdown,BenchmarkMaxDrawdown) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                if 'fund_std_3M' in locals():
                    data['Fund_Std_3M'] = fund_std_3M
                    data['Benchmark_Std_3M'] = benchmark_std_3M
                    data['MaxDD_Fund_3M'] = MaxDD_Fund_3M
                    data['MaxDD_Benchmark_3M'] = MaxDD_Benchmark_3M
                    savableCols += ['Fund_Std_3M', 'MaxDD_Fund_3M', 'Benchmark_Std_3M', 'MaxDD_Benchmark_3M']
                    sql = 'insert into RiskDb.risk.RiskFundBenchmarkReportExternal(AsOfDate,FundCode,NetUnit,BenchmarkIndexName,Pct,CumReturn,Std,Drawdown,BenchmarkPx,BenchmarkPct,BenchmarkStd,BenchmarkCumReturn,BenchmarkDrawdown,MaxDrawdown,BenchmarkMaxDrawdown, Std3M, MaxDrawdown3M, BenchmarkStd3M, BenchmarkMaxDrawdown3M) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

                records = pdUtil.dataFrameToSavableRecords(data, savableCols)
                self.removeRiskFundBenchmarkReportData(fundCode, index_name, dateStr)
                self.insertToDatabase(sql, records)

    def removeRiskFundBenchmarkReportData(self, fundCode, benchmark, dateStr):
        sql = 'delete from RiskDb.risk.RiskFundBenchmarkReportExternal where FundCode=\''+fundCode+'\' and BenchmarkIndexName=\''+benchmark+'\' and AsOfDate=\''+dateStr+'\''
        self.cursor.execute(sql)

    def removeRiskFundMonthlyReportData(self, fundCode, subFundCode, dateStr):
        sql = 'delete from RiskDb.risk.RiskFundMonthlyReportExternal where FundCode=\'' + fundCode + '\' and AsOfDate=\'' + dateStr + '\''
        if subFundCode != '':
            sql += ' and SubFundCode=\'' + subFundCode + '\''
        self.cursor.execute(sql)

    def insertIntoMonthlyReport(self, result):
        result['SubFundCode'] = 'Default'
        savableCols = ['Date', 'FundCode','SubFundCode', 'Fund_Annualized_Return',  'Fund_AR',
                       'Fund_Std', 'Sharpe', 'DD_Fund','MaxDD_Fund', 'Sortino_Ratio','NetValueType','FreqType',
                       'Fund_MTD', 'Fund_Return_3M',  'Fund_Return_1Y', 'Fund_Return_Current1Y','NetUnit','Fund_Return_Since_Open','VolCurrYear']
        records = pdUtil.dataFrameToSavableRecords(result, savableCols)
        sql = 'insert into RiskDb.risk.RiskFundMonthlyReportExternal(AsOfDate,FundCode,SubFundCode,AnnualizedReturn,CumReturn,Std,Sharpe,Drawdown,MaxDrawdown,SortinoRatio,NetValueType,FreqType,MTD,MTD_3M,YTD_Recent_1Y,YTD_Current_Year,NAV,YTD_Since_Open,Vol_Curreny_Year) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, records)

    def saveHistNav(self, records):
        sql = 'insert into RiskDb.risk.FundHistNav(AsOfDate,FundCode,SubFundCode,NetPrice) values(?,?,?,?)'
        self.insertToDatabase(sql, records)

    # def get_offshorefund_netprice(self, end_date, FundCode):
    #     sql = 'select fle.name, fle.short_name, n.GrossPrice, n.NetPrice from marking.Nav n ' \
    #           'join ( ' \
    #           'select * from dbo.FundLegalEntity ' \
    #           'where short_name like \''+FundCode+'%\' and organization_start_date = ' \
    #           '(select max(organization_start_date) from dbo.FundLegalEntity where organization_start_date <=  \''+end_date+'\' and short_name like \''+FundCode+'%\') ' \
    #           ')fle on n.Guid=fle.guid ' \
    #           'where NavSettlementId = ( ' \
    #           'select max(NavSettlementId) from marking.NavSettlement ' \
    #           'where FundCode=\''+FundCode+'\' ' \
    #           'and date = \''+end_date+'\' ' \
    #           ')'
    #     result = self.selectDataFromDb(sql)
    #     return result

    def get_offshorefund_netprice(self, start_date, end_date, fundCode):
        sql = 'select * from ( ' \
              '      select ROW_NUMBER() OVER(PARTITION by short_name,Date order by organization_start_date desc) row_no,' \
                    'organization_start_date, short_name, name,Guid,NavSettlementId, Date,GrossPrice, NetPrice,FundCode from ( ' \
                        'select ent.organization_start_date, ent.short_name, ent.name,n.Guid,n.NavSettlementId, n.Date,n.GrossPrice, n.NetPrice,n.FundCode from Partnership.marking.Nav n ' \
                        'left join Partnership.dbo.FundLegalEntity ent on ent.guid=n.Guid '  \
                        'where NavSettlementId in ( ' \
                            'select NavSettlementId from ' \
                                '(select ROW_NUMBER() OVER(PARTITION by date order by NavSettlementId desc) row_no , NavSettlementId, date,FundCode  from Partnership.marking.NavSettlement ' \
                                'where FundCode=\''+fundCode+'\' ' \
                                'and date between \''+start_date+'\' and \''+end_date+'\') raw_data ' \
                                'where raw_data.row_no=1 ' \
                            ') and ent.organization_start_date is not null ' \
                    ') raw_data1 ' \
                ') raw_data where raw_data.row_no=1 '
        result = self.selectDataFromDb(sql)
        return result

    def loadOffshoreFundNAVData(self, path, fundName, subFundName, end_date):
        db_cutoff_date = '2020-08-01'
        '''
        :param path:
        :param fundName:
        :return: return NAV data from excel
        IMPORTANT: only for Offshore funds [PMSF,PLUS,PCF]
        '''
        if fundName in ['PMSF','PLUS','PCF']:
            ####海外基金直接用结算团队提供的fund price计算相关数据  - IT marking未保存fund price数据，目前还是数据验证阶段，以后会替代。
            data = pd.read_excel(path+fundName+' '+subFundName+'.xlsx',sheet_name='Pinpoint',skiprows=[0,1])
            data.drop(data.columns.difference(['Date', 'NAV']), 1, inplace=True)
            data['Date'] = pd.to_datetime(data['Date'])
            data.index = data['Date']
            if fundName=='PLUS':
                start_date = pd.to_datetime(self.PLUS_START_DATE, format='%Y-%m-%d')
                data = data[data['Date'] >= start_date]

            '''Before 2020-09-01: excel'''
            data = data[data['Date'] < pd.to_datetime(db_cutoff_date, format='%Y-%m-%d')]
            '''After 2020-09-01: database'''
            data_from_db = self.get_offshorefund_netprice(db_cutoff_date, end_date, fundName)
            ##offshore_fund_list = ['PMSF_ClassA', 'PMSF_ClassB', 'PMSF_ClassD', 'PCF_ClassA', 'PCF_ClassB', 'PLUS_ClassA']
            if subFundName == 'ClassA':
                db_sub_class_name = fundName+'_A_NR'
            elif subFundName == 'ClassB':
                db_sub_class_name = fundName+'_B_NR'
            elif subFundName == 'ClassC':
                db_sub_class_name = fundName+'_C_NR'
            elif subFundName == 'ClassD':
                db_sub_class_name = fundName+'_D_NR'
            else:
                db_sub_class_name = fundName + 'UNKOWN'
            data_from_db = data_from_db[data_from_db['short_name']==db_sub_class_name]
            data_from_db['Date'] = pd.to_datetime(data_from_db['Date'])
            data_from_db['NAV'] =data_from_db['NetPrice'].astype(float)
            data_from_db.index = data_from_db['Date']
            data_from_db = data_from_db[['Date','NAV']]
            data = pd.concat([data, data_from_db], axis=0)
            data.dropna(subset=['NAV'], how='all', inplace=True)
            #data['MTD Return'] = data['MTD\nReturn']
            #data['AUM (mln)'] = data['AUM\n(mln)']
            #data['YTD Return'] = data['YTD\nReturn']
            data['NAV'] = data['NAV'].astype(float)
            # data['SubFundCode'] = subFundName
            # data['FundCode'] = fundName
            # records = pdUtil.dataFrameToSavableRecords(data, ['Date','FundCode','SubFundCode','NAV'])
            # self.saveHistNav(records)
            data.to_excel('c:\\temp\\'+db_sub_class_name+'.xlsx')
            return data
        else:
            raise Exception('not support for given fund:'+fundName)

    def getNAVYTDBase(self, date, data):
        data_copy = data.copy()
        year = date.year-1
        month = 12
        if data_copy[(data_copy['Date'].dt.year==year) & (data_copy['Date'].dt.month==month)].empty:
            return 100
        else:
            return data_copy[(data_copy['Date'].dt.year==year) & (data_copy['Date'].dt.month==month)]['NAV'].iloc[0]

    def calcOffshoreMothlyReportFactors(self, dateStr, data, fundName, classInfo, freq='monthly'):
        logging.info(dateStr+' '+fundName +' '+classInfo)
        record_date = pd.to_datetime(dateStr, format='%Y-%m-%d')
        data = data[data['Date'] <= record_date]
        if data.empty:
            return
        data['Fund']=fundName
        data['SubFundCode'] = classInfo
        if freq == 'daily':
            freq_number = 250  ##working day in a year
        elif freq == 'monthly':
            freq_number = 12
            data = self.getMonthEndDataOnly(data, fundName+classInfo)
        elif freq == 'weekly':
            freq_number = 52
            data = self.getWeekEndDataOnly(data, fundName+classInfo)
        data.sort_index(ascending=True, inplace=True)
        data['LastNAV'] = data['NAV'].shift(1)
        if fundName == 'PMSF' and classInfo in ['ClassB','ClassD']:
            pass
            #data['LastNAV'].iloc[0] = 264.81
        elif fundName == 'PMSF' and classInfo in ['ClassF']:
            pass
            #data['LastNAV'].iloc[0] = 268.16
        elif fundName == 'PCF' and classInfo in ['ClassB']:
            pass
            #data['LastNAV'].iloc[0] = 1388.56
        else:
            data['LastNAV'].iloc[0] = 100

        data['MTD Return'] = (data['NAV'] - data['LastNAV'])/data['LastNAV']
        data['MTD Return'] = data['MTD Return'].astype(float).round(4)
        data['NAV_YTD_BASE'] = data['Date'].apply(lambda x: self.getNAVYTDBase(x, data))
        data['YTD Return'] = (data['NAV'] / data['NAV_YTD_BASE']) - 1
        data.dropna(subset=['MTD Return'], how='all', inplace=True)
        data.index = np.arange(1, len(data) + 1)
        data['Accumulative Return'] = (1 + data['MTD Return']).astype(float).cumprod() - 1  # AR: accumulative return
        data['Index'] = data.index
        if fundName=='PMSF' and classInfo in ['ClassA']:
            pmsf_to_platform_date = pd.to_datetime('2014-05-30', format='%Y-%m-%d')
            data_since2014 = data[data['Date'] >= pmsf_to_platform_date].copy()
            data_since2014['LastNAV'] = data_since2014['NAV'].shift(1)
            data_since2014['MTD Return'] = (data_since2014['NAV'] - data_since2014['LastNAV']) / data_since2014['LastNAV']
            data_since2014['MTD Return'] = data_since2014['MTD Return'].astype(float).round(4)
            data_since2014['Accumulative Return'] = (1 + data_since2014['MTD Return']).astype(float).cumprod() - 1  # AR: accumulative return
            data_since2014.index = np.arange(0, len(data_since2014))
            data_since2014['Index'] = data_since2014.index
            data_since2014['Annualized Return'] = ((1 + data_since2014['Accumulative Return']).astype(float).pow(1/(data_since2014['Index'])).pow(12) - 1).astype(float).round(4)
            data_since2014['Std'] = data_since2014['MTD Return'].rolling(len(data_since2014), min_periods=2).std() * (12 ** 0.5) * 100
            data_since2014['Sharpe'] = data_since2014['Annualized Return'] / data_since2014['Std'] * 100
            data_since2014['MTD_MIN'] = np.where(data_since2014['MTD Return'] > 0, 0, data_since2014['MTD Return'])
            data_since2014['MTD_MIN_POWER_2'] = (data_since2014['MTD_MIN'].pow(2)).astype(float).round(4)
            data_since2014['Annualized_Downside_STD'] = ((data_since2014['MTD_MIN_POWER_2'].cumsum() / data_since2014.index * 12).astype(float) ** 0.5).astype(float).round(4)
            data_since2014['Sortino_Ratio'] = np.where(data_since2014['Annualized_Downside_STD'].astype(float) == 0.0, np.nan,
                                                       data_since2014['Annualized Return'] / data_since2014['Annualized_Downside_STD'])
        data['Annualized Return'] = ((1+data['Accumulative Return']).astype(float).pow(1/(data['Index'])).pow(12) - 1).astype(float).round(4)
        data['Last3Month NAV'] = data['NAV'].shift(3)
        data['Last3Month Return'] = data['NAV']/data['Last3Month NAV'] - 1
        data['Last12Month NAV'] = data['NAV'].shift(12)
        data['Last12Month Return'] = data['NAV']/data['Last12Month NAV'] - 1
        data['Last3Yrs NAV'] = data['NAV'].shift(36)
        data['Last3Yrs Return'] = data['NAV']/data['Last3Yrs NAV'] - 1
        data['test'] = data['MTD Return'].rolling(len(data), min_periods=2).std()
        data['Leng'] = len(data)
        data['Std'] = data['MTD Return'].rolling(len(data), min_periods=2).std() * (12 ** 0.5) * 100
        data['Std_12M'] = data['MTD Return'].rolling(12).std() * (12 ** 0.5) * 100
        data['Std_36M'] = data['MTD Return'].rolling(36).std() * (12 ** 0.5) * 100
        data['Sharpe'] = data['Annualized Return'] / data['Std'] * 100
        data['MTD_MIN'] = np.where(data['MTD Return'] > 0, 0, data['MTD Return'])
        data['MTD_MIN_POWER_2'] = (data['MTD_MIN'].pow(2)).astype(float).round(4)
        data['Annualized_Downside_STD'] = ((data['MTD_MIN_POWER_2'].cumsum()/data.index*12).astype(float) ** 0.5).astype(float).round(4)
        data['MTD_MIN_POWER_2_ROLLING36'] = data['MTD_MIN_POWER_2'].rolling(36).sum()
        data['Annualized_Downside_36M_STD'] = (data['MTD_MIN_POWER_2'].rolling(36).sum()/36*12) ** 0.5
        data['Sortino_Ratio'] = np.where(data['Annualized_Downside_STD'].astype(float)==0.0, np.nan, data['Annualized Return'] / data['Annualized_Downside_STD'])

        test_data =data.iloc[::-1].copy()
        test_data['NAV+1'] = test_data['NAV'].astype(float).shift(1)
        test_data['MIN_NAV'] =  test_data['NAV+1'].rolling(len(test_data), min_periods=1).min()
        data=test_data.iloc[::-1]
        data['MaxDrawdown'] = (1+data['MIN_NAV']).astype(float)/(1 + data['NAV']).astype(float) - 1
        floatCols= ['NAV','MTD Return','YTD Return','Accumulative Return', 'Annualized Return','Last3Month Return', 'Last12Month Return', 'Last3Yrs Return','Std','Std_12M' ,'Std_36M','Sharpe', 'Annualized_Downside_STD', 'Annualized_Downside_36M_STD', 'Sortino_Ratio', 'MaxDrawdown']
        savableCols = ['NAV', 'Date','Fund','SubFundCode','MTD Return','YTD Return','Accumulative Return', 'Annualized Return','Last3Month Return', 'Last12Month Return', 'Last3Yrs Return','Std','Std_12M' ,'Std_36M','Sharpe', 'Annualized_Downside_STD', 'Annualized_Downside_36M_STD', 'Sortino_Ratio', 'MaxDrawdown','FreqType','NetValueType']
        data = data.round(6)
        data = self.nanToNone(data,floatCols)
        data['FreqType'] = 12  ##Monthly
        data['NetValueType'] = RiskFundMonthlyReportExternalNetValueType.AFTER_FEE.value
        data = data[data['Date']==record_date]
        if fundName=='PMSF' and classInfo in ['ClassA']:
            data_since2014 = data_since2014[data_since2014['Date'] == record_date]
            data['Annualized Return'] = data_since2014['Annualized Return'].values
            data['Sharpe'] = data_since2014['Sharpe'].values
            data['Sortino_Ratio'] = data_since2014['Sortino_Ratio'].values
        records = pdUtil.dataFrameToSavableRecords(data, savableCols)
        if records:
            sql = 'insert into RiskDb.risk.RiskFundMonthlyReportExternal(NAV,AsOfDate,FundCode,SubFundCode,MTD,YTD,CumReturn,AnnualizedReturn,Last3MonthReturn,Last12MonthReturn,Last3YrsReturn,Std,Std12M,Std36M,Sharpe,AnnualizedDownsideStd,AnnualizedDownside36MStd,SortinoRatio,Drawdown,FreqType,NetValueType) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, records)
        else:
            logging.error(fundName+' '+classInfo+' '+dateStr+'is empty')
        return data

    def normalizeIndexPx(self, data):
        index_names = [benchmark_name for benchmark_name in data.columns.values if benchmark_name != 'Date']
        for index_name in index_names:
            data[index_name] = data[index_name].astype(float)
            firt_value = data[index_name].astype(float).iloc[0]
            data[index_name] = (data[index_name]/firt_value) * 100
        return data

    def getIndexBenchmarkPxData(self, fundName):
        '''
        read from excel for now, can be replaced from DB once we have enough data
        :param indexName: C:\devel\2019MDD\Fund\quarterly\offshore\index-data\  + fundName+'_Index.xlsx'
        :return:F
        '''

        if fundName=='PMSF':
            useCols = 'A,CW:DJ'
            cols = ['Date','EHFI20 Index','MXEF Index','MXAP Index','MXWD Index','MXWO Index','SPX Index','EHFI115 Index']
            skipRows = [0]
            dropRows = [0]  ##[0,1] rows
            dropnaCol = ['EHFI20 Index']
            index_start_date =pd.to_datetime(self.PMSF_INDEX_START_DATE)
        elif fundName=='PLUS':
            useCols = 'A,CV:DJ'
            cols = ['Date','HSCEI Index','MXCN Index','SHSZ300 Index','MXASJ Index','HSI Index','SHCOMP Index']
            skipRows = []
            dropRows = [1,2,3]
            dropnaCol = ['HSCEI Index']
            index_start_date =pd.to_datetime(self.PLUS_INDEX_START_DATE)
        elif fundName=='PCF':
            useCols = 'A,BZ:CN'
            cols = ['Date','HSCEI Index','MXCN Index','SHSZ300 Index','MXASJ Index','HSI Index']
            skipRows = []
            dropRows = [1,2,3]
            dropnaCol = ['HSCEI Index']
            index_start_date =pd.to_datetime(self.PCF_INDEX_START_DATE)
        data = pd.read_excel('C:\\devel\\2019MDD\\Fund\\quarterly\\offshore\\index-data\\' + fundName + '_Index.xlsx',
                             sheet_name=fundName + ' Stock Market Index', skiprows=skipRows, usecols=useCols)
        data = data.drop(dropRows)
        data = data[cols]
        data.dropna(subset=dropnaCol, how='all', inplace=True)
        data['Date'] = pd.to_datetime(data['Date'])
        data = data[data['Date'] >= index_start_date]
        data = self.normalizeIndexPx(data)
        data.index = pd.to_datetime(data['Date'])
        data.sort_index(ascending=True, inplace=True)
        return data

    def getLastDayOfCurrentMonth(self, runMonthStr):
        run_date = pd.to_datetime(runMonthStr, format='%Y-%m')
        run_year = run_date.year
        run_month = run_date.month
        day_range = calendar.monthrange(run_year, run_month)
        end = datetime.date(run_year, run_month, day_range[1])
        return end.strftime('%Y-%m-%d')

    def getLastBussinessOnCurrentMonth(self, runMonthStr):
        run_date = pd.to_datetime(runMonthStr, format='%Y-%m')
        run_year = run_date.year
        run_month = run_date.month
        day_range = calendar.monthrange(run_year, run_month)
        start = datetime.date(run_year, run_month, 1)
        end = datetime.date(run_year, run_month, day_range[1])
        #BM::business month end frequency
        business_days_rng = pd.date_range(start, end, freq='BM')
        db = pd.DataFrame()
        db['Date'] = business_days_rng
        return db['Date'][0].strftime('%Y-%m-%d')

    def addYTDBaseValue(self, fundName, data, mode='LastBussinessMode'):
        if fundName == 'PMSF':
            start_date = pd.to_datetime(self.PMSF_INDEX_START_DATE)
            start_year = start_date.year
        if fundName == 'PLUS':
            start_date = pd.to_datetime(self.PLUS_INDEX_START_DATE)
            start_year = start_date.year

        index_names = [benchmark_name for benchmark_name in data.columns.values if benchmark_name != 'Date']
        data['DateStr'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
        data['PreYear'] = data['Date'].dt.year-1
        data['PreYLastDate'] = data['PreYear'].astype(str) + '-12'
        if mode == 'LastBussinessMode':
            data['LastYEndDate'] = data['PreYLastDate'].apply(lambda x: self.getLastBussinessOnCurrentMonth(x))
        elif mode == 'LastDayMode':
            data['LastYEndDate'] = data['PreYLastDate'].apply(lambda x: self.getLastDayOfCurrentMonth(x))

        lastYEndDateList = list(data['LastYEndDate'].unique())
        last_year_end_data = data[data['DateStr'].isin(lastYEndDateList)].copy()
        last_year_end_data['LastYEndDate'] = last_year_end_data['DateStr']
        for index_name in index_names:
            last_year_end_data[index_name+'_YTD_BASE'] = last_year_end_data[index_name]
            data = pd.merge(data, last_year_end_data[['LastYEndDate', index_name+'_YTD_BASE']], how='left', on=['LastYEndDate'])
            data[index_name+'_YTD_BASE'] = np.where(data['Date'].dt.year == start_year, data[data['Date'] == start_date][index_name].iloc[0], data[index_name+'_YTD_BASE'])
        return data

    # def calcOffshoreBenchmarkFactors(self, dateStr, fundName, fund_data, mode):
    #     '''
    #     :param data:
    #     :return:
    #     PCF: [HSCEI Index, MXCN Index, SHSZ300 Index, MXASJ Index, HSI Index]
    #     PLUS: [HSCEI Index,Shanghai Composite Index CSI 300 Index,MXCN Index,SHSZ300 Index, HSI Index]
    #     PMSF:[Eurekahedge Asia Multi Strategy Hedge Fund Index - EHFI20 Index,
    #           Eurekahedge Greater China Long Short Equities Hedge Fund I - EHFI115 Index,
    #           MSCI AC Asia Pacific Index - MXAP Index,
    #           MSCI Emerging Marketing Index  - MXEF Index,
    #           MSCI World(Developed/Emerging - MXWD Index),
    #           MSCI World(Developed) - MXWO Index]
    #     '''
    #     record_date = pd.to_datetime(dateStr, format='%Y-%m-%d')
    #     current_year = record_date.year
    #     current_month = record_date.month
    #     previous_3m_month = current_month - 2
    #     previous_1y_date = record_date + relativedelta.relativedelta(years=-1)
    #
    #     open_date = fund_data['Date'].iloc[0]
    #     benchmark_data = self.getIndexBenchmarkPxData(fundName)
    #     #benchmark_data = pd.merge(fund_data[['Date']], benchmark_data, how='left',on=['Date'])
    #     benchmark_data = self.addYTDBaseValue(fundName, benchmark_data, mode)
    #     benchmark_data = benchmark_data[benchmark_data['Date']<=record_date]
    #     index_names = [benchmark_name for benchmark_name in benchmark_data.columns.values if benchmark_name != 'Date']
    #     for index_name in index_names:
    #         index_data = benchmark_data[['Date', index_name, index_name+'_YTD_BASE']].copy()
    #         index_data[index_name + '_LastMPx'] = index_data[index_name].shift(1)
    #         index_data[index_name + '_MTD'] = (index_data[index_name]-index_data[index_name+'_LastMPx'])/index_data[index_name+'_LastMPx']
    #         index_data.dropna(subset=[index_name + '_MTD'], how='all', inplace=True)
    #         index_data[index_name + '_YTD'] = (index_data[index_name]-index_data[index_name+'_YTD_BASE'])/index_data[index_name+'_YTD_BASE']
    #         index_data[index_name + '_Accumulative'] = (index_data[index_name]-100)/100
    #         index_data.index = np.arange(1, len(index_data)+1)
    #         index_data['Index'] = index_data.index
    #         index_data[index_name + '_Annualized'] = (1 + index_data[index_name + '_Accumulative']).astype(float).pow(1 / (index_data['Index'])).pow(12) - 1
    #         index_data['Std'] = index_data[index_name + '_MTD'].rolling(len(index_data), min_periods=2).std() * (12 ** 0.5) * 100
    #         index_data['Std_12M'] = index_data[index_name + '_MTD'].rolling(12).std() * (12 ** 0.5) * 100
    #         index_data['Std_36M'] = index_data[index_name + '_MTD'].rolling(36).std() * (12 ** 0.5) * 100
    #
    #         index_data['Sharpe'] = index_data[index_name + '_Annualized'] / data['Std']
    #         ''' 3m return'''
    #         previous_3m_data = index_data[(index_data['Date'].dt.year == current_year) & (index_data['Date'].dt.month >= previous_3m_month) & (index_data['Date'].dt.month <= current_month)].copy()
    #         previous_3m_data[index_name+'_MTD_3M'] = (1 + previous_3m_data[index_name+'_MTD']).astype(float).cumprod() - 1
    #         previous_3m_data = previous_3m_data[['Date', index_name+'_MTD_3M']]
    #
    #         ''' 12m return'''
    #         previous_12m_data = index_data[index_data['Date']>previous_1y_date].copy()
    #         previous_12m_data[index_name + '_MTD_12M'] = (1 + previous_12m_data[index_name + '_MTD']).astype(float).cumprod() - 1
    #         previous_12m_data = previous_12m_data[['Date', index_name + '_MTD_12M']]
    #     print 'test '

    def calcFactorsWithGivenDate(self, dateStr, fundName, subFundName):
        sql = 'select AsOfDate as Date, NAV,MTD as [MTD Return], AUM as [AUM (mln)], YTD as [YTD Return] from RiskDb.risk.RiskFundMonthlyData where FundCode = \''+fundName+'\' and SubFundCode=\'' + subFundName+'\' and AsOfDate<=\''+dateStr+'\' order by AsOfDate'
        data = self.selectDataFromDb(sql)
        result = self.calcMothlyReportFactors(data, fundName,subFundName)
        savableCols = ['NAV', 'Date', 'Fund', 'SubFundCode', 'MTD Return', 'AUM (mln)', 'YTD Return',
                       'Accumulative Return', 'Annualized Return', 'Last3Month Return', 'Last12Month Return',
                       'Last3Yrs Return', 'Std', 'Std_12M', 'Std_36M', 'Sharpe', 'Annualized_Downside_STD',
                       'Annualized_Downside_36M_STD', 'Sortino_Ratio', 'MaxDrawdown']
        records =[]
        records.append(tuple(result[savableCols].iloc[-1].values.tolist()))
        sql = 'insert into RiskDb.risk.RiskFundMonthlyReportExternal(NAV,AsOfDate,FundCode,SubFundCode,MTD,AUM,YTD,CumReturn,AnnualizedReturn,Last3MonthReturn,Last12MonthReturn,Last3YrsReturn,Std,Std12M,Std36M,Sharpe,AnnualizedDownsideStd,AnnualizedDownside36MStd,SortinoRatio,Drawdown) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, records)

    def nanToNone(self, df_data, columnList):
        for column in columnList:
            df_data[column] = np.where(df_data[column].isna(), None, df_data[column])
        return df_data

    def semiYearReport(self, fund, quarter):
        year = quarter.split('-')[0]
        quarter = quarter.split('-')[1]
        if quarter == 'Q1':
            exposure_LS_quarterly_1, exposure_category_quarterly_1,exposure_industry_quarterly_1 = self.quarterlyReportFromExcel(fund,year+'-Q1')
            exposure_LS_HalfY = exposure_LS_quarterly_1.copy()
            if exposure_category_quarterly_1 is not None:
                exposure_category_HalfY = exposure_category_quarterly_1.copy()
            else:
                exposure_category_HalfY = None
            if exposure_industry_quarterly_1 is not None:
                exposure_industry_HalfY = exposure_industry_quarterly_1.copy()
            else:
                exposure_industry_HalfY = None

        elif quarter == 'Q2':
            exposure_LS_quarterly_1, exposure_category_quarterly_1,exposure_industry_quarterly_1 = self.quarterlyReportFromExcel(fund,year+'-Q1')
            exposure_LS_quarterly_2, exposure_category_quarterly_2,exposure_industry_quarterly_2 = self.quarterlyReportFromExcel(fund,year+'-Q2')
            exposure_LS_HalfY = pd.concat([exposure_LS_quarterly_1, exposure_LS_quarterly_2], axis=0)

            if exposure_category_quarterly_1 is not None:
                exposure_category_HalfY = pd.concat([exposure_category_quarterly_1, exposure_category_quarterly_2], axis=0)
            else:
                exposure_category_HalfY = None

            if exposure_industry_quarterly_1 is not None:
                exposure_industry_HalfY = pd.concat([exposure_industry_quarterly_1, exposure_industry_quarterly_2], axis=0)
            else:
                exposure_industry_HalfY = None
        elif quarter =='Q3':
            exposure_LS_quarterly_1, exposure_category_quarterly_1,exposure_industry_quarterly_1 = self.quarterlyReportFromExcel(fund,year+'-Q1')
            exposure_LS_quarterly_2, exposure_category_quarterly_2,exposure_industry_quarterly_2 = self.quarterlyReportFromExcel(fund,year+'-Q2')
            exposure_LS_quarterly_3, exposure_category_quarterly_3,exposure_industry_quarterly_3 = self.quarterlyReportFromExcel(fund,year+'-Q3')
            exposure_LS_HalfY = pd.concat([exposure_LS_quarterly_1, exposure_LS_quarterly_2], axis=0)
            exposure_LS_HalfY = pd.concat([exposure_LS_HalfY, exposure_LS_quarterly_3], axis=0)

            if exposure_category_quarterly_1 is not None:
                exposure_category_HalfY = pd.concat([exposure_category_quarterly_1, exposure_category_quarterly_2], axis=0)
                exposure_category_HalfY = pd.concat([exposure_category_HalfY, exposure_category_quarterly_3], axis=0)
            else:
                exposure_category_HalfY = None

            if exposure_industry_quarterly_1 is not None:
                exposure_industry_HalfY = pd.concat([exposure_industry_quarterly_1, exposure_industry_quarterly_2], axis=0)
                exposure_industry_HalfY = pd.concat([exposure_industry_HalfY, exposure_industry_quarterly_3], axis=0)
            else:
                exposure_industry_HalfY = None

        elif quarter == 'Q4':
            exposure_LS_quarterly_1, exposure_category_quarterly_1,exposure_industry_quarterly_1 = self.quarterlyReportFromExcel(fund,year+'-Q1')
            exposure_LS_quarterly_2, exposure_category_quarterly_2,exposure_industry_quarterly_2 = self.quarterlyReportFromExcel(fund,year+'-Q2')
            exposure_LS_quarterly_3, exposure_category_quarterly_3,exposure_industry_quarterly_3 = self.quarterlyReportFromExcel(fund,year+'-Q3')
            exposure_LS_quarterly_4, exposure_category_quarterly_4,exposure_industry_quarterly_4 = self.quarterlyReportFromExcel(fund,year+'-Q4')

            exposure_LS_HalfY = pd.concat([exposure_LS_quarterly_1, exposure_LS_quarterly_2], axis=0)
            exposure_LS_HalfY = pd.concat([exposure_LS_HalfY, exposure_LS_quarterly_3], axis=0)
            exposure_LS_HalfY = pd.concat([exposure_LS_HalfY, exposure_LS_quarterly_4], axis=0)
            if exposure_category_quarterly_1 is not None:
                exposure_category_HalfY = pd.concat([exposure_category_quarterly_1, exposure_category_quarterly_2], axis=0)
                exposure_category_HalfY = pd.concat([exposure_category_HalfY, exposure_category_quarterly_3], axis=0)
                exposure_category_HalfY = pd.concat([exposure_category_HalfY, exposure_category_quarterly_4], axis=0)
            else:
                exposure_category_HalfY = None

            if exposure_industry_quarterly_1 is not None:
                exposure_industry_HalfY = pd.concat([exposure_industry_quarterly_1, exposure_industry_quarterly_2], axis=0)
                exposure_industry_HalfY = pd.concat([exposure_industry_HalfY, exposure_industry_quarterly_3], axis=0)
                exposure_industry_HalfY = pd.concat([exposure_industry_HalfY, exposure_industry_quarterly_4], axis=0)
            else:
                exposure_industry_HalfY = None

        exposure_LS_HalfY['Term'] = quarter
        exposure_LS_HalfY['YTD'] = exposure_LS_HalfY['LongPnL'] + exposure_LS_HalfY['ShortPnL']
        exposure_LS_HalfY['YTD_For_Calc'] = exposure_LS_HalfY['YTD'].shift(1)
        exposure_LS_HalfY['YTD_For_Calc'] = exposure_LS_HalfY['YTD_For_Calc'].fillna(0)
        exposure_LS_HalfY['YTD_For_Calc_2'] = exposure_LS_HalfY['YTD'].shift(2)
        exposure_LS_HalfY['YTD_For_Calc_2'] = exposure_LS_HalfY['YTD_For_Calc_2'].fillna(0)
        exposure_LS_HalfY['YTD_For_Calc_3'] = exposure_LS_HalfY['YTD'].shift(3)
        exposure_LS_HalfY['YTD_For_Calc_3'] = exposure_LS_HalfY['YTD_For_Calc_3'].fillna(0)
        exposure_LS_HalfY['YTD_For_Calc_4'] = exposure_LS_HalfY['YTD'].shift(4)
        exposure_LS_HalfY['YTD_For_Calc_4'] = exposure_LS_HalfY['YTD_For_Calc_4'].fillna(0)

        exposure_LS_HalfY['LongPnL'] = exposure_LS_HalfY['LongPnL'] * (1+exposure_LS_HalfY['YTD_For_Calc'])* (1+exposure_LS_HalfY['YTD_For_Calc_2'])* (1+exposure_LS_HalfY['YTD_For_Calc_3'])* (1+exposure_LS_HalfY['YTD_For_Calc_4'])
        exposure_LS_HalfY['ShortPnL'] = exposure_LS_HalfY['ShortPnL'] * (1+exposure_LS_HalfY['YTD_For_Calc'])* (1+exposure_LS_HalfY['YTD_For_Calc_2'])* (1+exposure_LS_HalfY['YTD_For_Calc_3'])* (1+exposure_LS_HalfY['YTD_For_Calc_4'])
        exposure_LS_HalfY_Result = exposure_LS_HalfY.groupby(['Term','Fund', 'Year']).agg({'LongPnL': 'sum', 'ShortPnL': 'sum'})
        exposure_LS_HalfY_Result = exposure_LS_HalfY_Result.reset_index()

        if exposure_category_HalfY is not None:
            if fund in ['ZJNF','SLHL','DCL','CVF']:
                exposure_category_HalfY['YTD'] = exposure_category_HalfY['CNY_ASSET']+exposure_category_HalfY['HK_ASSET']+exposure_category_HalfY['Other_ASSET'] + exposure_category_HalfY['US_ASSET']
            else:
                exposure_category_HalfY['YTD'] = exposure_category_HalfY['CNY_ASSET']+exposure_category_HalfY['OVERSEA_ASSET'] + exposure_category_HalfY['US_ASSET'] + exposure_category_HalfY['FX_ASSET']
            exposure_category_HalfY['YTD_For_Calc'] = exposure_category_HalfY['YTD'].shift(1)
            exposure_category_HalfY['YTD_For_Calc'] = exposure_category_HalfY['YTD_For_Calc'].fillna(0)
            exposure_category_HalfY['YTD_For_Calc_2'] = exposure_category_HalfY['YTD'].shift(2)
            exposure_category_HalfY['YTD_For_Calc_2'] = exposure_category_HalfY['YTD_For_Calc_2'].fillna(0)
            exposure_category_HalfY['YTD_For_Calc_3'] = exposure_category_HalfY['YTD'].shift(3)
            exposure_category_HalfY['YTD_For_Calc_3'] = exposure_category_HalfY['YTD_For_Calc_3'].fillna(0)
            exposure_category_HalfY['YTD_For_Calc_4'] = exposure_category_HalfY['YTD'].shift(4)
            exposure_category_HalfY['YTD_For_Calc_4'] = exposure_category_HalfY['YTD_For_Calc_4'].fillna(0)
            exposure_category_HalfY['CNY_ASSET'] = exposure_category_HalfY['CNY_ASSET']*(1+exposure_category_HalfY['YTD_For_Calc'])*(1+exposure_category_HalfY['YTD_For_Calc_2'])*(1+exposure_category_HalfY['YTD_For_Calc_3'])*(1+exposure_category_HalfY['YTD_For_Calc_4'])
            exposure_category_HalfY['OVERSEA_ASSET'] = exposure_category_HalfY['OVERSEA_ASSET']*(1+exposure_category_HalfY['YTD_For_Calc'])*(1+exposure_category_HalfY['YTD_For_Calc_2'])*(1+exposure_category_HalfY['YTD_For_Calc_3'])*(1+exposure_category_HalfY['YTD_For_Calc_4'])
            exposure_category_HalfY['FX_ASSET'] = exposure_category_HalfY['FX_ASSET']*(1+exposure_category_HalfY['YTD_For_Calc'])*(1+exposure_category_HalfY['YTD_For_Calc_2'])*(1+exposure_category_HalfY['YTD_For_Calc_3'])*(1+exposure_category_HalfY['YTD_For_Calc_4'])
            exposure_category_HalfY['HK_ASSET'] = exposure_category_HalfY['HK_ASSET']*(1+exposure_category_HalfY['YTD_For_Calc'])*(1+exposure_category_HalfY['YTD_For_Calc_2'])*(1+exposure_category_HalfY['YTD_For_Calc_3'])*(1+exposure_category_HalfY['YTD_For_Calc_4'])
            exposure_category_HalfY['US_ASSET'] = exposure_category_HalfY['US_ASSET']*(1+exposure_category_HalfY['YTD_For_Calc'])*(1+exposure_category_HalfY['YTD_For_Calc_2'])*(1+exposure_category_HalfY['YTD_For_Calc_3'])*(1+exposure_category_HalfY['YTD_For_Calc_4'])
            exposure_category_HalfY['Other_ASSET'] = exposure_category_HalfY['Other_ASSET']*(1+exposure_category_HalfY['YTD_For_Calc'])*(1+exposure_category_HalfY['YTD_For_Calc_2'])*(1+exposure_category_HalfY['YTD_For_Calc_3'])*(1+exposure_category_HalfY['YTD_For_Calc_4'])
            exposure_category_HalfY['Term'] = quarter
            exposure_category_HalfY_Result = exposure_category_HalfY.groupby(['Term', 'Fund', 'Year']).agg({'CNY_ASSET': 'sum', 'OVERSEA_ASSET': 'sum', 'FX_ASSET': 'sum','HK_ASSET':'sum','Other_ASSET':'sum','US_ASSET':'sum'})
            exposure_category_HalfY_Result = exposure_category_HalfY_Result.reset_index()
        else:
            exposure_category_HalfY_Result = None


        if exposure_industry_HalfY is not None:
            exposure_industry_HalfY['YTD'] = exposure_industry_HalfY['ConsumerSector_ASSET']+exposure_industry_HalfY['FinancialRealEstateSector_ASSET']+ \
                                             exposure_industry_HalfY['CyclicalSector_ASSET'] + exposure_industry_HalfY['MedicalAndGreenEnergySector_ASSET']+ \
                                             exposure_industry_HalfY['TMTSector_ASSET'] + exposure_industry_HalfY['OtherSectors_ASSET']+ \
                                             exposure_industry_HalfY['BondSector_ASSET'] + exposure_industry_HalfY['MacroSector_ASSET']+ \
                                             exposure_industry_HalfY['QuantSector_ASSET']
            exposure_industry_HalfY['YTD_For_Calc'] = exposure_industry_HalfY['YTD'].shift(1)
            exposure_industry_HalfY['YTD_For_Calc'] = exposure_industry_HalfY['YTD_For_Calc'].fillna(0)
            exposure_industry_HalfY['YTD_For_Calc_2'] = exposure_industry_HalfY['YTD'].shift(2)
            exposure_industry_HalfY['YTD_For_Calc_2'] = exposure_industry_HalfY['YTD_For_Calc_2'].fillna(0)
            exposure_industry_HalfY['YTD_For_Calc_3'] = exposure_industry_HalfY['YTD'].shift(3)
            exposure_industry_HalfY['YTD_For_Calc_3'] = exposure_industry_HalfY['YTD_For_Calc_3'].fillna(0)
            exposure_industry_HalfY['YTD_For_Calc_4'] = exposure_industry_HalfY['YTD'].shift(4)
            exposure_industry_HalfY['YTD_For_Calc_4'] = exposure_industry_HalfY['YTD_For_Calc_4'].fillna(0)
            exposure_industry_HalfY['ConsumerSector_ASSET'] = exposure_industry_HalfY['ConsumerSector_ASSET']*(1+exposure_industry_HalfY['YTD_For_Calc'])*(1+exposure_industry_HalfY['YTD_For_Calc_2'])*(1+exposure_industry_HalfY['YTD_For_Calc_3'])*(1+exposure_industry_HalfY['YTD_For_Calc_4'])
            exposure_industry_HalfY['FinancialRealEstateSector_ASSET'] = exposure_industry_HalfY['FinancialRealEstateSector_ASSET']*(1+exposure_industry_HalfY['YTD_For_Calc'])*(1+exposure_industry_HalfY['YTD_For_Calc_2'])*(1+exposure_industry_HalfY['YTD_For_Calc_3'])*(1+exposure_industry_HalfY['YTD_For_Calc_4'])
            exposure_industry_HalfY['CyclicalSector_ASSET'] = exposure_industry_HalfY['CyclicalSector_ASSET']*(1+exposure_industry_HalfY['YTD_For_Calc'])*(1+exposure_industry_HalfY['YTD_For_Calc_2'])*(1+exposure_industry_HalfY['YTD_For_Calc_3'])*(1+exposure_industry_HalfY['YTD_For_Calc_4'])
            exposure_industry_HalfY['MedicalAndGreenEnergySector_ASSET'] = exposure_industry_HalfY['MedicalAndGreenEnergySector_ASSET']*(1+exposure_industry_HalfY['YTD_For_Calc'])*(1+exposure_industry_HalfY['YTD_For_Calc_2'])*(1+exposure_industry_HalfY['YTD_For_Calc_3'])*(1+exposure_industry_HalfY['YTD_For_Calc_4'])
            exposure_industry_HalfY['TMTSector_ASSET'] = exposure_industry_HalfY['TMTSector_ASSET']*(1+exposure_industry_HalfY['YTD_For_Calc'])*(1+exposure_industry_HalfY['YTD_For_Calc_2'])*(1+exposure_industry_HalfY['YTD_For_Calc_3'])*(1+exposure_industry_HalfY['YTD_For_Calc_4'])
            exposure_industry_HalfY['OtherSectors_ASSET'] = exposure_industry_HalfY['OtherSectors_ASSET']*(1+exposure_industry_HalfY['YTD_For_Calc'])*(1+exposure_industry_HalfY['YTD_For_Calc_2'])*(1+exposure_industry_HalfY['YTD_For_Calc_3'])*(1+exposure_industry_HalfY['YTD_For_Calc_4'])
            exposure_industry_HalfY['BondSector_ASSET'] = exposure_industry_HalfY['BondSector_ASSET']*(1+exposure_industry_HalfY['YTD_For_Calc'])*(1+exposure_industry_HalfY['YTD_For_Calc_2'])*(1+exposure_industry_HalfY['YTD_For_Calc_3'])*(1+exposure_industry_HalfY['YTD_For_Calc_4'])
            exposure_industry_HalfY['MacroSector_ASSET'] = exposure_industry_HalfY['MacroSector_ASSET']*(1+exposure_industry_HalfY['YTD_For_Calc'])*(1+exposure_industry_HalfY['YTD_For_Calc_2'])*(1+exposure_industry_HalfY['YTD_For_Calc_3'])*(1+exposure_industry_HalfY['YTD_For_Calc_4'])
            exposure_industry_HalfY['QuantSector_ASSET'] = exposure_industry_HalfY['QuantSector_ASSET']*(1+exposure_industry_HalfY['YTD_For_Calc'])*(1+exposure_industry_HalfY['YTD_For_Calc_2'])*(1+exposure_industry_HalfY['YTD_For_Calc_3'])*(1+exposure_industry_HalfY['YTD_For_Calc_4'])
            exposure_industry_HalfY['Term'] = quarter
            exposure_industry_HalfY_Result = exposure_industry_HalfY.groupby(['Term', 'Fund', 'Year']).agg({'ConsumerSector_ASSET': 'sum', 'FinancialRealEstateSector_ASSET': 'sum', 'CyclicalSector_ASSET': 'sum',
                                                                                                            'MedicalAndGreenEnergySector_ASSET':'sum','TMTSector_ASSET':'sum','OtherSectors_ASSET':'sum',
                                                                                                            'BondSector_ASSET':'sum','MacroSector_ASSET':'sum','QuantSector_ASSET':'sum'})
            exposure_industry_HalfY_Result = exposure_industry_HalfY_Result.reset_index()
        else:
            exposure_industry_HalfY_Result = None


        return exposure_LS_HalfY_Result,exposure_category_HalfY_Result,exposure_industry_HalfY_Result

    def quarterlyReportFromExcelOfffshore(self, fund, yearAndQuarter):
        year = yearAndQuarter.split('-')[0]
        quarter = yearAndQuarter.split('-')[1]
        files = self.get_offshore_file_name(fund, quarter, year)

        eq_net_gross_total = pd.DataFrame(columns=['Quarter','Fund','Month','Year','Gross','Net','Long','Short'])
        market_cap_last_month = pd.DataFrame(columns=['Quarter','Fund','Month','Year','Market Cap','Long','Short'])
        exposure_by_area_total = pd.DataFrame(columns=['Quarter','Fund','Month','Year','Area','LongExp','ShortExp','NetExp','LongPnL','ShortPnL','NetPnL'])
        exposure_by_strat_total = pd.DataFrame(columns=['Quarter','Fund','Month','Year','Strat','LongPnL','ShortPnL','NetPnL','TotalNetPnL','TotalLongPnL','TotalShortPnL'])
        for file_name in files:
            skip_rows_for_area = 4
            skip_rows_for_marketcap=15
            skip_rows_for_strat = 25
            area_exposure_data, strat_exposure_data, market_cap_data = self.extract_offshore_data(fund, year, yearAndQuarter, file_name, skip_rows_for_area, skip_rows_for_strat)
            if area_exposure_data.empty:
                logging.error(fund+' '+yearAndQuarter+' data is empty, pls check!')
                break
            eq_net_gross_total = pd.concat([eq_net_gross_total, area_exposure_data[area_exposure_data['Georgaphic (Equity)']=='Equity Total'][['Quarter','Fund','Month','Year','Area','Long','Short','Net','Gross']]],axis=0,sort=True)
            exposure_by_strat_total = pd.concat([exposure_by_strat_total, strat_exposure_data[['Quarter','Fund','Month','Year','Strat','LongPnL','ShortPnL','NetPnL','TotalNetPnL','TotalLongPnL','TotalShortPnL']]],axis=0,sort=True)
            if quarter=='Q1':
                last_month='03'
            elif quarter=='Q2':
                last_month='06'
            elif quarter=='Q3':
                last_month='09'
            elif quarter=='Q4':
                last_month='12'
            market_cap_last_month = market_cap_data[market_cap_data['Month']==last_month][['Quarter','Fund','Month','Year','Market Cap','Long','Short']]

        market_cap_last_month_long = market_cap_last_month[['Market Cap', 'Long' ]]
        market_cap_last_month_long =market_cap_last_month_long[(market_cap_last_month_long['Market Cap'] != 'Equity Total') & (market_cap_last_month_long['Market Cap'] != 'Index')]
        long_total = market_cap_last_month_long['Long'].astype(float).sum()
        market_cap_last_month_long['Proportion'] = market_cap_last_month['Long'] / long_total
        market_cap_last_month_short = market_cap_last_month[['Market Cap', 'Short']]
        market_cap_last_month_short =market_cap_last_month_short[(market_cap_last_month_short['Market Cap'] != 'Equity Total') & (market_cap_last_month_short['Market Cap'] != 'Index')]
        short_total = market_cap_last_month_short['Short'].astype(float).sum()
        market_cap_last_month_short['Proportion'] = market_cap_last_month_short['Short'] / short_total



        strat_list = list(exposure_by_strat_total['Strat'].unique())
        total_ytd_strat = pd.DataFrame(columns=['Fund','Quarter','Strat','Year','YTDNetPnL'])
        total_quarter_strat = pd.DataFrame(columns=['Fund','Strat','Year','Q1NetPnL','Q2NetPnL','Q3NetPnL','Q4NetPnL','YTDNetPnL'])
        for strat in strat_list:
            single_strat_data = exposure_by_strat_total[exposure_by_strat_total['Strat']==strat].copy()
            single_strat_ytd_data = single_strat_data.copy()
            single_strat_ytd_data.sort_values('Month', ascending=True, inplace=True)
            single_strat_ytd_data['YTDNetPnL_1'] = single_strat_ytd_data['TotalNetPnL'].shift(1)
            single_strat_ytd_data['YTDNetPnL_1'] = single_strat_ytd_data['YTDNetPnL_1'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_2'] = single_strat_ytd_data['TotalNetPnL'].shift(2)
            single_strat_ytd_data['YTDNetPnL_2'] = single_strat_ytd_data['YTDNetPnL_2'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_3'] = single_strat_ytd_data['TotalNetPnL'].shift(3)
            single_strat_ytd_data['YTDNetPnL_3'] = single_strat_ytd_data['YTDNetPnL_3'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_4'] = single_strat_ytd_data['TotalNetPnL'].shift(4)
            single_strat_ytd_data['YTDNetPnL_4'] = single_strat_ytd_data['YTDNetPnL_4'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_5'] = single_strat_ytd_data['TotalNetPnL'].shift(5)
            single_strat_ytd_data['YTDNetPnL_5'] = single_strat_ytd_data['YTDNetPnL_5'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_6'] = single_strat_ytd_data['TotalNetPnL'].shift(6)
            single_strat_ytd_data['YTDNetPnL_6'] = single_strat_ytd_data['YTDNetPnL_6'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_7'] = single_strat_ytd_data['TotalNetPnL'].shift(7)
            single_strat_ytd_data['YTDNetPnL_7'] = single_strat_ytd_data['YTDNetPnL_7'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_8'] = single_strat_ytd_data['TotalNetPnL'].shift(8)
            single_strat_ytd_data['YTDNetPnL_8'] = single_strat_ytd_data['YTDNetPnL_8'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_9'] = single_strat_ytd_data['TotalNetPnL'].shift(9)
            single_strat_ytd_data['YTDNetPnL_9'] = single_strat_ytd_data['YTDNetPnL_9'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_10'] = single_strat_ytd_data['TotalNetPnL'].shift(10)
            single_strat_ytd_data['YTDNetPnL_10'] = single_strat_ytd_data['YTDNetPnL_10'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_11'] = single_strat_ytd_data['TotalNetPnL'].shift(11)
            single_strat_ytd_data['YTDNetPnL_11'] = single_strat_ytd_data['YTDNetPnL_11'].fillna(0)
            single_strat_ytd_data['YTDNetPnL_12'] = single_strat_ytd_data['TotalNetPnL'].shift(12)
            single_strat_ytd_data['YTDNetPnL_12'] = single_strat_ytd_data['YTDNetPnL_12'].fillna(0)
            single_strat_ytd_data['YTDNetPnL'] = single_strat_ytd_data['NetPnL'] \
                                             * (1 + single_strat_ytd_data['YTDNetPnL_1']) * (1 + single_strat_ytd_data['YTDNetPnL_2']) * (1 + single_strat_ytd_data['YTDNetPnL_3']) \
                                             * (1 + single_strat_ytd_data['YTDNetPnL_4']) * (1 + single_strat_ytd_data['YTDNetPnL_5']) * (1 + single_strat_ytd_data['YTDNetPnL_6']) \
                                             * (1 + single_strat_ytd_data['YTDNetPnL_7']) * (1 + single_strat_ytd_data['YTDNetPnL_8']) * (1 + single_strat_ytd_data['YTDNetPnL_9']) \
                                             * (1 + single_strat_ytd_data['YTDNetPnL_10']) * (1 + single_strat_ytd_data['YTDNetPnL_11']) * (1 + single_strat_ytd_data['YTDNetPnL_12'])
            total_ytd_single_strat = single_strat_ytd_data.groupby(['Fund','Quarter','Strat','Year']).agg({'YTDNetPnL':'sum'})
            total_ytd_single_strat = total_ytd_single_strat.reset_index()
            total_ytd_strat = pd.concat([total_ytd_strat, total_ytd_single_strat], axis=0)

            single_strat_data = single_strat_data.copy()
            if quarter=='Q1':
                month_lists={'Q1':['01','02','03']}
            elif quarter=='Q2':
                month_lists={'Q1':['01','02','03'], 'Q2':['04','05','06']}
            elif quarter=='Q3':
                month_lists={'Q1':['01','02','03'], 'Q2':['04','05','06'],'Q3':['07','08','09']}
            elif quarter=='Q4':
                month_lists={'Q1':['01','02','03'], 'Q2':['04','05','06'],'Q3':['07','08','09'], 'Q4':['10', '11','12']}
            total_quarter_single_strat = pd.DataFrame()
            for quarter_str, month_list in month_lists.items():
                single_strat_quarter_data = single_strat_data[single_strat_data['Month'].isin(month_list)].copy()
                single_strat_quarter_data.sort_values('Month', ascending=True, inplace=True)
                single_strat_quarter_data['QuarterNetPnL_1'] = single_strat_quarter_data['TotalNetPnL'].shift(1)
                single_strat_quarter_data['QuarterNetPnL_1'] = single_strat_quarter_data['QuarterNetPnL_1'].fillna(0)
                single_strat_quarter_data['QuarterNetPnL_2'] = single_strat_quarter_data['TotalNetPnL'].shift(2)
                single_strat_quarter_data['QuarterNetPnL_2'] = single_strat_quarter_data['QuarterNetPnL_2'].fillna(0)
                single_strat_quarter_data['QuarterNetPnL_3'] = single_strat_quarter_data['TotalNetPnL'].shift(3)
                single_strat_quarter_data['QuarterNetPnL_3'] = single_strat_quarter_data['QuarterNetPnL_3'].fillna(0)
                single_strat_quarter_data[quarter_str+'NetPnL'] = single_strat_quarter_data['NetPnL'] * (1 + single_strat_quarter_data['QuarterNetPnL_1']) \
                                                             * (1 + single_strat_quarter_data['QuarterNetPnL_2'])  * (1 + single_strat_quarter_data['QuarterNetPnL_3'])

                quarter_single_strat = single_strat_quarter_data.groupby(['Fund', 'Quarter', 'Strat', 'Year']).agg({quarter_str + 'NetPnL': 'sum'})
                quarter_single_strat = quarter_single_strat.reset_index()
                if total_quarter_single_strat.empty:
                    total_quarter_single_strat = quarter_single_strat
                else:
                    total_quarter_single_strat = pd.merge(total_quarter_single_strat, quarter_single_strat[['Fund','Year',quarter_str+'NetPnL']], how='left', on=['Fund','Year'])
            total_quarter_single_strat = pd.merge(total_quarter_single_strat, total_ytd_strat[['Fund','Strat','Year','YTDNetPnL']], how='left', on=['Fund','Strat','Year'])
            total_quarter_strat = pd.concat([total_quarter_strat, total_quarter_single_strat], axis=0)

        # total_strat = pd.merge(total_quarter_strat, total_ytd_strat, how='left', on=['Fund', 'Quarter', 'Strat','Year'])

        sector_pnl_quarterly = exposure_by_strat_total.groupby(['Quarter', 'Fund', 'Strat']).agg({'NetPnL': 'sum'})
        sector_pnl_quarterly = sector_pnl_quarterly.reset_index()

        eq_net_gross_total['Date'] = eq_net_gross_total['Year'] +eq_net_gross_total['Month']
        eq_net_gross_total['Date'] = pd.to_datetime(eq_net_gross_total['Date'], format='%Y%m')
        return total_quarter_strat[['Year','Fund','Quarter','Strat','Q1NetPnL','Q2NetPnL','Q3NetPnL','Q4NetPnL','YTDNetPnL']], sector_pnl_quarterly, market_cap_last_month_long, market_cap_last_month_short, eq_net_gross_total[['Date','Gross','Net','Long','Short']]


    def extract_offshore_liquidity_data(self, transparency_file):
        liquidity_data = pd.read_excel('C:\\temp\\quarterly_data\\offshore\\'+transparency_file, sheet_name='Data')
        liquidity_data['LiquidityCategory'] = np.where(liquidity_data['Liquidity']==1,'< 2 days',
                                                       np.where(liquidity_data['Liquidity']==2, '2 days to 1 week',
                                                                np.where((liquidity_data['Liquidity']==3) | (liquidity_data['Liquidity']==4), '1 week to 1 month',
                                                                         np.where(liquidity_data['Liquidity']==5, '>1 month','Other'))))
        liquidity_data = liquidity_data[liquidity_data['LiquidityCategory']!='Other']
        liquidity_data['Market Value Total'] = liquidity_data['Market Value Total'].abs()
        liquidity_data = liquidity_data.groupby(['LiquidityCategory', 'LSF (mtd) (USD)']).agg({'Market Value Total': 'sum','Liquidity':'max'})
        liquidity_data = liquidity_data.reset_index()
        liquidity_data.sort_values('Liquidity', ascending=True, inplace=True)
        return liquidity_data

    def extract_performance_data(self, fund, year, alpha_outperform_file):
        content = xlrd.open_workbook(filename='C:\\temp\\quarterly_data\\offshore\\'+alpha_outperform_file,encoding_override='gb2312')
        if fund == 'PMSF':
            performance_area_useCols = 'A,O'
            performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', usecols = performance_area_useCols)
            performance_data.dropna(subset=[fund+' monthly return'], how='all', inplace=True)
            performance_data['Date'] = pd.to_datetime(performance_data.index)

            hscei_area_useCols = 'A, DV'
            skip_rows_area = 1
            hscei_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = hscei_area_useCols)
            hscei_performance_data.dropna(subset=['HSCEI MTD'], how='all', inplace=True)
            hscei_performance_data['Date'] = pd.to_datetime(hscei_performance_data['Date'])

            MXCN_index_cols = 'A,AU'
            mxcn_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = MXCN_index_cols)
            mxcn_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            mxcn_performance_data['MXCN_MTD'] = mxcn_performance_data['MTD']
            mxcn_performance_data['Date'] = pd.to_datetime(mxcn_performance_data['Date'])
            del mxcn_performance_data['MTD']

            Eurekahedge_index_cols = 'A,AF'
            Eurekahedge_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = Eurekahedge_index_cols)
            Eurekahedge_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            Eurekahedge_performance_data['Eurekahedge_MTD'] = Eurekahedge_performance_data['MTD']
            Eurekahedge_performance_data['Date'] = pd.to_datetime(Eurekahedge_performance_data['Date'])
            del Eurekahedge_performance_data['MTD']

            MSCI_Emerging_index_cols = 'A,BY'
            MSCI_Emerging_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = MSCI_Emerging_index_cols)
            MSCI_Emerging_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            MSCI_Emerging_performance_data['MSCI_EMERGING_MTD'] = MSCI_Emerging_performance_data['MTD']
            MSCI_Emerging_performance_data['Date'] = pd.to_datetime(MSCI_Emerging_performance_data['Date'])
            del MSCI_Emerging_performance_data['MTD']

            MSCI_AC_index_cols = 'A,BJ'
            MSCI_AC_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = MSCI_AC_index_cols)
            MSCI_AC_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            MSCI_AC_performance_data['MSCI_AC_MTD'] = MSCI_AC_performance_data['MTD']
            MSCI_AC_performance_data['Date'] = pd.to_datetime(MSCI_AC_performance_data['Date'])
            del MSCI_AC_performance_data['MTD']

            performance_data = pd.merge(performance_data, hscei_performance_data, how='left', on=['Date'])
            performance_data = pd.merge(performance_data,mxcn_performance_data, how='left', on=['Date'])
            performance_data = pd.merge(performance_data,Eurekahedge_performance_data, how='left', on=['Date'])
            performance_data = pd.merge(performance_data,MSCI_Emerging_performance_data, how='left', on=['Date'])
            performance_data = pd.merge(performance_data,MSCI_AC_performance_data, how='left', on=['Date'])
            performance_data = performance_data[performance_data['Date'].dt.year == np.int64(year)]
            performance_data = performance_data[['Date','PMSF monthly return','HSCEI MTD','MXCN_MTD','Eurekahedge_MTD','MSCI_EMERGING_MTD','MSCI_AC_MTD']]
        elif fund=='PCF':
            performance_area_useCols = 'A,M'
            performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', usecols = performance_area_useCols)
            performance_data.dropna(subset=[fund+' monthly return'], how='all', inplace=True)
            performance_data['Date'] = pd.to_datetime(performance_data.index)

            skip_rows_area = 1

            shcomp_area_useCols = 'A, AH'
            shcomp_index_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = shcomp_area_useCols)
            shcomp_index_performance_data.dropna(subset=['SHCOMP MTD'], how='all', inplace=True)
            shcomp_index_performance_data['Date'] = pd.to_datetime(shcomp_index_performance_data['Date'])
            performance_data = pd.merge(performance_data, shcomp_index_performance_data, how='left', on=['Date'])

            hscei_area_useCols = 'A, AP'
            hscei_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = hscei_area_useCols)
            hscei_performance_data.dropna(subset=['HSCEI MTD'], how='all', inplace=True)
            hscei_performance_data['Date'] = pd.to_datetime(hscei_performance_data['Date'])
            performance_data = pd.merge(performance_data, hscei_performance_data, how='left', on=['Date'])

            shsz300_area_useCols = 'A, AX'
            shsz300_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = shsz300_area_useCols)
            shsz300_performance_data.dropna(subset=['SHSZ300 MTD'], how='all', inplace=True)
            shsz300_performance_data['Date'] = pd.to_datetime(shsz300_performance_data['Date'])
            performance_data = pd.merge(performance_data, shsz300_performance_data, how='left', on=['Date'])

            hsi_area_useCols = 'A, BE'
            hsi_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = hsi_area_useCols)
            hsi_performance_data.dropna(subset=['HSI MTD'], how='all', inplace=True)
            hsi_performance_data['Date'] = pd.to_datetime(hsi_performance_data['Date'])
            performance_data = pd.merge(performance_data, hsi_performance_data, how='left', on=['Date'])

            MXCN_index_cols = 'A,P'
            mxcn_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = MXCN_index_cols)
            mxcn_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            mxcn_performance_data['MXCN_MTD'] = mxcn_performance_data['MTD']
            mxcn_performance_data['Date'] = pd.to_datetime(mxcn_performance_data['Date'])
            del mxcn_performance_data['MTD']
            performance_data = pd.merge(performance_data, mxcn_performance_data, how='left', on=['Date'])

            Eurekahedge_index_cols = 'A,BL'
            Eurekahedge_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = Eurekahedge_index_cols)
            Eurekahedge_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            Eurekahedge_performance_data['Eurekahedge_MTD'] = Eurekahedge_performance_data['MTD']
            Eurekahedge_performance_data['Date'] = pd.to_datetime(Eurekahedge_performance_data['Date'])
            del Eurekahedge_performance_data['MTD']
            performance_data = pd.merge(performance_data, Eurekahedge_performance_data, how='left', on=['Date'])

            MSCI_AEJ_index_cols = 'A,BD'
            MSCI_AEJ_index_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = MSCI_AEJ_index_cols)
            MSCI_AEJ_index_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            MSCI_AEJ_index_performance_data['MSCI_AEJ_MTD'] = MSCI_AEJ_index_performance_data['MTD']
            MSCI_AEJ_index_performance_data['Date'] = pd.to_datetime(MSCI_AEJ_index_performance_data['Date'])
            del MSCI_AEJ_index_performance_data['MTD']
            performance_data = pd.merge(performance_data, MSCI_AEJ_index_performance_data, how='left', on=['Date'])

            performance_data = performance_data[performance_data['Date'].dt.year == np.int64(year)]
            #performance_data = performance_data[['Date','PMSF monthly return','HSCEI MTD','MXCN_MTD','Eurekahedge_MTD','MSCI_EMERGING_MTD','MSCI_AC_MTD']]
        elif fund=='PLUS':
            performance_area_useCols = 'A,M'
            performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', usecols = performance_area_useCols)
            performance_data.dropna(subset=[fund+' monthly return'], how='all', inplace=True)
            performance_data['Date'] = pd.to_datetime(performance_data.index)

            skip_rows_area = 2

            MXCN_index_cols = 'A,P'
            mxcn_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = MXCN_index_cols)
            mxcn_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            mxcn_performance_data['MXCN_MTD'] = mxcn_performance_data['MTD']
            mxcn_performance_data['Date'] = pd.to_datetime(mxcn_performance_data['Date'])
            del mxcn_performance_data['MTD']
            performance_data = pd.merge(performance_data, mxcn_performance_data, how='left', on=['Date'])

            shcomp_area_useCols = 'A, AH'
            shcomp_index_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = shcomp_area_useCols)
            shcomp_index_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            shcomp_index_performance_data['SHCOMP_MTD'] = shcomp_index_performance_data['MTD']
            shcomp_index_performance_data['Date'] = pd.to_datetime(shcomp_index_performance_data['Date'])
            del shcomp_index_performance_data['MTD']
            performance_data = pd.merge(performance_data, shcomp_index_performance_data, how='left', on=['Date'])

            hscei_area_useCols = 'A, AP'
            hscei_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = hscei_area_useCols)
            hscei_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            hscei_performance_data['HSCEI_MTD'] = hscei_performance_data['MTD']
            hscei_performance_data['Date'] = pd.to_datetime(hscei_performance_data['Date'])
            del hscei_performance_data['MTD']
            performance_data = pd.merge(performance_data, hscei_performance_data, how='left', on=['Date'])

            shsz300_area_useCols = 'A, BH'
            shsz300_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = shsz300_area_useCols)
            shsz300_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            shsz300_performance_data['SHSZ300_MTD'] = shsz300_performance_data['MTD']
            shsz300_performance_data['Date'] = pd.to_datetime(shsz300_performance_data['Date'])
            del shsz300_performance_data['MTD']
            performance_data = pd.merge(performance_data, shsz300_performance_data, how='left', on=['Date'])

            MSCI_AEJ_index_cols = 'A,BZ'
            MSCI_AEJ_index_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = MSCI_AEJ_index_cols)
            MSCI_AEJ_index_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            MSCI_AEJ_index_performance_data['MSCI_AEJ_MTD'] = MSCI_AEJ_index_performance_data['MTD']
            MSCI_AEJ_index_performance_data['Date'] = pd.to_datetime(MSCI_AEJ_index_performance_data['Date'])
            del MSCI_AEJ_index_performance_data['MTD']
            performance_data = pd.merge(performance_data, MSCI_AEJ_index_performance_data, how='left', on=['Date'])

            hsi_area_useCols = 'A, CA'
            hsi_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = hsi_area_useCols)
            hsi_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            hsi_performance_data['HSI_MTD'] = hsi_performance_data['MTD']
            hsi_performance_data['Date'] = pd.to_datetime(hsi_performance_data['Date'])
            del hsi_performance_data['MTD']
            performance_data = pd.merge(performance_data, hsi_performance_data, how='left', on=['Date'])

            Eurekahedge_index_cols = 'A,CH'
            Eurekahedge_performance_data = pd.read_excel(content, sheet_name=fund+' Stock Market Index', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = Eurekahedge_index_cols)
            Eurekahedge_performance_data.dropna(subset=['MTD'], how='all', inplace=True)
            Eurekahedge_performance_data['Eurekahedge_MTD'] = Eurekahedge_performance_data['MTD']
            Eurekahedge_performance_data['Date'] = pd.to_datetime(Eurekahedge_performance_data['Date'])
            del Eurekahedge_performance_data['MTD']
            performance_data = pd.merge(performance_data, Eurekahedge_performance_data, how='left', on=['Date'])

            performance_data = performance_data[performance_data['Date'].dt.year == np.int64(year)]
            #performance_data = performance_data[['Date',fund+' monthly return','HSCEI MTD','MXCN_MTD','Eurekahedge_MTD','MSCI_EMERGING_MTD','MSCI_AC_MTD']]

        return performance_data

    def quarterlyReportFromExcel(self, fund, yearAndQuarter):
        year = yearAndQuarter.split('-')[0]
        quarter = yearAndQuarter.split('-')[1]
        files = self.get_file_name(fund, quarter, year)
        exposure_by_area_total = pd.DataFrame(columns=['Quarter','Fund','Month','Year','Area','LongExp','ShortExp','TotalExp','NetExp','LongPnL','ShortPnL','NetPnL'])
        exposure_by_strat_total = pd.DataFrame(columns=['Quarter','Fund','Month','Year','Strat','LongExp','ShortExp','TotalExp','NetExp','LongPnL','ShortPnL','NetPnL'])
        exposure_by_industry_total = pd.DataFrame(columns=['Quarter','Fund','Month','Year','Industry','LongPnL','ShortPnL','NetPnL'])
        if fund == 'SLHL':
            skip_rows_for_area = 5
            skip_rows_for_strat = 30
            skip_rows_industry = -1  ## -1 mean no need to read
        elif fund in ['ZJNF', 'DCL']:
            skip_rows_for_area = 5
            skip_rows_for_strat = -1  ## -1 mean no need to read
            skip_rows_industry = 37
        elif fund in ['CVF']:
            skip_rows_for_area = 5
            skip_rows_for_strat = 13
            skip_rows_industry = 41
        for file_name in files:
            area_exposure_data, strat_exposure_data, industry_exposure_data = self.extract_data(fund, year, yearAndQuarter, file_name, skip_rows_for_area, skip_rows_for_strat, skip_rows_industry)
            if area_exposure_data.empty:
                logging.error(fund+' '+yearAndQuarter+' data is empty, pls check!')
                break
            exposure_by_area_total = pd.concat([exposure_by_area_total, area_exposure_data[['Quarter','Fund','Month','Year','Area','LongExp','ShortExp','TotalExp','NetExp','LongPnL','ShortPnL','NetPnL']]],axis=0)
            if skip_rows_for_strat != -1:
                exposure_by_strat_total = pd.concat([exposure_by_strat_total, strat_exposure_data[['Quarter','Fund','Month','Year','Strat','LongExp','ShortExp','TotalExp','NetExp','LongPnL','ShortPnL','NetPnL']]],axis=0)
            if skip_rows_industry != -1:
                exposure_by_industry_total = pd.concat([exposure_by_industry_total, industry_exposure_data[['Quarter','Fund','Month','Year','Industry','LongPnL','ShortPnL','NetPnL']]],axis=0)

        #### area
        exposure_for_LS = exposure_by_area_total[exposure_by_area_total['Area']=='合计'].copy()
        exposure_for_LS['MTD'] = exposure_for_LS['LongPnL'] + exposure_for_LS['ShortPnL']
        exposure_for_LS['MTD_FOR_CALCULATE_1'] =exposure_for_LS['MTD'].shift(1)
        exposure_for_LS['MTD_FOR_CALCULATE_1'] = exposure_for_LS['MTD_FOR_CALCULATE_1'].fillna(0)
        exposure_for_LS['MTD_FOR_CALCULATE_2'] =exposure_for_LS['MTD'].shift(2)
        exposure_for_LS['MTD_FOR_CALCULATE_2'] = exposure_for_LS['MTD_FOR_CALCULATE_2'].fillna(0)
        exposure_for_LS['LongPnL'] = exposure_for_LS['LongPnL'] * (1+exposure_for_LS['MTD_FOR_CALCULATE_1'])* (1+exposure_for_LS['MTD_FOR_CALCULATE_2'])
        exposure_for_LS['ShortPnL'] = exposure_for_LS['ShortPnL'] * (1+exposure_for_LS['MTD_FOR_CALCULATE_1'])* (1+exposure_for_LS['MTD_FOR_CALCULATE_2'])

        exposure_LS_quarterly = exposure_for_LS.groupby(['Quarter','Fund','Year']).agg({'LongPnL': 'sum', 'ShortPnL': 'sum'})
        exposure_LS_quarterly = exposure_LS_quarterly.reset_index()

        ### strategy

        exposure_by_category_total = exposure_by_area_total.pivot_table(index=['Quarter', 'Fund', 'Month', 'Year'],columns='Area', values='NetPnL',aggfunc='first').reset_index()
        if fund in ['CVF']:
            exposure_by_fx_total = exposure_by_strat_total[exposure_by_strat_total['Strat'] == '汇率']
            exposure_by_fx_total['FxNetPnL'] = exposure_by_fx_total['NetPnL']
            exposure_by_category_total = pd.merge(exposure_by_category_total,exposure_by_fx_total[['Quarter','Fund','Month','Year','Strat','FxNetPnL']], how='left', on=['Quarter','Fund','Month','Year'])
        exposure_by_category_total['Mainland'] = exposure_by_category_total['大陆'.decode('utf-8')]
        exposure_by_category_total['HK'] = exposure_by_category_total['香港'.decode('utf-8')]
        exposure_by_category_total['US'] = exposure_by_category_total['美国'.decode('utf-8')]
        if fund in ['SLHL']:
            exposure_by_category_total['Others']=0
        else:
            exposure_by_category_total['Others'] = exposure_by_category_total['其他***'.decode('utf-8')]

        if fund in ['SLHL']:
            exposure_by_category_total['MTD'] = exposure_by_category_total['Mainland'] + exposure_by_category_total['HK'] + exposure_by_category_total['US']
            exposure_by_category_total['Oversea'] = 0
            exposure_by_category_total['FxNetPnL'] = 0
        elif fund in ['ZJNF','DCL','CVF']:
            exposure_by_category_total['MTD'] = exposure_by_category_total['Mainland'] + exposure_by_category_total['HK'] + exposure_by_category_total['Others'] + exposure_by_category_total['US']
            exposure_by_category_total['Oversea'] = 0
            exposure_by_category_total['FxNetPnL'] = 0
        elif fund in ['UNKOWN']:
            exposure_by_category_total['Oversea'] = exposure_by_category_total['HK'] + exposure_by_category_total['Others'] - exposure_by_category_total['FxNetPnL']
            exposure_by_category_total['MTD'] = exposure_by_category_total['Mainland'] + exposure_by_category_total['FxNetPnL'] + exposure_by_category_total['Oversea'] + exposure_by_category_total['US']


        exposure_by_category_total['MTD_FOR_CALC'] = exposure_by_category_total['MTD'].shift(1)
        exposure_by_category_total['MTD_FOR_CALC'] = exposure_by_category_total['MTD_FOR_CALC'].fillna(0)
        exposure_by_category_total['MTD_FOR_CALCULATE_2'] =exposure_by_category_total['MTD'].shift(2)
        exposure_by_category_total['MTD_FOR_CALCULATE_2'] = exposure_by_category_total['MTD_FOR_CALCULATE_2'].fillna(0)
        exposure_by_category_total['CNY_ASSET'] = exposure_by_category_total['Mainland'] * (1+exposure_by_category_total['MTD_FOR_CALC']) * (1+exposure_by_category_total['MTD_FOR_CALCULATE_2'])
        exposure_by_category_total['OVERSEA_ASSET'] = exposure_by_category_total['Oversea'] * (1+exposure_by_category_total['MTD_FOR_CALC']) * (1+exposure_by_category_total['MTD_FOR_CALCULATE_2'])
        exposure_by_category_total['FX_ASSET'] = exposure_by_category_total['FxNetPnL'] * (1+exposure_by_category_total['MTD_FOR_CALC']) * (1+exposure_by_category_total['MTD_FOR_CALCULATE_2'])
        exposure_by_category_total['HK_ASSET'] = exposure_by_category_total['HK'] * (1+exposure_by_category_total['MTD_FOR_CALC']) * (1+exposure_by_category_total['MTD_FOR_CALCULATE_2'])
        exposure_by_category_total['US_ASSET'] = exposure_by_category_total['US'] * (1+exposure_by_category_total['MTD_FOR_CALC']) * (1+exposure_by_category_total['MTD_FOR_CALCULATE_2'])
        exposure_by_category_total['Other_ASSET'] = exposure_by_category_total['Others'] * (1+exposure_by_category_total['MTD_FOR_CALC']) * (1+exposure_by_category_total['MTD_FOR_CALCULATE_2'])

        exposure_category_quarterly = exposure_by_category_total.groupby(['Quarter','Fund','Year']).agg({'CNY_ASSET': 'sum', 'OVERSEA_ASSET': 'sum', 'FX_ASSET':'sum', 'HK_ASSET':'sum', 'Other_ASSET':'sum','US_ASSET':'sum'})
        exposure_category_quarterly = exposure_category_quarterly.reset_index()


        ### industry
        if skip_rows_industry != -1:
            exposure_by_industry_category = exposure_by_industry_total.pivot_table(index=['Quarter', 'Fund', 'Month', 'Year'],columns='Industry', values='NetPnL',aggfunc='first').reset_index()
            exposure_by_industry_category['ConsumerSector'] = exposure_by_industry_category['消费'.decode('utf-8')]
            if fund in ['ZJNF']:
                exposure_by_industry_category['FinancialRealEstateSector'] = exposure_by_industry_category['金融房地产'.decode('utf-8')]
                exposure_by_industry_category['CyclicalSector'] = exposure_by_industry_category['制造'.decode('utf-8')]
                exposure_by_industry_category['MedicalAndGreenEnergySector'] = exposure_by_industry_category['医疗及医药'.decode('utf-8')]
                exposure_by_industry_category['BondSector'] = 0
                exposure_by_industry_category['MacroSector'] = 0
            else:
                exposure_by_industry_category['FinancialRealEstateSector'] = exposure_by_industry_category['金融地产'.decode('utf-8')]
                exposure_by_industry_category['CyclicalSector'] = exposure_by_industry_category['周期'.decode('utf-8')]
                exposure_by_industry_category['MedicalAndGreenEnergySector'] = exposure_by_industry_category['医药新能源'.decode('utf-8')]
                exposure_by_industry_category['BondSector'] = exposure_by_industry_category['债券'.decode('utf-8')]
                exposure_by_industry_category['MacroSector'] = exposure_by_industry_category['宏观'.decode('utf-8')]

            exposure_by_industry_category['TMTSector'] = exposure_by_industry_category['TMT']
            exposure_by_industry_category['OtherSectors'] = exposure_by_industry_category['其他'.decode('utf-8')]
            exposure_by_industry_category['QuantSector'] = exposure_by_industry_category['量化'.decode('utf-8')]
            exposure_by_industry_category['MTD'] = exposure_by_industry_category['ConsumerSector'] + exposure_by_industry_category['FinancialRealEstateSector'] \
                                                + exposure_by_industry_category['CyclicalSector'] + exposure_by_industry_category['MedicalAndGreenEnergySector'] \
                                                + exposure_by_industry_category['TMTSector'] + exposure_by_industry_category['OtherSectors'] \
                                                + exposure_by_industry_category['BondSector'] + exposure_by_industry_category['MacroSector'] \
                                                + exposure_by_industry_category['QuantSector']
            exposure_by_industry_category['MTD_FOR_CALC'] = exposure_by_industry_category['MTD'].shift(1)
            exposure_by_industry_category['MTD_FOR_CALC'] = exposure_by_industry_category['MTD_FOR_CALC'].fillna(0)
            exposure_by_industry_category['MTD_FOR_CALCULATE_2'] =exposure_by_industry_category['MTD'].shift(2)
            exposure_by_industry_category['MTD_FOR_CALCULATE_2'] = exposure_by_industry_category['MTD_FOR_CALCULATE_2'].fillna(0)
            exposure_by_industry_category['ConsumerSector_ASSET'] = exposure_by_industry_category['ConsumerSector'] * (1+exposure_by_industry_category['MTD_FOR_CALC']) * (1+exposure_by_industry_category['MTD_FOR_CALCULATE_2'])
            exposure_by_industry_category['FinancialRealEstateSector_ASSET'] = exposure_by_industry_category['FinancialRealEstateSector'] * (1+exposure_by_industry_category['MTD_FOR_CALC']) * (1+exposure_by_industry_category['MTD_FOR_CALCULATE_2'])
            exposure_by_industry_category['CyclicalSector_ASSET'] = exposure_by_industry_category['CyclicalSector'] * (1+exposure_by_industry_category['MTD_FOR_CALC']) * (1+exposure_by_industry_category['MTD_FOR_CALCULATE_2'])
            exposure_by_industry_category['MedicalAndGreenEnergySector_ASSET'] = exposure_by_industry_category['MedicalAndGreenEnergySector'] * (1+exposure_by_industry_category['MTD_FOR_CALC']) * (1+exposure_by_industry_category['MTD_FOR_CALCULATE_2'])
            exposure_by_industry_category['TMTSector_ASSET'] = exposure_by_industry_category['TMTSector'] * (1+exposure_by_industry_category['MTD_FOR_CALC']) * (1+exposure_by_industry_category['MTD_FOR_CALCULATE_2'])
            exposure_by_industry_category['OtherSectors_ASSET'] = exposure_by_industry_category['OtherSectors'] * (1+exposure_by_industry_category['MTD_FOR_CALC']) * (1+exposure_by_industry_category['MTD_FOR_CALCULATE_2'])
            exposure_by_industry_category['BondSector_ASSET'] = exposure_by_industry_category['BondSector'] * (1+exposure_by_industry_category['MTD_FOR_CALC']) * (1+exposure_by_industry_category['MTD_FOR_CALCULATE_2'])
            exposure_by_industry_category['MacroSector_ASSET'] = exposure_by_industry_category['MacroSector'] * (1+exposure_by_industry_category['MTD_FOR_CALC']) * (1+exposure_by_industry_category['MTD_FOR_CALCULATE_2'])
            exposure_by_industry_category['QuantSector_ASSET'] = exposure_by_industry_category['QuantSector'] * (1+exposure_by_industry_category['MTD_FOR_CALC']) * (1+exposure_by_industry_category['MTD_FOR_CALCULATE_2'])

            exposure_for_industry_quarterly = exposure_by_industry_category.groupby(['Quarter','Fund','Year']).agg({'ConsumerSector_ASSET': 'sum', 'FinancialRealEstateSector_ASSET': 'sum',
                                                                                                                'CyclicalSector_ASSET':'sum', 'MedicalAndGreenEnergySector_ASSET':'sum',
                                                                                                                'TMTSector_ASSET':'sum', 'OtherSectors_ASSET':'sum',
                                                                                                                'BondSector_ASSET':'sum', 'MacroSector_ASSET':'sum',
                                                                                                                'QuantSector_ASSET':'sum'})
            exposure_for_industry_quarterly = exposure_for_industry_quarterly.reset_index()
        else:
            exposure_for_industry_quarterly = None

        return exposure_LS_quarterly,exposure_category_quarterly,exposure_for_industry_quarterly

    def calcAndSaveQuarterlyReportOnshore(self,fund,quarter):
        (exposure_LS_quarterly,exposure_category_quarterly,exposure_for_industry_quarterly) = self.quarterlyReportFromExcel(fund,quarter)
        self.saveOrUpsertIntoMonthlyData(exposure_LS_quarterly,exposure_category_quarterly,exposure_for_industry_quarterly)
        (exposure_LS_quarterly_Accumulate, exposure_category_quarterly_Accumulate, exposure_industry_quarterly_Accumulate) = self.semiYearReport(fund, quarter)
        self.saveOrUpsertIntoMonthlyData(exposure_LS_quarterly_Accumulate,exposure_category_quarterly_Accumulate,exposure_industry_quarterly_Accumulate,isAccum=True)

    def calcAndSaveQuarterlyReportOffshore(self,fund,quarter,file_path):
        #(by_sector_pnl_quarterly,sector_pnl_quarterly,market_cap_last_month_long,market_cap_last_month_short,eq_net_gross_total) = self.quarterlyReportFromExcelOfffshore(fund,quarter)

        year = quarter.split('-')[0]
        quarter = quarter.split('-')[1]
        transparency_file = self.get_transparency_file_name(fund,quarter,year)
        alpha_outperform_file = self.get_alphaoutperform_file_name(quarter, year)

        liquidity_data  = self.extract_offshore_liquidity_data(transparency_file)
        performance_data = self.extract_performance_data(fund, year, alpha_outperform_file)
        #
        #self.saveOrUpsertIntoMonthlyData(exposure_LS_quarterly,exposure_category_quarterly)
        #(exposure_LS_quarterly_Accumulate, exposure_category_quarterly_Accumulate) = self.semiYearReport(fund, quarter)
        #self.saveOrUpsertIntoMonthlyData(exposure_LS_quarterly_Accumulate,exposure_category_quarterly_Accumulate,isAccum=True)
        #self.genOffshoreQuarterlyReport(fund, quarter, file_path, by_sector_pnl_quarterly, sector_pnl_quarterly, market_cap_last_month_long, market_cap_last_month_short, eq_net_gross_total,liquidity_data,performance_data)

    def genOffshoreQuarterlyReport(self,fund,quarter,file_path,by_sector_pnl_quarterly,sector_pnl_quarterly,market_cap_last_month_long, market_cap_last_month_short,eq_net_gross_total,liquidity_data,performance_data):
        shutil.copy(file_path + 'OffshoreTemplate.xlsx', file_path + fund + '-' + quarter + '.xlsx')
        path = file_path + fund + '-' + quarter + '.xlsx'
        pdExcelUtil.append_df_to_excel(path, eq_net_gross_total, sheet_name='Eq Net Gross',index=False, header=False)
        pdExcelUtil.append_df_to_excel(path, by_sector_pnl_quarterly, sheet_name='Sector', startrow=0, startcol=0,index=False, header=True)
        #pdExcelUtil.append_df_to_excel(path, sector_pnl_quarterly, sheet_name='Sector', startrow=0, startcol=0,index=False, header=False)


        pdExcelUtil.append_df_to_excel(path, market_cap_last_month_long, sheet_name='Market cap', startrow=0, startcol=0,index=False, header=True)
        pdExcelUtil.append_df_to_excel(path, market_cap_last_month_short, sheet_name='Market cap', startrow=market_cap_last_month_long.shape[0]+2, startcol=0,index=False, header=True)

        pdExcelUtil.append_df_to_excel(path, liquidity_data[liquidity_data['LSF (mtd) (USD)']=='Long']['Market Value Total'], sheet_name='Liquidity', startrow=2, startcol=2,index=False, header=False)
        pdExcelUtil.append_df_to_excel(path, liquidity_data[liquidity_data['LSF (mtd) (USD)']=='Short']['Market Value Total'], sheet_name='Liquidity', startrow=11, startcol=2,index=False, header=False)
        pdExcelUtil.append_df_to_excel(path, performance_data[:3], sheet_name='Performance', startrow=0, startcol=0, index=False, header=True)
        pdExcelUtil.append_df_to_excel(path, performance_data[3:6], sheet_name='Performance', startrow=6, startcol=0, index=False, header=False)
        pdExcelUtil.append_df_to_excel(path, performance_data[6:9], sheet_name='Performance', startrow=11, startcol=0, index=False, header=False)
        pdExcelUtil.append_df_to_excel(path, performance_data[9:12], sheet_name='Performance', startrow=16, startcol=0, index=False, header=False)

    def saveOrUpsertIntoMonthlyData(self, exposure_LS_quarterly, exposure_category_quarterly, exposure_for_industry_quarterly, isAccum=False):
        if isAccum:
            exposure_LS_quarterly['QuarterNo'] = exposure_LS_quarterly['Term']
        else:
            exposure_LS_quarterly['QuarterNo'] = exposure_LS_quarterly['Quarter'].str.split(pat='-').str[1]
        exposure_LS_quarterly['Month'] = np.where(exposure_LS_quarterly['QuarterNo']=='Q1','3',
                                                  np.where(exposure_LS_quarterly['QuarterNo'] == 'Q2', '6',
                                                           np.where(exposure_LS_quarterly['QuarterNo'] == 'Q3', '9','12')))
        fundCode = exposure_LS_quarterly['Fund'].iloc[0]
        year = exposure_LS_quarterly['Year'].iloc[0]
        month = exposure_LS_quarterly['Month'].iloc[0]
        sql1= 'select max(AsOfDate) as AsOfDate from RiskDb.risk.RiskFundBenchmarkReportExternal where FundCode=\''+fundCode+'\' and YEAR (AsOfDate)='+year+' and MONTH(AsOfDate)=\''+month+'\''
        max_data = self.selectDataFromDb(sql1)
        max_date = pd.to_datetime(max_data['AsOfDate']).iloc[0]

        if exposure_category_quarterly is not None:
            if isAccum:
                update_data = pd.merge(exposure_LS_quarterly, exposure_category_quarterly, how='left',on=['Term', 'Fund', 'Year'])
            else:
                update_data = pd.merge(exposure_LS_quarterly, exposure_category_quarterly, how='left', on=['Quarter','Fund','Year'])
            update_data['AsOfDate'] = max_date.strftime('%Y-%m-%d')
            records = pdUtil.dataFrameToSavableRecords(update_data,['US_ASSET', 'HK_ASSET', 'Other_ASSET', 'LongPnL', 'ShortPnL', 'CNY_ASSET', 'OVERSEA_ASSET', 'FX_ASSET','Fund','AsOfDate'])
            sql = 'update RiskDb.risk.RiskFundBenchmarkReportExternal set USAsset=?,HKAsset=?, OtherAsset=?, LongPNL=?, ShortPNL=?, CNYAsset=?, OverseaAsset=?, FXAsset=? where FundCode=? and AsOfDate=?'
            if isAccum:
                sql = 'update RiskDb.risk.RiskFundBenchmarkReportExternal set USAssetAccum=?,HKAssetAccum=?,OtherAssetAccum=?,LongPNLAccum=?,ShortPNLAccum=?,CNYAssetAccum=?,OverseaAssetAccum=?,FXAssetAccum=? where FundCode=? and AsOfDate=?'
            self.insertToDatabase(sql,records)
        else:
            update_data= exposure_LS_quarterly
            update_data['AsOfDate'] = max_date.strftime('%Y-%m-%d')
            records = pdUtil.dataFrameToSavableRecords(update_data,['LongPnL', 'ShortPnL','Fund','AsOfDate'])
            sql = 'update RiskDb.risk.RiskFundBenchmarkReportExternal set LongPNL=?, ShortPNL=? where FundCode=? and AsOfDate=?'
            if isAccum:
                sql = 'update RiskDb.risk.RiskFundBenchmarkReportExternal set LongPNLAccum=?,ShortPNLAccum=? where FundCode=? and AsOfDate=?'
            self.insertToDatabase(sql,records)

        self.save_for_industry_quarterly(exposure_for_industry_quarterly,max_date.strftime('%Y-%m-%d'), isAccum)

    def save_for_industry_quarterly(self,exposure_for_industry_quarterly,date_str, isAccum):
        if exposure_for_industry_quarterly is not None:
            exposure_for_industry_quarterly['AsOfDate'] = date_str
            records = pdUtil.dataFrameToSavableRecords(exposure_for_industry_quarterly, ['ConsumerSector_ASSET', 'FinancialRealEstateSector_ASSET', 'CyclicalSector_ASSET', 'MedicalAndGreenEnergySector_ASSET', 'TMTSector_ASSET', 'OtherSectors_ASSET', 'BondSector_ASSET', 'MacroSector_ASSET', 'QuantSector_ASSET','Fund','AsOfDate'])
            sql = 'update RiskDb.risk.RiskFundBenchmarkReportExternal set ConsumerSectorAsset=?, FinancialRealEstateSectorAsset=?, CyclicalSectorAsset=?, MedicalAndGreenEnergySectorAsset=?, TMTSectorAsset=?, OtherSectorsAsset=?, BondSectorAsset=?, MacroSectorAsset=?, QuantSectorAsset=? where FundCode=? and AsOfDate=?'
            if isAccum:
                sql = 'update RiskDb.risk.RiskFundBenchmarkReportExternal set ConsumerSectorAccum=?, FinancialRealEstateSectorAccum=?, CyclicalSectorAccum=?, MedicalAndGreenEnergySectorAccum=?, TMTSectorAccum=?, OtherSectorsAccum=?, BondSectorAccum=?, MacroSectorAccum=?, QuantSectorAccum=? where FundCode=? and AsOfDate=?'
            self.insertToDatabase(sql, records)

    def extract_data(self, fund, year, yearAndQuarter, file_name, skip_rows_area, skip_rows_strat, skip_rows_industry):
        # if fund in ['SLHL']:
        #     file_extension = '.xlsx'
        # else:
        file_extension = '.xlsm'
        content = xlrd.open_workbook(filename='\\\\192.168.200.3\\ftp\\Quarterly\\Sector_Analysis_Data\\' + file_name + file_extension,encoding_override='gb2312')
        area_exposure_data = pd.read_excel(content, sheet_name='国内基金', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area)
        na_indexes = area_exposure_data['多头'.decode('utf-8')].index[area_exposure_data['多头'.decode('utf-8')].isna()]
        na_index = na_indexes.values[0]
        exposure_by_area = area_exposure_data.iloc[:na_index]
        exposure_by_area['Quarter'] = yearAndQuarter
        exposure_by_area['Fund'] = fund
        exposure_by_area['Month'] = file_name[-2:]
        exposure_by_area['Year'] = year
        exposure_by_area['Area'] = exposure_by_area['区域'.decode('utf-8')]
        exposure_by_area['LongExp'] = exposure_by_area['多头'.decode('utf-8')]
        exposure_by_area['ShortExp'] = exposure_by_area['空头'.decode('utf-8')]
        exposure_by_area['TotalExp'] = exposure_by_area['总头寸'.decode('utf-8')]
        exposure_by_area['NetExp'] = exposure_by_area['净头寸'.decode('utf-8')]
        exposure_by_area['LongPnL'] = exposure_by_area['多头盈亏'.decode('utf-8')]
        exposure_by_area['ShortPnL'] = exposure_by_area['空头盈亏'.decode('utf-8')]
        exposure_by_area['NetPnL'] = exposure_by_area['区域净盈亏'.decode('utf-8')]

        if skip_rows_strat != -1:
            strat_exposure_data = pd.read_excel(content, sheet_name='国内基金', encoding='gb2312', engine='xlrd', skiprows=skip_rows_strat)
            na_indexes = strat_exposure_data['多头'.decode('utf-8')].index[strat_exposure_data['多头'.decode('utf-8')].isna()]
            na_index = na_indexes.values[0]
            exposure_by_strategy = strat_exposure_data.iloc[:na_index]
            exposure_by_strategy['Quarter'] = yearAndQuarter
            exposure_by_strategy['Fund'] = fund
            exposure_by_strategy['Month'] = file_name[-2:]
            exposure_by_strategy['Year'] = year
            exposure_by_strategy['Strat'] = exposure_by_strategy['策略'.decode('utf-8')]
            exposure_by_strategy['LongExp'] = exposure_by_strategy['多头'.decode('utf-8')]
            exposure_by_strategy['ShortExp'] = exposure_by_strategy['空头'.decode('utf-8')]
            exposure_by_strategy['TotalExp'] = exposure_by_strategy['总头寸'.decode('utf-8')]
            exposure_by_strategy['NetExp'] = exposure_by_strategy['净头寸'.decode('utf-8')]
            exposure_by_strategy['LongPnL'] = exposure_by_strategy['多头盈亏'.decode('utf-8')]
            exposure_by_strategy['ShortPnL'] = exposure_by_strategy['空头盈亏'.decode('utf-8')]
            exposure_by_strategy['NetPnL'] = exposure_by_strategy['策略净盈亏'.decode('utf-8')]
        else:
            exposure_by_strategy = None

        if skip_rows_industry != -1:
            area_useCols = 'P:X'
            industry_exposure_data = pd.read_excel(content, sheet_name='国内基金', encoding='gb2312', engine='xlrd', skiprows=skip_rows_industry, usecols=area_useCols)
            end_idx = industry_exposure_data.ix[industry_exposure_data['股票'.decode('utf-8')] == '合计'.decode('utf-8'), :].index.tolist()[0]

            industry_exposure_data = industry_exposure_data.iloc[:end_idx+1].copy()
            industry_exposure_data = industry_exposure_data[['股票'.decode('utf-8'),'权重'.decode('utf-8'), '行业盈亏'.decode('utf-8'),'Unnamed: 7','Unnamed: 8']]
            industry_exposure_data = industry_exposure_data.rename(columns={'行业盈亏'.decode('utf-8'): '多头'.decode('utf-8'),
                                                                             'Unnamed: 7': '空头'.decode('utf-8'),
                                                                             'Unnamed: 8': '净盈亏'.decode('utf-8'), })

            industry_exposure_data = industry_exposure_data.iloc[1:]
            industry_exposure_data.dropna(subset=['空头'.decode('utf-8')], how='all', inplace=True)
            industry_exposure_data['Quarter'] = yearAndQuarter
            industry_exposure_data['Fund'] = fund
            industry_exposure_data['Month'] = file_name[-2:]
            industry_exposure_data['Year'] = year
            industry_exposure_data['Industry'] = industry_exposure_data['股票'.decode('utf-8')]
            industry_exposure_data['LongPnL'] = industry_exposure_data['多头'.decode('utf-8')]
            industry_exposure_data['ShortPnL'] = industry_exposure_data['空头'.decode('utf-8')]
            industry_exposure_data['NetPnL'] = industry_exposure_data['净盈亏'.decode('utf-8')]
        else:
            industry_exposure_data = None


        return exposure_by_area, exposure_by_strategy, industry_exposure_data

    def get_offshore_file_name(self, fund, quarter, year):
        files = []
        if quarter == 'Q1':
            files.append(fund + year + '01')
            files.append(fund + year + '02')
            files.append(fund + year + '03')
        elif quarter == 'Q2':
            files.append(fund + year + '01')
            files.append(fund + year + '02')
            files.append(fund + year + '03')
            files.append(fund + year + '04')
            files.append(fund + year + '05')
            files.append(fund + year + '06')
        elif quarter == 'Q3':
            files.append(fund + year + '01')
            files.append(fund + year + '02')
            files.append(fund + year + '03')
            files.append(fund + year + '04')
            files.append(fund + year + '05')
            files.append(fund + year + '06')
            files.append(fund + year + '07')
            files.append(fund + year + '08')
            files.append(fund + year + '09')
        elif quarter == 'Q4':
            files.append(fund + year + '01')
            files.append(fund + year + '02')
            files.append(fund + year + '03')
            files.append(fund + year + '04')
            files.append(fund + year + '05')
            files.append(fund + year + '06')
            files.append(fund + year + '07')
            files.append(fund + year + '08')
            files.append(fund + year + '09')
            files.append(fund + year + '10')
            files.append(fund + year + '11')
            files.append(fund + year + '12')
        return files

    def get_file_name(self, fund, quarter, year):
        files = []
        if quarter == 'Q1':
            files.append(fund + year + '01')
            files.append(fund + year + '02')
            files.append(fund + year + '03')
        elif quarter == 'Q2':
            files.append(fund + year + '04')
            files.append(fund + year + '05')
            files.append(fund + year + '06')
        elif quarter == 'Q3':
            files.append(fund + year + '07')
            files.append(fund + year + '08')
            files.append(fund + year + '09')
        elif quarter == 'Q4':
            files.append(fund + year + '10')
            files.append(fund + year + '11')
            files.append(fund + year + '12')
        return files

    def get_transparency_file_name(self, fund, quarter, year):
        file = ''
        file_ext = '.xlsm'
        if quarter == 'Q1':
            file = fund + ' Transparency_' + year + '03' + file_ext
        elif quarter == 'Q2':
            file = fund + ' Transparency_' + year + '06' + file_ext
        elif quarter == 'Q3':
            file = fund + ' Transparency_' + year + '09' + file_ext
        elif quarter == 'Q4':
            file = fund + ' Transparency_' + year + '12' + file_ext
        return file

    def get_alphaoutperform_file_name(self, quarter, year):
        ##Pinpoint Alpha Outperform_201906
        file = ''
        file_ext='.xlsx'
        if quarter == 'Q1':
            file = 'Pinpoint Alpha Outperform_' + year + '03' + file_ext
        elif quarter == 'Q2':
            file = 'Pinpoint Alpha Outperform_' + year + '06' + file_ext
        elif quarter == 'Q3':
            file = 'Pinpoint Alpha Outperform_' + year + '09' + file_ext
        elif quarter == 'Q4':
            file = 'Pinpoint Alpha Outperform_' + year + '12' + file_ext
        return file

    def extract_offshore_data(self, fund, year, yearAndQuarter, file_name, skip_rows_area,skip_rows_strat):
        content = xlrd.open_workbook(filename='C:\\temp\\quarterly_data\\offshore\\' + file_name + '.xlsm',encoding_override='gb2312')
        area_useCols = 'P:Y'
        all_data = pd.read_excel(content, sheet_name='Transparency Report', encoding='gb2312', engine='xlrd', skiprows=skip_rows_area, usecols = area_useCols)
        end_idx = all_data.ix[all_data['Georgaphic (Equity)']=='Market Cap',:].index.tolist()[0]


        ####Market start
        area_exposure_data = all_data.iloc[:end_idx].copy()
        area_exposure_data =area_exposure_data[['Georgaphic (Equity)','Long','Short','Gross','Net','Long.1','Short.1','Net.1']]
        area_exposure_data = area_exposure_data.rename(columns={'Long.1': 'LongPnLContribution','Short.1': 'ShortPnLContribution','Net.1': 'NetPnLContribution'})

        area_exposure_data['Quarter'] = yearAndQuarter
        area_exposure_data['Fund'] = fund
        area_exposure_data['Month'] = file_name[-2:]
        area_exposure_data['Year'] = year
        area_exposure_data['Area'] = area_exposure_data['Georgaphic (Equity)']
        area_exposure_data['LongExp'] = area_exposure_data['Long']
        area_exposure_data['ShortExp'] = area_exposure_data['Short']
        area_exposure_data['NetExp'] = area_exposure_data['Net']
        area_exposure_data['LongPnL'] = area_exposure_data['LongPnLContribution']
        area_exposure_data['ShortPnL'] = area_exposure_data['ShortPnLContribution']
        area_exposure_data['NetPnL'] = area_exposure_data['NetPnLContribution']
        ####Market end


        ####Market cap start
        rest_data = all_data.iloc[end_idx+1:].copy()
        market_cap_data = rest_data.copy()
        market_cap_data = market_cap_data.rename(columns={'Georgaphic (Equity)':'Market Cap'})
        market_cap_data =market_cap_data[['Market Cap','Long','Short','Gross','Net','Long.1','Short.1','Net.1']]
        market_cap_data = market_cap_data.rename(columns={'Long.1': 'LongPnLContribution','Short.1': 'ShortPnLContribution','Net.1': 'NetPnLContribution',})
        market_cap_data = market_cap_data.reset_index()
        na_indexes = market_cap_data['Market Cap'].index[market_cap_data['Market Cap'].isna()]
        na_index = na_indexes.values[0]
        market_cap_data = market_cap_data.iloc[:na_index]
        market_cap_data['Quarter'] = yearAndQuarter
        market_cap_data['Fund'] = fund
        market_cap_data['Month'] = file_name[-2:]
        market_cap_data['Year'] = year
        ####Market cap end

        ####Strategy start
        rest_data = rest_data.iloc[na_index+4:].copy()
        equity_sector_data = rest_data.copy()
        del equity_sector_data['Net']
        equity_sector_data = equity_sector_data.rename(columns={'Georgaphic (Equity)':'Equity','Gross':'Net'})
        equity_sector_data = equity_sector_data[['Equity','Long','Short','Net']]
        equity_sector_data = equity_sector_data.reset_index()
        end_idx = equity_sector_data.ix[equity_sector_data['Equity']=='Other Strategies',:].index.tolist()[0]
        equity_sector_data = equity_sector_data.iloc[:end_idx-1].copy()
        equity_sector_data[['Long', 'Short', 'Net']] = equity_sector_data[['Long','Short','Net']].astype(float)
        equity_sector_data.dropna(subset=['Equity'], how='all', inplace=True)

        other_sector_data = rest_data.iloc[end_idx+1:].copy()
        del other_sector_data['Net']
        other_sector_data = other_sector_data.rename(columns={'Georgaphic (Equity)': 'Equity', 'Gross': 'Net'})
        other_sector_data = other_sector_data[['Equity', 'Long', 'Short', 'Net']]
        other_sector_data = other_sector_data.reset_index()
        end_idx = other_sector_data.ix[other_sector_data['Equity'] == 'Total', :].index.tolist()[0]
        other_sector_data = other_sector_data.iloc[:end_idx+1].copy()
        other_sector_data[['Long', 'Short', 'Net']] = other_sector_data[['Long', 'Short', 'Net']].astype(float)
        other_sector_data.dropna(subset=['Equity'], how='all', inplace=True)

        exposure_by_strategy = pd.concat([equity_sector_data, other_sector_data], axis=0)
        exposure_by_strategy['Quarter'] = yearAndQuarter
        exposure_by_strategy['Fund'] = fund
        exposure_by_strategy['Month'] = file_name[-2:]
        exposure_by_strategy['Year'] = year
        exposure_by_strategy['Strat'] = exposure_by_strategy['Equity']
        exposure_by_strategy['LongPnL'] = exposure_by_strategy['Long']
        exposure_by_strategy['ShortPnL'] = exposure_by_strategy['Short']
        exposure_by_strategy['NetPnL'] = exposure_by_strategy['Net']
        exposure_by_strategy['TotalNetPnL'] = exposure_by_strategy[exposure_by_strategy['Equity']=='Total']['Net'].iloc[0]
        exposure_by_strategy['TotalLongPnL'] = exposure_by_strategy[exposure_by_strategy['Equity']=='Total']['LongPnL'].iloc[0]
        exposure_by_strategy['TotalShortPnL'] = exposure_by_strategy[exposure_by_strategy['Equity']=='Total']['ShortPnL'].iloc[0]
        exposure_by_strategy = exposure_by_strategy[exposure_by_strategy['Equity']!='Total']
        ####Strategy end

        return area_exposure_data,exposure_by_strategy,market_cap_data

    def readOffshoreFundData(self,fund,yearAndQuarter):
        year = yearAndQuarter.split('-')[0]
        quarter = yearAndQuarter.split('-')[1]
        files = self.get_file_name(fund, quarter, year)
        exposure_by_area_total = pd.DataFrame(
            columns=['Quarter', 'Fund', 'Month', 'Year', 'Area', 'LongExp', 'ShortExp', 'TotalExp', 'NetExp', 'LongPnL',
                     'ShortPnL', 'NetPnL'])
        exposure_by_strat_total = pd.DataFrame(
            columns=['Quarter', 'Fund', 'Month', 'Year', 'Strat', 'LongExp', 'ShortExp', 'TotalExp', 'NetExp',
                     'LongPnL', 'ShortPnL', 'NetPnL'])
        for file_name in files:
            skip_rows_for_area = 4
            skip_rows_for_marketcap=15
            skip_rows_for_strat = 25
            area_exposure_data, strat_exposure_data = self.extract_offshore_data(fund, year, yearAndQuarter, file_name, skip_rows_for_area, skip_rows_for_strat)
            if area_exposure_data.empty:
                logging.error(fund + ' ' + yearAndQuarter + ' data is empty, pls check!')
                break
            exposure_by_area_total = pd.concat([exposure_by_area_total, area_exposure_data[
                ['Quarter', 'Fund', 'Month', 'Year', 'Area', 'LongExp', 'ShortExp', 'TotalExp', 'NetExp', 'LongPnL',
                 'ShortPnL', 'NetPnL']]], axis=0)
            exposure_by_strat_total = pd.concat([exposure_by_strat_total, strat_exposure_data[
                ['Quarter', 'Fund', 'Month', 'Year', 'Strat', 'LongExp', 'ShortExp', 'TotalExp', 'NetExp', 'LongPnL',
                 'ShortPnL', 'NetPnL']]], axis=0)

        exposure_for_LS = exposure_by_area_total[exposure_by_area_total['Area'] == 'Equity Total'].copy()
        exposure_for_LS['MTD'] = exposure_for_LS['LongPnL'] + exposure_for_LS['ShortPnL']
        exposure_for_LS['MTD_FOR_CALCULATE_1'] = exposure_for_LS['MTD'].shift(1)
        exposure_for_LS['MTD_FOR_CALCULATE_1'] = exposure_for_LS['MTD_FOR_CALCULATE_1'].fillna(0)
        exposure_for_LS['MTD_FOR_CALCULATE_2'] = exposure_for_LS['MTD'].shift(2)
        exposure_for_LS['MTD_FOR_CALCULATE_2'] = exposure_for_LS['MTD_FOR_CALCULATE_2'].fillna(0)
        exposure_for_LS['LongPnL'] = exposure_for_LS['LongPnL'] * (1 + exposure_for_LS['MTD_FOR_CALCULATE_1']) * (
                    1 + exposure_for_LS['MTD_FOR_CALCULATE_2'])
        exposure_for_LS['ShortPnL'] = exposure_for_LS['ShortPnL'] * (1 + exposure_for_LS['MTD_FOR_CALCULATE_1']) * (
                    1 + exposure_for_LS['MTD_FOR_CALCULATE_2'])

        exposure_LS_quarterly = exposure_for_LS.groupby(['Quarter', 'Fund', 'Year']).agg(
            {'LongPnL': 'sum', 'ShortPnL': 'sum'})
        exposure_LS_quarterly = exposure_LS_quarterly.reset_index()

        ''' '''
        exposure_by_category_total = exposure_by_area_total.pivot_table(index=['Quarter', 'Fund', 'Month', 'Year'], columns='Area', values='NetPnL', aggfunc='first').reset_index()
        exposure_by_fx_total = exposure_by_strat_total[exposure_by_strat_total['Strat'] == '汇率']
        exposure_by_fx_total['FxNetPnL'] = exposure_by_fx_total['NetPnL']
        exposure_by_category_total = pd.merge(exposure_by_category_total, exposure_by_fx_total[ ['Quarter', 'Fund', 'Month', 'Year', 'Strat', 'FxNetPnL']], how='left', on=['Quarter', 'Fund', 'Month', 'Year'])
        exposure_by_category_total['Mainland'] = exposure_by_category_total['大陆'.decode('utf-8')]
        exposure_by_category_total['HK'] = exposure_by_category_total['香港'.decode('utf-8')]
        exposure_by_category_total['Others'] = exposure_by_category_total['其他'.decode('utf-8')]
        exposure_by_category_total['Oversea'] = exposure_by_category_total['HK'] + exposure_by_category_total[
            'Others'] - exposure_by_category_total['FxNetPnL']
        if fund in ['ZJNF', 'SLHL', 'DCL']:
            exposure_by_category_total['MTD'] = exposure_by_category_total['Mainland'] + exposure_by_category_total[
                'HK'] + exposure_by_category_total['Others']
        else:
            exposure_by_category_total['MTD'] = exposure_by_category_total['Mainland'] + exposure_by_category_total[
                'FxNetPnL'] + exposure_by_category_total['Oversea']
        exposure_by_category_total['MTD_FOR_CALC'] = exposure_by_category_total['MTD'].shift(1)
        exposure_by_category_total['MTD_FOR_CALC'] = exposure_by_category_total['MTD_FOR_CALC'].fillna(0)
        exposure_by_category_total['MTD_FOR_CALCULATE_2'] = exposure_by_category_total['MTD'].shift(2)
        exposure_by_category_total['MTD_FOR_CALCULATE_2'] = exposure_by_category_total['MTD_FOR_CALCULATE_2'].fillna(0)
        exposure_by_category_total['CNY_ASSET'] = exposure_by_category_total['Mainland'] * (
                    1 + exposure_by_category_total['MTD_FOR_CALC']) * (
                                                              1 + exposure_by_category_total['MTD_FOR_CALCULATE_2'])
        exposure_by_category_total['OVERSEA_ASSET'] = exposure_by_category_total['Oversea'] * (
                    1 + exposure_by_category_total['MTD_FOR_CALC']) * (
                                                                  1 + exposure_by_category_total['MTD_FOR_CALCULATE_2'])
        exposure_by_category_total['FX_ASSET'] = exposure_by_category_total['FxNetPnL'] * (
                    1 + exposure_by_category_total['MTD_FOR_CALC']) * (
                                                             1 + exposure_by_category_total['MTD_FOR_CALCULATE_2'])
        exposure_by_category_total['HK_ASSET'] = exposure_by_category_total['HK'] * (
                    1 + exposure_by_category_total['MTD_FOR_CALC']) * (
                                                             1 + exposure_by_category_total['MTD_FOR_CALCULATE_2'])
        exposure_by_category_total['Other_ASSET'] = exposure_by_category_total['Others'] * (
                    1 + exposure_by_category_total['MTD_FOR_CALC']) * (
                                                                1 + exposure_by_category_total['MTD_FOR_CALCULATE_2'])

        exposure_category_quarterly = exposure_by_category_total.groupby(['Quarter', 'Fund', 'Year']).agg(
            {'CNY_ASSET': 'sum', 'OVERSEA_ASSET': 'sum', 'FX_ASSET': 'sum', 'HK_ASSET': 'sum', 'Other_ASSET': 'sum'})
        exposure_category_quarterly = exposure_category_quarterly.reset_index()


        return exposure_LS_quarterly, exposure_category_quarterly

    def getPreviousMonth(self, dateStr):
        run_date = pd.to_datetime(dateStr, format='%Y-%m-%d')
        previous_1m_date = run_date + relativedelta.relativedelta(months=-1)
        return previous_1m_date.strftime('%Y-%m')

    def getLastDayOfCurrentMonth(self, runMonthStr):
        run_date = pd.to_datetime(runMonthStr, format='%Y-%m')
        run_year = run_date.year
        run_month = run_date.month
        day_range = calendar.monthrange(run_year, run_month)
        end = datetime.date(run_year, run_month, day_range[1])
        return end.strftime('%Y-%m-%d')

    def runOffshoreFunds(self):
        currentDate = datetime.datetime.now()
        currentDateStr = currentDate.strftime('%Y-%m-%d')
        dateStr = self.getLastDayOfCurrentMonth(self.getPreviousMonth(currentDateStr))
        offshore_fund_list = ['PMSF_ClassA', 'PMSF_ClassB', 'PMSF_ClassD', 'PCF_ClassA', 'PCF_ClassB', 'PLUS_ClassA']
        #offshore_fund_list = ['PMSF_ClassA']
        for fundCode_Class in offshore_fund_list:
            fundCode = fundCode_Class.split('_')[0]
            class_info = fundCode_Class.split('_')[1]
            #self.loadOffshoreFundNAVData('C:\\devel\\2019MDD\\Fund\\quarterly\\offshore\\', fundCode, class_info)
            data = self.calcOffshoreMothlyReportFactors(dateStr, self.loadOffshoreFundNAVData('\\\\192.168.200.3\\ftp\\Quarterly\\offshore\\', fundCode, class_info, dateStr), fundCode, class_info)

    def runOnshoreFunds(self):
        currentDate = datetime.datetime.now()
        currentDateStr = currentDate.strftime('%Y-%m-%d')
        dateStr = self.getLastBussinessOnCurrentMonth(self.getPreviousMonth(currentDateStr))
        ##dateStr='2020-04-30'
        #onshore_fund_list = ['SLHL']
        onshore_fund_list = ['CVF','DCL','ZJNF','SLHL']
        # data = historical_return_data.calcFactors(dateStr,historical_return_data.loadAumAdjustmentFromExcel(dateStr,'\\\\192.168.200.3\\ftp\\Quarterly\\NetUnit_Data\\'+fundCode+'.xlsx',fundCode, benchmarkForSLHL=''),fundCode,benchmarkForSLHL='',freq='monthly')
        for fundCode in onshore_fund_list:
            logging.warning('running at '+fundCode)
            data = self.calcFactors(dateStr, self.loadAumAdjustmentFromExcel(dateStr, '\\\\192.168.200.3\\ftp\\Quarterly\\NetUnit_Data\\' + fundCode + '.xlsx', fundCode, 'HEDGESTRAT'), fundCode, 'HEDGESTRAT', 'monthly')


if __name__ == '__main__':
    historical_return_data = HistoricalFundPerformance()
    historical_return_data.initSqlServer('prod')
    historical_return_data.runOnshoreFunds()
    # for fund in ['DCL','ZJNF']:
    #     historical_return_data.calcAndSaveQuarterlyReportOnshore(fund,'2020-Q2')   ###onshore

    '''
    Quarterly Fund Report  - 结算团队季度对外报告 - 对外e
    '''
    historical_return_data.runOffshoreFunds()
    #historical_return_data.quarterlyReportFromExcelOfffshore('PMSF','2019-Q4')
    #historical_return_data.calcAndSaveQuarterlyReportOffshore('PLUS','2019-Q2','C:\\temp\\quarterly_data\\offshore\\') ###offshore
    historical_return_data.closeSqlServerConnection()




