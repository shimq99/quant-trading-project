# encoding:UTF-8
from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import pandas as pd
import logging
from benchmark.tools import PandasDBUtils as pdUtil
from benchmark.marketdata.MarketDataDownloader import *
from benchmark.risk_control_2019.RiksControlReportPMSFT05 import *
from benchmark.risk_control_2019.RiksControlReportPMSFT22 import *
from benchmark.base.CommonEnums import RiskControlHoldingLimitInfoType
from benchmark.base.CommonEnums import RiskControlStatus
from benchmark.base.CommonEnums import RiskControlGrossMarginLimitType
from benchmark.base.CommonEnums import RiskControlNoStatus
from benchmark.base.CommonEnums import RiskCommonReportType
from benchmark.base.CommonEnums import RiskLimitType
from benchmark.base.CommonEnums import RiskLimitStatus
from benchmark.tools import Utils
import numpy as np
import datetime
import json
from decimal import *
getcontext().prec = 6
from benchmark.position.PmsfCvfPosition import *
from benchmark.risk_control_2019.RiskMaxAbsValue import *



class RiskControlReports(Base):
    def __init__(self, env):
        self.env = env
        LogManager('RiskControlReports')

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
            self.cursor.executemany(sql, data)
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def initBenchmarkPortfolioWeight(self, filePath, team):
        data = pd.read_excel(filePath,sheet_name='Worksheet')
        data['TeamCode'] = team
        data['PortfolioCode'] = '2019'+team
        data['Weight'] = data['% Wgt (P)']
        data['ConstituentTicker'] = data['Ticker']+' Equity'
        data['InceptionDate'] = pd.to_datetime(self.inceptionDate,format='%Y-%m-%d')
        data['HoldingShares'] = data['Pos (Disp) (P)']
        data['InceptionDatePrice'] = data['Px Close (P)']
        data['Currency'] = data['Crncy']

        #BenchmarkPortfolioWeight
        records = pdUtil.dataFrameToSavableRecords(data,['PortfolioCode','ConstituentTicker','Weight','HoldingShares','InceptionDate','InceptionDatePrice','TeamCode','Currency'])
        sql = 'insert into RiskDb.bench.BenchmarkPortfolioWeight(PortfolioCode,ConstituentTicker,Weight,HoldingShares,InceptionDate,InceptionDatePrice,TeamCode,Currency) values(?,?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, records)


        ##BenchmarkPortfolioConstEodPrice
        data['CloseValue'] = data['Mkt Val (P)']
        data['Memo'] = 'Init'
        sql2 = 'insert into RiskDb.bench.BenchmarkPortfolioConstEodPrice(PortfolioCode,BbgTicker,TradeDate,CloseValue,Memo) values(?, ?, ?, ?, ?)'
        self.insertToDatabase(sql2, data[['PortfolioCode','ConstituentTicker','InceptionDate','CloseValue','Memo']].values.tolist())

        ##BenchmarkPortfolioEodPrice
        records=[]
        sql3 = 'insert into RiskDb.bench.BenchmarkPortfolioEodPrice(PortfolioCode,TradeDate,CloseValue,Memo) values(?, ?, ?, ?)'
        records.append(('2019'+team,self.inceptionDate,self.initPortfolioValue,'Init'))
        self.insertToDatabase(sql3, records)

    def initRiskControlData(self):
        self.initSqlServer(self.env)
        fundInfoData = self.getFundInfo()
        fundInfoData['FUND'] =fundInfoData['FundCode']
        fundInfoData['FundId'] =fundInfoData['FundId'].astype(str)
        bookInfoData = self.getBookInfo()
        bookInfoData['Book'] =bookInfoData['BookCode']
        bookInfoData['BookId'] =bookInfoData['BookId'].astype(str)
        data = pd.read_csv('C:\\devel\\2019risk\\RiskControlData-20190128.csv')

        data = pd.merge(data, fundInfoData[['FUND','FundId']],how='left', on=['FUND'])
        data = pd.merge(data, bookInfoData[['Book','BookId']], how='left', on=['Book'])
        data['BeginDate'] = '2019-01-01'
        data['EndDate'] = '2029-01-01'
        naData = data[data['BookId'].isna()]
        naData = naData[['FUND','Book','BookId','FundId']]
        data = data[~data['Book'].isin(['CC02','CC03','CC04','CC05','UB04','UB05'])]
        data = data.fillna(0)
        data['StopLossLimit1'] = data['StopLossLimit1'].astype(str)
        data['StopLossLimit2'] = data['StopLossLimit2'].astype(str)
        data['StopLossLimit3'] = data['StopLossLimit3'].astype(str)

        data['RecoveryLimit1'] = data['RecoveryLimit1'].astype(str)
        data['RecoveryLimit2'] = data['RecoveryLimit2'].astype(str)
        data['RecoveryLimit3'] = data['RecoveryLimit3'].astype(str)
        stoploss_records = pdUtil.dataFrameToSavableRecords(data,['BeginDate', 'EndDate', 'FundId', 'BookId', 'StopLossLimit1', 'StopLossLimit2', 'StopLossLimit3'])

        recovery_records = pdUtil.dataFrameToSavableRecords(data,['BeginDate', 'EndDate', 'FundId', 'BookId', 'RecoveryLimit1', 'RecoveryLimit2', 'RecoveryLimit3'])
        sql = 'insert into RiskDb.ref.RiskControlLossLimitInfo(BeginDate, EndDate, FundId, BookId, StopLossLimit1, StopLossLimit2, StopLossLimit3) values(?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, stoploss_records)

        sql = 'insert into RiskDb.ref.RiskControlRecoveryLimitInfo(BeginDate, EndDate, FundId, BookId, RecoveryLimit1, RecoveryLimit2, RecoveryLimit3) values(?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, recovery_records)

        self.closeSqlServerConnection()

    def initRiskControlNetLimitData(self):
        self.initSqlServer(self.env)
        fundInfoData = self.getFundInfo()
        fundInfoData['FUND'] =fundInfoData['FundCode']
        fundInfoData['FundId'] =fundInfoData['FundId'].astype(str)
        bookInfoData = self.getBookInfo()
        bookInfoData['Book'] =bookInfoData['BookCode']
        bookInfoData['BookId'] =bookInfoData['BookId'].astype(str)
        data = pd.read_csv('C:\\devel\\2019risk\\RiskControlData-20190128_Net_Limit.csv')

        data = pd.merge(data, fundInfoData[['FUND','FundId']],how='left', on=['FUND'])
        data = pd.merge(data, bookInfoData[['Book','BookId']], how='left', on=['Book'])
        data['BeginDate'] = '2019-01-01'
        data['EndDate'] = '2029-01-01'
        naData = data[data['BookId'].isna()]
        naData = naData[['FUND','Book','BookId','FundId']]
        data = data[~data['Book'].isin(['CC02','CC03','CC04','CC05','UB04','UB05'])]
        data = data.fillna(0)
        data['Net_Ceiling'] = data['Net_Ceiling'].astype(str)
        data['Net_Floor'] = data['Net_Floor'].astype(str)
        data['Gross'] = data['Gross'].astype(str)
        data['Type'] = np.where(data['BookId']!=0,RiskControlHoldingLimitInfoType.SINGLE_BOOK.value,np.where(data['Book']==data['FUND'], RiskControlHoldingLimitInfoType.SINGLE_FUND.value, RiskControlHoldingLimitInfoType.FUND_IN_FUND.value))
        data['BookId'] = np.where(data['Type']==RiskControlHoldingLimitInfoType.FUND_IN_FUND.value, fundInfoData[fundInfoData['FUND']==data['Book']]['FundId'], data['BookId'])
        holding_limit_records = pdUtil.dataFrameToSavableRecords(data,['BeginDate', 'EndDate', 'FundId', 'BookId', 'Net_Ceiling', 'Net_Floor', 'Gross','Type'])
        sql = 'insert into RiskDb.ref.RiskControlHoldingLimitInfo(BeginDate, EndDate, FundId, BookId, NetLimitCeiling, NetLimitFloor, GrossLimit,Type) values(?,?,?,?,?,?,?,?)'
        self.insertToDatabase(sql, holding_limit_records)
        self.closeSqlServerConnection()

    def selectFromDB(self,sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def getRiskControlLossLimitData(self, dateStr):
        sql = 'select BeginDate,EndDate,FundId,BookId,StopLossLimit1,StopLossLimit2,StopLossLimit3,BookCode,FundCode,LimitType,Status from RiskDb.ref.RiskControlLossLimitInfo where BeginDate <=\''+dateStr+'\' AND EndDate >=\''+dateStr+'\''
        data = self.selectFromDB(sql)
        data['FundId'] = data['FundId'].astype(str)
        data['BookId'] = data['BookId'].astype(str)

        data['LimitName'] = data['FundCode'] + '-' + data['BookCode']
        data['BeginDate'] = pd.to_datetime(data['BeginDate'])
        data['EndDate'] = pd.to_datetime(data['EndDate'])
        currentRunDate = pd.to_datetime(dateStr, format='%Y-%m-%d')
        temporary_limit_data = data[(data['LimitType'] == RiskLimitType.TEMPORARY_LIMIT.value)
                                    & (data['Status'] == RiskLimitStatus.APPROVED.value)
                                    & (data['BeginDate'] <= currentRunDate)
                                    & (data['EndDate'] >= currentRunDate)].copy()
        data = data[data['LimitType'] == RiskLimitType.DEFAULT_LIMIT.value]
        data = pd.concat([data, temporary_limit_data], axis=0, sort=True)
        if not temporary_limit_data.empty:
            # replace default limit if temporary limit still valid which dateStr between begindate and enddate
            temp_limit_list = list(temporary_limit_data['LimitName'].unique())
            data.drop(data[(data['LimitName'].isin(temp_limit_list)) & (data['LimitType'] == RiskLimitType.DEFAULT_LIMIT.value)].index, inplace=True)
        del data['LimitName']
        del data['BeginDate']
        del data['EndDate']
        del data['Status']
        return data

    def getFundInfo(self):
        sql = 'select FundId,FundCode from RiskDb.ref.Fund where IsActive=1 and IsReal=1'
        data = self.selectFromDB(sql)
        data['FundId'] = data['FundId'].astype(str)
        return data

    def getBookInfo(self):
        sql = 'select BookId,BookCode from RiskDb.ref.Book'
        data = self.selectFromDB(sql)
        data['BookId'] = data['BookId'].astype(str)
        return data


    def getTeamMigrationInfo(self):
        sql = 'SELECT OldFundCode, OldTeamCode,NewFundCode,NewTeamCode,Date FROM RiskDb.ref.TeamMigrationInfo'
        data = self.selectFromDB(sql)
        data['OldFundBookCode'] = data['OldFundCode']+'-'+data['OldTeamCode']
        data['NewFundBookCode'] = data['NewFundCode']+'-'+data['NewTeamCode']
        return data

    def getMaxGrossNotionalAfterMaxDD(self,endDateStr, fundBookCodeList):
        endDate = datetime.datetime.strptime(endDateStr, '%Y-%m-%d')
        startDateStr = str(endDate.year)+'-01-01'
        team_migration_info = self.getTeamMigrationInfo()
        raw_data_list = []
        for fundBookCode in fundBookCodeList:
            fundCode = fundBookCode.split('-')[0]
            bookCode = fundBookCode.split('-')[1]
            data = self.getHistoryPerformance(startDateStr, fundCode, bookCode)
            team_migration_data = team_migration_info[team_migration_info['NewFundBookCode']==fundBookCode].copy()
            if not team_migration_data.empty:
                oldFundBookCode = team_migration_data['OldFundBookCode'].iloc[0]
                old_fundCode = oldFundBookCode.split('-')[0]
                old_bookCode = oldFundBookCode.split('-')[1]
                old_data = self.getHistoryPerformance(startDateStr, old_fundCode, old_bookCode)
                old_data['Fund'] = fundCode
                old_data['Book'] = bookCode
                data = pd.concat([data, old_data], axis=0, sort=True)
            data.loc[:, ('Date')] = pd.to_datetime(data['Date'])
            current_data = self.getPerformance(startDateStr,endDateStr, fundCode, bookCode)
            current_data.loc[:, ('Date')] = pd.to_datetime(current_data['AsOfDate'])
            data = pd.merge(data, current_data[['Date', 'Fund', 'Book', 'maxDDFrom', 'maxDDTo']], how='left', on=['Date', 'Fund', 'Book'])
            data.sort_values('Date', ascending=True, inplace=True)
            data = data[data['Date']<=pd.to_datetime(endDateStr,format='%Y-%m-%d')]
            max_dd = data['MaxDD'].min()
            minMaxDD_data = data[data['MaxDD']==max_dd].copy()
            minMaxDD_data['Date'].iloc[0]
            max_gross_notional = data[data['Date'] >= minMaxDD_data['maxDDFrom'].iloc[0]]['GrossNotional'].max()
            raw_data_list.append([fundCode,bookCode,max_gross_notional])
        max_gross_notional_data = pd.DataFrame(raw_data_list,columns=['FundCode', 'BookCode','MaxGrossNotionalAfterMDD'])
        max_gross_notional_data['BookCode'] = np.where(max_gross_notional_data['BookCode'] == max_gross_notional_data['FundCode'], None, max_gross_notional_data['BookCode'])
        return max_gross_notional_data

    def getPerformance(self, startDateStr, endDateStr, fundCode, bookCode):
        sql = 'SELECT PerformanceId,AsOfDate,F.FundCode as Fund,B.BookCode as Book,MaxDd,CurrDd,Recovery,maxDDFrom,maxDDTo  FROM RiskDb.risk.Performance P left join RiskDb.ref.Fund F on F.FundId=P.FundId left join RiskDb.ref.Book B on B.BookId=P.BookId where F.Fundcode=\'' + fundCode + '\' and AsOfDate between\'' + startDateStr + '\' and \'' + endDateStr
        if fundCode==bookCode:
            sql +=  '\' and B.BookCode is null'
        else:
            sql +=  '\' and B.BookCode=\'' + bookCode +'\''
        data = self.selectFromDB(sql)
        if fundCode == bookCode:
            data['Book'] = bookCode
        return data

    def getHistoryPerformance(self,dateStr, fundCode, bookCode):
        sql = 'EXEC RiskDb.risk.usp_getHistoryPerformance @beginDate = \'' + dateStr + '\', @fund=\'' +fundCode+'\', @team=\'' + bookCode+'\''
        return self.selectFromDB(sql)

    def getRiskControlHoldingLimitData(self, dateStr):
        sql = 'select BeginDate,EndDate,HoldingNetLimitCeiling,HoldingNetLimitFloor,GrossLimit,Type,FundCode,BookCode,DerivativeCashLimit,StockLongLimit,StockShortLimit,StopLossNetLimitCeiling,StopLossNetLimitFloor,GrossMarginLimit,GrossMarginLimitTypeStr,MarketHoldingLimit,StockLimit,BondHoldingLimit,LimitType,Status from RiskDb.ref.RiskControlHoldingLimitInfo where BeginDate <=\''+dateStr+'\' AND EndDate >=\''+dateStr+'\''
        data = self.selectFromDB(sql)
        data['HoldingNetLimitCeiling'] = data['HoldingNetLimitCeiling'].fillna(value=pd.np.nan)
        data['HoldingNetLimitFloor'] = data['HoldingNetLimitFloor'].fillna(value=pd.np.nan)
        data['GrossLimit'] = data['GrossLimit'].fillna(value=pd.np.nan)
        data['DerivativeCashLimit'] = data['DerivativeCashLimit'].fillna(value=pd.np.nan)
        data['StockLongLimit'] = data['StockLongLimit'].fillna(value=pd.np.nan)
        data['StockShortLimit'] = data['StockShortLimit'].fillna(value=pd.np.nan)
        data['StopLossNetLimitCeiling'] = data['StopLossNetLimitCeiling'].fillna(value=pd.np.nan)
        data['StopLossNetLimitFloor'] = data['StopLossNetLimitFloor'].fillna(value=pd.np.nan)
        data['GrossMarginLimit'] = data['GrossMarginLimit'].fillna(value=pd.np.nan)
        data['GrossMarginLimitType'] = data['GrossMarginLimitTypeStr'].fillna(value=pd.np.nan)
        data['MarketHoldingLimit'] = data['MarketHoldingLimit'].fillna(value=pd.np.nan)
        data['StockLimit'] = data['StockLimit'].fillna(value=pd.np.nan)
        data['BondHoldingLimit'] = data['BondHoldingLimit'].fillna(value=pd.np.nan)

        data['LimitName'] = data['FundCode'] +'-'+ data['BookCode']
        data['BeginDate'] = pd.to_datetime(data['BeginDate'])
        data['EndDate'] = pd.to_datetime(data['EndDate'])
        currentRunDate = pd.to_datetime(dateStr, format='%Y-%m-%d')
        temporary_limit_data = data[(data['LimitType']==RiskLimitType.TEMPORARY_LIMIT.value)
                                    & (data['Status']==RiskLimitStatus.APPROVED.value)
                                    & (data['BeginDate']<=currentRunDate)
                                    & (data['EndDate']>=currentRunDate)].copy()
        data = data[data['LimitType']==RiskLimitType.DEFAULT_LIMIT.value]
        data = pd.concat([data, temporary_limit_data], axis=0, sort=True)
        if not temporary_limit_data.empty:
            #replace default limit if temporary limit still valid which dateStr between begindate and enddate
            temp_limit_list = list(temporary_limit_data['LimitName'].unique())
            exclude_data = data[(data['LimitName'].isin(temp_limit_list)) & (data['LimitType']==RiskLimitType.DEFAULT_LIMIT.value)]
            index_test = exclude_data.index
            data.drop(data[(data['LimitName'].isin(temp_limit_list)) & (data['LimitType']==RiskLimitType.DEFAULT_LIMIT.value)].index, inplace=True)
        del data['LimitName']
        del data['BeginDate']
        del data['EndDate']
        del data['Status']
        return data

    def getRiskControlRecoveryLimitData(self, dateStr):
        sql = 'select BeginDate,EndDate,RecoveryLimit1,RecoveryLimit2,RecoveryLimit3,FundCode,BookCode,LimitType,Status from RiskDb.ref.RiskControlRecoveryLimitInfo where EndDate >=\''+dateStr+'\''
        data = self.selectFromDB(sql)
        #data['FundId'] = data['FundId'].astype(str)
        #data['BookId'] = data['BookId'].astype(str)
        data['LimitName'] = data['FundCode'] + '-' + data['BookCode']
        data['BeginDate'] = pd.to_datetime(data['BeginDate'])
        data['EndDate'] = pd.to_datetime(data['EndDate'])
        currentRunDate = pd.to_datetime(dateStr, format='%Y-%m-%d')
        temporary_limit_data = data[(data['LimitType'] == RiskLimitType.TEMPORARY_LIMIT.value)
                                    & (data['Status'] == RiskLimitStatus.APPROVED.value)
                                    & (data['BeginDate'] <= currentRunDate)
                                    & (data['EndDate'] >= currentRunDate)].copy()
        data = data[data['LimitType'] == RiskLimitType.DEFAULT_LIMIT.value]
        data = pd.concat([data, temporary_limit_data], axis=0, sort=True)
        if not temporary_limit_data.empty:
            # replace default limit if temporary limit still valid which dateStr between begindate and enddate
            temp_limit_list = list(temporary_limit_data['LimitName'].unique())
            data.drop(data[(data['LimitName'].isin(temp_limit_list)) & (data['LimitType'] == RiskLimitType.DEFAULT_LIMIT.value)].index, inplace=True)
        del data['LimitName']
        del data['BeginDate']
        del data['EndDate']
        del data['Status']
        return data

    def getPerformanceData(self, dateStr):#db_performance
        sql='EXEC RiskDb.risk.usp_excelGetPerformance @asofdate = \''+dateStr+'\''
        return self.selectFromDB(sql)

    def getNewTeamInfo(self, dateStr, currentData, fundList, teamList):
        currentData_copy = currentData.copy()
        currentData_copy['FundBook'] = currentData_copy['Fund']+'-'+currentData_copy['Book']
        currentDate = datetime.datetime.strptime(dateStr, '%Y-%m-%d')
        weekDay = currentDate.date().weekday()
        diff = 1
        if (weekDay == 0):
            diff += 2
        runDay = currentDate - datetime.timedelta(days=diff)
        runDayStr = runDay.strftime('%Y-%m-%d')

        sql = 'select * from RiskDb.risk.ExposureView where Instrument != \'Sector\' and Instrument != \'Market\' and AsOfDate = \'' + runDayStr + '\''
        if teamList:
            sql += ' and Book in (\'' + ('\',\'').join(teamList) + '\')'
        if fundList:
            sql += ' and Fund in (\'' + ('\',\'').join(fundList) + '\')'
        old_data = self.selectFromDB(sql)
        old_data['FundBook'] = old_data['Fund']+'-'+old_data['Book']
        old_fundbook_list = list(old_data['FundBook'].unique())
        currentData_copy['IsNew'] = np.where(currentData_copy['FundBook'].isin(old_fundbook_list),0,1)

        currentData_copy.drop_duplicates(subset='FundBook', inplace=True)
        return currentData_copy[['FundBook','IsNew']]

    def groupby_checks(self, data, cols):
        no_existed_cols = [col for col in cols if col not in data.columns]
        for no_existed_col in no_existed_cols:
            data[no_existed_col] = 0
        data[cols] = data[cols].astype(float)
        data[cols] = data[cols].fillna(0)
        return data


    def getExposureData(self, dateStr, fundList, teamList): #db_exposure
        sql='select * from RiskDb.risk.ExposureView where Instrument != \'Sector\' and Instrument != \'Market\' and AsOfDate = \''+dateStr+'\''
        if teamList:
            sql += ' and Book in (\'' + ('\',\'').join(teamList)+'\')'
        if fundList:
            sql += ' and Fund in (\'' + ('\',\'').join(fundList)+'\')'
        data = self.selectFromDB(sql)

        new_team_info = self.getNewTeamInfo(dateStr, data, fundList, teamList)
        data['FundCode'] = data['Fund']
        data['BookCode'] = data['Book']

        summaryCountryData = data.groupby(['FundCode', 'BookCode', 'Country']).agg({'NetNavBeta': 'sum', 'GrossNav': 'sum', 'NetNav': 'sum'})
        summaryCountryData = summaryCountryData.reset_index()
        ##T41(Yichi) T42(Dhawal) T43(Bryan) T44(Ajit) T46(Jack.Zhuo) T47(Zhou.Zou) T48(Bingliang.Yan) T49(Tiantao.Zheng) T50(Yu Hidema)
        runDate = datetime.datetime.strptime(dateStr, '%Y-%m-%d')
        effective_date = datetime.datetime.strptime('2020-03-31', '%Y-%m-%d')
        if runDate >= effective_date:
            summaryCountryData['NetNav'] = summaryCountryData['NetNavBeta']
        summaryCountryData = summaryCountryData.pivot_table(index=['FundCode','BookCode'], columns='Country', values='NetNav',aggfunc='first').reset_index()

        summaryInstrumentData = data.groupby(['FundCode', 'BookCode', 'Instrument']).agg({'GrossNav': 'sum', 'LongNavBeta': 'sum', 'ShortNavBeta': 'sum', 'NetNav': 'sum', 'LongNav': 'sum', 'ShortNav': 'sum'})
        summaryInstrumentData = summaryInstrumentData.reset_index()
        summaryNumberOfStockData = summaryInstrumentData[summaryInstrumentData['Instrument']=='N_Stock'].copy()
        summaryNumberOfStockData['NumberOfStock'] =summaryNumberOfStockData['GrossNav']

        summaryInstrumentLiquidityData = data.groupby(['FundCode', 'BookCode', 'Instrument', 'Liqudity']).agg({'LongNav': 'sum','ShortNav':'sum', 'GrossNav':'sum'})
        summaryInstrumentLiquidityData = summaryInstrumentLiquidityData.reset_index()
        twoDaysIlliquidData = summaryInstrumentLiquidityData[(summaryInstrumentLiquidityData['Instrument'] == 'EQTY_LIQDTY') & (summaryInstrumentLiquidityData['Liqudity'] == 2)].copy()
        twoDaysIlliquidData['TwoDaysIlliquidLongNav'] = twoDaysIlliquidData['LongNav']
        twoDaysIlliquidData['TwoDaysIlliquidShortNav'] = twoDaysIlliquidData['ShortNav']
        del twoDaysIlliquidData['LongNav']
        del twoDaysIlliquidData['ShortNav']
        fiveDaysIlliquidData = summaryInstrumentLiquidityData[(summaryInstrumentLiquidityData['Instrument'] == 'EQTY_LIQDTY') & (summaryInstrumentLiquidityData['Liqudity'] == 5)].copy()
        fiveDaysIlliquidData['FiveDaysIlliquidLongNav'] = fiveDaysIlliquidData['LongNav']
        fiveDaysIlliquidData['FiveDaysIlliquidShortNav'] = fiveDaysIlliquidData['ShortNav']
        del fiveDaysIlliquidData['LongNav']
        del fiveDaysIlliquidData['ShortNav']
        reitsAssetData = summaryInstrumentData[summaryInstrumentData['Instrument']=='REITS'].copy()
        reitsAssetData['REITSAssetNetNav'] = reitsAssetData['NetNav']
        del reitsAssetData['NetNav']

        summaryLongShortData = summaryInstrumentData[summaryInstrumentData['Instrument']=='MaxAbsMv'].copy()
        summaryLongShortData['StockLongNavBeta'] = summaryLongShortData['LongNavBeta']
        summaryLongShortData['StockShortNavBeta'] = summaryLongShortData['ShortNavBeta']


        summaryData = data.groupby(['FundCode', 'BookCode', 'InstrumentType','Category']).agg({'LongNavBeta':'sum','ShortNavBeta':'sum','LongNav':'sum','ShortNav':'sum','NetNav':'sum','GrossNav':'sum'})
        summaryData = summaryData.reset_index()
        summaryData2 = summaryData.groupby(['FundCode', 'BookCode', 'InstrumentType']).agg({'LongNavBeta':'sum','ShortNavBeta':'sum','LongNav':'sum','ShortNav':'sum','NetNav':'sum','GrossNav':'sum'})
        summaryData2 = summaryData2.reset_index()
        equityData = summaryData2[summaryData2['InstrumentType']=='Equity'].copy()
        equityData['EquityLongNav'] = equityData['LongNav'].astype(float)
        equityData['EquityShortNav'] = equityData['ShortNav'].astype(float)
        equityData['EquityLongNavBeta'] = equityData['LongNavBeta'].astype(float)
        equityData['EquityShortNavBeta'] = equityData['ShortNavBeta'].astype(float)
        equityData['EquityGrossNav'] = equityData['GrossNav'].astype(float)

        summaryData3 = summaryData.groupby(['FundCode', 'BookCode', 'Category']).agg({'LongNavBeta': 'sum', 'ShortNavBeta': 'sum', 'LongNav': 'sum', 'ShortNav': 'sum', 'NetNav': 'sum', 'GrossNav': 'sum'})

        summaryData3 = summaryData3.reset_index()
        equityCategoryData = summaryData3[summaryData3['Category'] == 'Equity'].copy()
        equityCategoryData['EquityCategoryGrossNav'] = equityCategoryData['GrossNav']

        summaryData4 = data.groupby(['FundCode', 'BookCode', 'Instrument', 'Category']).agg({'LongNavBeta': 'sum', 'ShortNavBeta': 'sum', 'LongNav': 'sum', 'ShortNav': 'sum', 'NetNav': 'sum', 'GrossNav': 'sum'})
        summaryData4 = summaryData4.reset_index()
        summaryData5 = summaryData4[(summaryData4['Category'] == 'Equity') & (summaryData4['Instrument'] != 'EQINDX_FT')].copy()
        summaryData5 = summaryData5.groupby(['FundCode', 'BookCode', 'Category']).agg({'LongNavBeta': 'sum', 'ShortNavBeta': 'sum', 'LongNav': 'sum', 'ShortNav': 'sum', 'NetNav': 'sum', 'GrossNav': 'sum'})
        summaryData5 = summaryData5.reset_index()
        equityCategoryDataExEQINDXFTInstrument = summaryData5[summaryData5['Category'] == 'Equity'].copy()
        equityCategoryDataExEQINDXFTInstrument['EquityCategoryGrossNavExcludeEQINDX_FT'] = equityCategoryDataExEQINDXFTInstrument['GrossNav']
        ##equityCategoryDataExEQINDXFTInstrument



        bondData = summaryData2[summaryData2['InstrumentType']=='Bond'].copy()
        bondData['BondNetNav'] = bondData['NetNav']
        bondData['BondGrossNav'] = bondData['GrossNav']
        fodata = summaryData[(summaryData['InstrumentType']=='FO') & (summaryData['Category'] == 'Equity')].copy()
        fodata['FOLongNavValueBeta'] = fodata['LongNavBeta']
        fodata['FOShortNavBeta'] = fodata['ShortNavBeta']
        fodata['FOLongNavNoBeta'] = fodata['LongNav']
        fodata['FOShortNavNoBeta'] = fodata['ShortNav']

        otherFOData = summaryData[(summaryData['InstrumentType']=='FO') & (summaryData['Category'] != 'Equity')]
        otherFOData = otherFOData.groupby(['FundCode', 'BookCode']).agg({'LongNav':'sum','ShortNav':'sum','NetNav':'sum','LongNavBeta':'sum','ShortNavBeta':'sum'})
        otherFOData = otherFOData.reset_index()
        otherFOData['OtherFOLongNavBeta'] = otherFOData['LongNavBeta']
        otherFOData['OtherFOShortNavBeta'] = otherFOData['ShortNavBeta']
        otherFOData['OtherFOLongNavNoBeta'] = otherFOData['LongNav']
        otherFOData['OtherFOShortNavNoBeta'] = otherFOData['ShortNav']
        othersData = summaryData2[(summaryData2['InstrumentType'] != 'FO') & (summaryData['InstrumentType'] != 'Equity') & (summaryData2['InstrumentType'] != 'Bond') & (summaryData2['InstrumentType'] != 'Currency')].copy()
        othersData = othersData.groupby(['FundCode', 'BookCode']).agg({'LongNav':'sum', 'ShortNav':'sum', 'NetNav':'sum'})
        othersData = othersData.reset_index()
        othersData['OthersNetNav'] = othersData['NetNav']

        equityIndxFTData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'EQINDX_FT'].copy()
        equityIndxFTData['EquityIndxFTLongNav'] = equityIndxFTData['LongNav']
        equityIndxFTData['EquityIndxFTShortNav'] = equityIndxFTData['ShortNav']


        if not bondData.empty:
            exposureUniData = pd.merge(equityData, bondData[['FundCode','BookCode','BondNetNav','BondGrossNav']], how='left', on=['FundCode','BookCode'])
        else:
            exposureUniData = equityData.copy()
            exposureUniData['BondNetNav'] = 0
            exposureUniData['BondGrossNav'] = 0
        if not equityCategoryData.empty:
            exposureUniData = pd.merge(exposureUniData, equityCategoryData[['FundCode', 'BookCode', 'EquityCategoryGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            exposureUniData['EquityCategoryGrossNav'] = 0
        if not equityCategoryDataExEQINDXFTInstrument.empty:
            exposureUniData = pd.merge(exposureUniData, equityCategoryDataExEQINDXFTInstrument[['FundCode', 'BookCode', 'EquityCategoryGrossNavExcludeEQINDX_FT']], how='left', on=['FundCode', 'BookCode'])
        else:
            exposureUniData['EquityCategoryGrossNavExcludeEQINDX_FT'] = 0
        if not fodata.empty:
            exposureUniData = pd.merge(exposureUniData, fodata[['FundCode','BookCode','FOLongNavValueBeta','FOShortNavBeta','FOLongNavNoBeta','FOShortNavNoBeta']], how='left', on=['FundCode', 'BookCode'])
        else:
            exposureUniData['FOLongNavValueBeta'] = 0
            exposureUniData['FOShortNavBeta'] = 0
            exposureUniData['FOLongNavNoBeta'] = 0
            exposureUniData['FOShortNavNoBeta'] = 0
        if not otherFOData.empty:
            exposureUniData = pd.merge(exposureUniData, otherFOData[['FundCode','BookCode','OtherFOLongNavBeta','OtherFOShortNavBeta', 'OtherFOLongNavNoBeta', 'OtherFOShortNavNoBeta']], how='left', on=['FundCode', 'BookCode'])
        else:
            exposureUniData['OtherFOLongNavBeta'] = 0
            exposureUniData['OtherFOShortNavBeta'] = 0
            exposureUniData['OtherFOLongNavNoBeta'] = 0
            exposureUniData['OtherFOShortNavNoBeta'] = 0
        if not othersData.empty:
            exposureUniData = pd.merge(exposureUniData, othersData[['FundCode','BookCode','OthersNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            exposureUniData['OthersNetNav'] = 0
        if not equityIndxFTData.empty:
            exposureUniData = pd.merge(exposureUniData, equityIndxFTData[['FundCode','BookCode','EquityIndxFTLongNav','EquityIndxFTShortNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            exposureUniData['EquityIndxFTLongNav'] = 0
            exposureUniData['EquityIndxFTShortNav'] = 0
        if not summaryCountryData.empty:
            summaryCountryData = self.groupby_checks(summaryCountryData,['China','Hong Kong','United States','Taiwan','Australia','Japan','Korea (South)','India','Singapore','Others'])
            exposureUniData = pd.merge(exposureUniData, summaryCountryData, how='left', on=['FundCode', 'BookCode'])
        else:
            exposureUniData['China'] = 0
            exposureUniData['Hong Kong'] = 0
            exposureUniData['United States'] = 0
            exposureUniData['Taiwan'] = 0
            exposureUniData['Australia'] = 0
            exposureUniData['Japan'] = 0
            exposureUniData['Korea (South)'] = 0
            exposureUniData['India'] = 0
            exposureUniData['Singapore'] = 0
        if not summaryNumberOfStockData.empty:
            exposureUniData = pd.merge(exposureUniData, summaryNumberOfStockData[['FundCode','BookCode','NumberOfStock']], how='left', on=['FundCode', 'BookCode'])
        else:
            exposureUniData['NumberOfStock'] = 0
        if not summaryLongShortData.empty:
            exposureUniData = pd.merge(exposureUniData, summaryLongShortData[['FundCode', 'BookCode', 'StockLongNavBeta', 'StockShortNavBeta']], how='left', on=['FundCode', 'BookCode'])
        else:
            exposureUniData['StockLongNavBeta'] = 0
            exposureUniData['StockShortNavBeta'] = 0
        exposureUniData = self.calcFundLevelExposureData(exposureUniData, summaryLongShortData)

        liquidityData = twoDaysIlliquidData.copy()

        if not fiveDaysIlliquidData.empty:
            liquidityData = pd.merge(liquidityData, fiveDaysIlliquidData, how='left', on=['FundCode', 'BookCode'])
        else:
            liquidityData['FiveDaysIlliquidLongNav'] = 0
            liquidityData['FiveDaysIlliquidShortNav'] = 0

        if not reitsAssetData.empty:
            liquidityData = pd.merge(liquidityData, reitsAssetData, how='left', on=['FundCode', 'BookCode'])
        else:
            liquidityData['REITSAssetNetNav'] = 0

        liquidityData = self.calcFundLevelLiquidityData(liquidityData)

        #varData = self.getVarData(dateStr)
        performanceData = self.getPerformanceData(dateStr)

        exposureUniData[['BondNetNav','BondGrossNav', 'EquityCategoryGrossNav', 'EquityCategoryGrossNavExcludeEQINDX_FT', 'FOLongNavValueBeta','FOShortNavBeta','FOLongNavNoBeta','FOShortNavNoBeta', 'OtherFOLongNavBeta','OtherFOShortNavBeta', 'OtherFOLongNavNoBeta', 'OtherFOShortNavNoBeta', 'OthersNetNav', 'EquityIndxFTLongNav','EquityIndxFTShortNav','NumberOfStock', 'StockLongNavBeta', 'StockShortNavBeta']] = exposureUniData[['BondNetNav','BondGrossNav', 'EquityCategoryGrossNav', 'EquityCategoryGrossNavExcludeEQINDX_FT', 'FOLongNavValueBeta','FOShortNavBeta','FOLongNavNoBeta','FOShortNavNoBeta', 'OtherFOLongNavBeta','OtherFOShortNavBeta', 'OtherFOLongNavNoBeta', 'OtherFOShortNavNoBeta', 'OthersNetNav', 'EquityIndxFTLongNav','EquityIndxFTShortNav','NumberOfStock', 'StockLongNavBeta', 'StockShortNavBeta']].astype(float)
        liquidityData[['FiveDaysIlliquidLongNav','FiveDaysIlliquidShortNav', 'TwoDaysIlliquidLongNav', 'TwoDaysIlliquidShortNav']] = liquidityData[['FiveDaysIlliquidLongNav','FiveDaysIlliquidShortNav', 'TwoDaysIlliquidLongNav', 'TwoDaysIlliquidShortNav']].astype(float)
        exposureUniData[['BondNetNav','BondGrossNav', 'EquityCategoryGrossNav', 'EquityCategoryGrossNavExcludeEQINDX_FT', 'FOLongNavValueBeta','FOShortNavBeta','FOLongNavNoBeta','FOShortNavNoBeta', 'OtherFOLongNavBeta','OtherFOShortNavBeta', 'OtherFOLongNavNoBeta', 'OtherFOShortNavNoBeta', 'OthersNetNav', 'EquityIndxFTLongNav','EquityIndxFTShortNav','NumberOfStock', 'StockLongNavBeta', 'StockShortNavBeta']] = exposureUniData[['BondNetNav','BondGrossNav', 'EquityCategoryGrossNav', 'EquityCategoryGrossNavExcludeEQINDX_FT', 'FOLongNavValueBeta','FOShortNavBeta','FOLongNavNoBeta','FOShortNavNoBeta', 'OtherFOLongNavBeta','OtherFOShortNavBeta', 'OtherFOLongNavNoBeta', 'OtherFOShortNavNoBeta', 'OthersNetNav', 'EquityIndxFTLongNav','EquityIndxFTShortNav','NumberOfStock', 'StockLongNavBeta', 'StockShortNavBeta']].fillna(0)
        liquidityData[['FiveDaysIlliquidLongNav','FiveDaysIlliquidShortNav', 'TwoDaysIlliquidLongNav', 'TwoDaysIlliquidShortNav']] = liquidityData[['FiveDaysIlliquidLongNav','FiveDaysIlliquidShortNav', 'TwoDaysIlliquidLongNav', 'TwoDaysIlliquidShortNav']].fillna(0)
        performanceData['Aum'] = performanceData['Aum'].astype(float)
        return exposureUniData, liquidityData, performanceData,new_team_info

    def calcFundLevelExposureData(self,  data, summaryLongShortData):
        fundData = data.copy()
        del fundData['StockLongNavBeta']
        del fundData['StockShortNavBeta']
        del fundData['BookCode']
        del fundData['InstrumentType']
        fundDataSummaryData = fundData.groupby(['FundCode']).sum()
        fundDataSummaryData = fundDataSummaryData.reset_index()
        fundDataSummaryData['BookCode'] = None
        fundDataSummaryData['InstrumentType'] = ''
        summaryLongShortData['BookCode'] = np.where(summaryLongShortData['BookCode']==summaryLongShortData['FundCode'], None,summaryLongShortData['BookCode'])
        fundDataSummaryData = pd.merge(fundDataSummaryData, summaryLongShortData[['FundCode', 'BookCode', 'StockLongNavBeta', 'StockShortNavBeta']], how='left', on=['FundCode', 'BookCode'])
        return pd.concat([data, fundDataSummaryData], axis=0,sort=True)

    def calcFundLevelLiquidityData(self,  data):
        fundData = data.copy()
        del fundData['BookCode']
        if 'Instrument' in fundData.columns:
            del fundData['Instrument']
        if 'Instrument_x' in fundData.columns:
            del fundData['Instrument_x']
        if 'Instrument_y' in fundData.columns:
            del fundData['Instrument_y']
        fundDataSummaryData = fundData.groupby(['FundCode']).sum()
        fundDataSummaryData = fundDataSummaryData.reset_index()
        fundDataSummaryData['BookCode'] =None
        fundDataSummaryData['Instrument'] = ''
        return pd.concat([data, fundDataSummaryData], axis=0,sort=True)

    def getPositionData(self, dateStr): #db_position
        sql = 'EXEC RiskDb.risk.usp_GetRiskPosition @asofdate = \''+dateStr+'\''
        return self.selectFromDB(sql)

    def getVarData(self, dateStr): #db_varrpt
        sql = 'select AsOfDate,Fund,FundId,Book,BookId,VaR,ETL,Correlation,ConfidenceLevel from RiskDb.risk.VaRView where AsOfDate = \''+dateStr+'\''
        data = self.selectFromDB(sql)
        data['FundCode'] = data['Fund']
        data['BookCode'] = data['Book']
        return data

    def getScenario(self, dateStr): #db_histscene
        sql = 'select * from RiskDb.risk.HistoricalScenarioView where AsOfDate = \''+dateStr+'\''
        return self.selectFromDB(sql)

    def getHighestYTDIntraday(self, dateStr): #db_highest_day
        noCount = 'SET NOCOUNT ON; '
        sql = 'EXEC RiskDb.risk.usp_get_mkv_by_max_ytdreturn @as_of_date= \''+dateStr+'\''
        data = self.selectFromDB(noCount+sql)
        highestYTDUniData  = data.groupby(['FundCode', 'BookCode']).agg({'Gross': 'sum'})
        highestYTDUniData = highestYTDUniData.reset_index()

        fund_highestYTDUniData = data[(data['BookCode']==None) | (data['BookCode'].isna())].copy()
        return pd.concat([highestYTDUniData, fund_highestYTDUniData], axis=0)

    def getSecificTeamLimit(self, dateStr):
        sql = 'SELECT FundId,BookId,FundCode,BookCode,LimitName,LimitValue,LimitDesc,LimitType,LimitStatus FROM RiskDb.ref.RiskControlSpecificTeamLimitInfo where BeginDate <=\''+dateStr+'\' AND EndDate >=\''+dateStr+'\''
        data = self.selectFromDataBase(sql)
        groupbyTeamData = data.groupby(['FundId', 'BookId', 'FundCode', 'BookCode', 'LimitName']).agg({'LimitValue': 'sum', 'LimitType': 'sum','LimitStatus':'sum'})
        groupbyTeamData = groupbyTeamData.reset_index()
        groupbyTeamData = groupbyTeamData.pivot_table(index=['FundId', 'BookId', 'FundCode', 'BookCode'], columns='LimitName', values='LimitValue', aggfunc='first').reset_index()
        return groupbyTeamData

    def saveSpecificTeamRiskStatus(self, dateStr, specificTeamRiskReportData):
        colsList = ['MRCGrossLeverageValue', 'MRCPV01Value', 'MRCNetNavValue', 'MRCVaRAt95LevelValue', 'CRCFXSpotNavCeilingValue', 'CRCCNYFXSpotNavCeilingValue', 'RRCBondMatureG10YValue' ,'RRCBondMatureLE10YG5YValue','RRCBondMatureLE5YG1YValue' ,'RRCFXSwapG1YValue','RRCFXSwapLE1YValue','LRCInvestTotalNoValue','SLRCPosMaxDDValue','SLRCMaxDDValue']
        statusList = ['MRCGrossLeverageValueStatus' ,'MRCPV01ValueStatus','MRCNetNavValueStatus','MRCVaRAt95LevelValueStatus','CRCFXSpotNavCeilingValueStatus','CRCCNYFXSpotNavCeilingValueStatus','RRCBondMatureG10YValueStatus','RRCBondMatureLE10YG5YValueStatus', 'RRCBondMatureLE5YG1YValueStatus', 'RRCFXSwapG1YValueStatus', 'RRCFXSwapLE1YValueStatus', 'LRCInvestTotalNoValueStatus', 'SLRCPosMaxDDValueStatus', 'SLRCMaxDDValueStatus']
        specificTeamRiskReportData['DateStr'] = dateStr
        allRecords = []
        for col in colsList:
            specificTeamRiskReportData['ColName'] = col
            specificTeamRiskReportData['ReportType'] = RiskCommonReportType.SPECIFIC_TEAM_REPORT.value
            specificTeamRiskReportData[col] = specificTeamRiskReportData[col].astype(float).round(6)
            recordsColumns = ['DateStr',  'FundId', 'BookId', 'FundCode', 'BookCode', 'ColName', col, col+'Status', 'ReportType']
            records = pdUtil.dataFrameToSavableRecords(specificTeamRiskReportData, recordsColumns)
            allRecords += records
        if allRecords:
            sql = 'insert into RiskDb.risk.RiskCommonReport(AsOfDate,FundId,BookId,FundCode,BookCode,ReportColName,ReportColValue,ReportColStatus,ReportType) values(?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, allRecords)

    def calcPerformanaExposureLiquidityStatus(self, dateStr, exposureData, liquidityData, performanceData,new_team_info):
        holdingLimitData = self.getRiskControlHoldingLimitData(dateStr)
        highestYTDIntraday = self.getHighestYTDIntraday(dateStr)
        stopLossLimitData = self.getRiskControlLossLimitData(dateStr)
        recoveryLimitData = self.getRiskControlRecoveryLimitData(dateStr)
        performanceData['MaxDd'] = np.where((performanceData['FundCode'].isin(['ZJNF', 'CACF'])) & (performanceData['BookCode'].isin(['AC','CC02','ZS01','ZX01','SL01','CC02','ZS01','ZX01','SL01'])), performanceData['RelReturnMaxDD'], performanceData['MaxDd'])
        performanceData['CurrDd'] = np.where((performanceData['FundCode'].isin(['ZJNF', 'CACF'])) & (performanceData['BookCode'].isin(['AC','CC02','ZS01','ZX01','SL01','CC02','ZS01','ZX01','SL01'])), performanceData['RelReturnCurDD'], performanceData['CurrDd'])
        uniData = pd.merge(performanceData, stopLossLimitData, how='left', on=['FundCode', 'BookCode'])
        uniData['RiskControlStopLossStatus1'] = np.where((uniData['MaxDd'] < uniData['StopLossLimit1']) & (uniData['CurrDd'] < uniData['StopLossLimit1']), RiskControlStatus.NEED_RISK_CONTROL.value, RiskControlStatus.PASS.value)
        uniData['RiskControlStopLossStatus2'] = np.where((uniData['MaxDd'] < uniData['StopLossLimit2']) & (uniData['CurrDd'] < uniData['StopLossLimit2']), RiskControlStatus.NEED_RISK_CONTROL.value, RiskControlStatus.PASS.value)
        uniData['RiskControlStopLossStatus3'] = np.where((uniData['MaxDd'] < uniData['StopLossLimit3']) & (uniData['CurrDd'] < uniData['StopLossLimit3']), RiskControlStatus.NEED_RISK_CONTROL.value, RiskControlStatus.PASS.value)

        uniData = pd.merge(uniData, recoveryLimitData, how='left', on=['FundCode','BookCode'])
        uniData['RiskControlRecoveryStatus1'] = np.where(uniData['RiskControlStopLossStatus1']==RiskControlStatus.PASS.value,
                                                         RiskControlStatus.PASS.value,
                                                         np.where(uniData['CurrDd'] < uniData['RecoveryLimit1'], RiskControlStatus.NEED_RISK_CONTROL.value,RiskControlStatus.PASS.value))
        uniData['RiskControlRecoveryStatus2'] = np.where(uniData['RiskControlStopLossStatus2']==RiskControlStatus.PASS.value,
                                                         RiskControlStatus.PASS.value,
                                                         np.where(uniData['CurrDd'] < uniData['RecoveryLimit2'], RiskControlStatus.NEED_RISK_CONTROL.value,RiskControlStatus.PASS.value))
        uniData['RiskControlRecoveryStatus3'] = np.where(uniData['RiskControlStopLossStatus3']==RiskControlStatus.PASS.value,
                                                         RiskControlStatus.PASS.value,
                                                         np.where(uniData['CurrDd'] < uniData['RecoveryLimit3'], RiskControlStatus.NEED_RISK_CONTROL.value,RiskControlStatus.PASS.value))
        uniData['RiskControlMDDStatus'] = np.where((uniData['RiskControlStopLossStatus1'] == RiskControlStatus.PASS.value) &
                                                   (uniData['RiskControlStopLossStatus2'] == RiskControlStatus.PASS.value) &
                                                   (uniData['RiskControlStopLossStatus3'] == RiskControlStatus.PASS.value) &
                                                   (uniData['RiskControlRecoveryStatus1'] == RiskControlStatus.PASS.value) &
                                                   (uniData['RiskControlRecoveryStatus2'] == RiskControlStatus.PASS.value) &
                                                   (uniData['RiskControlRecoveryStatus3'] == RiskControlStatus.PASS.value),
                                                   RiskControlStatus.PASS.value,
                                                   RiskControlStatus.NEED_RISK_CONTROL.value)
        uniData['RiskControlTotalNo'] = np.where(uniData['RiskControlStopLossStatus3'] == RiskControlStatus.NEED_RISK_CONTROL.value,RiskControlNoStatus.RISK_CONTROL_THIRD_TIME.value,
                                                 np.where(uniData['RiskControlStopLossStatus2'] == RiskControlStatus.NEED_RISK_CONTROL.value,RiskControlNoStatus.RISK_CONTROL_SEC_TIME.value,
                                                          np.where(uniData['RiskControlStopLossStatus1'] == RiskControlStatus.NEED_RISK_CONTROL.value,RiskControlNoStatus.RISK_CONTROL_FIRST_TIME.value,RiskControlStatus.PASS.value)))
        #uniData['RiskControlTotalNo'] = uniData['RiskControlTotalNo'].astype(str).astype(int)

        pendingMergePerformanceUniData = uniData[['FundCode','BookCode','RiskControlMDDStatus','RiskControlTotalNo','Aum']].copy()
        #pendingMergePerformanceUniData['BookCode'] = np.where(pendingMergePerformanceUniData['BookCode']=='',pendingMergePerformanceUniData['FundCode'], pendingMergePerformanceUniData['BookCode'])
        liquidityData = pd.merge(liquidityData, pendingMergePerformanceUniData, how='left', on=['FundCode', 'BookCode'])
        exposureData = pd.merge(exposureData, pendingMergePerformanceUniData, how='left', on=['FundCode', 'BookCode'])
        exposureUniData = pd.merge(exposureData, holdingLimitData, how='left', on=['FundCode','BookCode'])
        exposureUniData = pd.merge(exposureUniData, highestYTDIntraday, how='left', on=['FundCode','BookCode'])
        exposureUniData['Type'] = exposureUniData['Type'].astype(str)
        exposureUniData.dropna(subset=['Aum'], how='all', inplace=True)
        exposureUniData = exposureUniData[exposureUniData['Aum']!=0]
        exposureUniData['RiskControlTotalNo'] = exposureUniData['RiskControlTotalNo'].astype(int)
        exposureUniData['EquityLongBeta'] = np.where(((exposureUniData['FundCode'].isin(['SLHL'])) | (exposureUniData['BookCode'].isin(['T12','T34']))),exposureUniData['EquityLongNav']/ exposureUniData['Aum'],exposureUniData['EquityLongNavBeta']/ exposureUniData['Aum'])
        exposureUniData['EquityShortBeta'] = np.where(((exposureUniData['FundCode'].isin(['SLHL'])) | (exposureUniData['BookCode'].isin(['T12','T34']))),exposureUniData['EquityShortNav'] / exposureUniData['Aum'],exposureUniData['EquityShortNavBeta']  / exposureUniData['Aum'])
        exposureUniData['EquityLongNoBeta'] = exposureUniData['EquityLongNav'] / exposureUniData['Aum']
        exposureUniData['EquityShortNoBeta'] = exposureUniData['EquityShortNav'] / exposureUniData['Aum']
        exposureUniData['EquityGross'] = exposureUniData['EquityGrossNav'] / exposureUniData['Aum']
        exposureUniData['EquityCategoryGross'] = exposureUniData['EquityCategoryGrossNav']  / exposureUniData['Aum']
        exposureUniData['EquityCategoryGrossExcludeINDX_FT'] = exposureUniData['EquityCategoryGrossNavExcludeEQINDX_FT']  / exposureUniData['Aum']
        exposureUniData['Bond'] = exposureUniData['BondNetNav']  / exposureUniData['Aum']
        liquidityData = pd.merge(liquidityData, exposureUniData[['FundCode','BookCode','Bond']], how='left', on=['FundCode', 'BookCode'])

        exposureUniData['BondGross'] = exposureUniData['BondGrossNav']  / exposureUniData['Aum']
        exposureUniData['CategoryEquityFOLongBeta'] = exposureUniData['FOLongNavValueBeta'] / exposureUniData['Aum']
        exposureUniData['CategoryEquityFOShortBeta'] = exposureUniData['FOShortNavBeta'] / exposureUniData['Aum']
        exposureUniData['CategoryEquityFOLongNoBeta'] = exposureUniData['FOLongNavNoBeta'] / exposureUniData['Aum']
        exposureUniData['CategoryEquityFOShortNoBeta'] = exposureUniData['FOShortNavNoBeta'] / exposureUniData['Aum']
        exposureUniData['FOLongBeta'] = exposureUniData['FOLongNavValueBeta'] / exposureUniData['Aum']
        exposureUniData['FOShortBeta'] = exposureUniData['FOShortNavBeta']  / exposureUniData['Aum']
        exposureUniData['OtherFOLongBeta'] = exposureUniData['OtherFOLongNavBeta'] / exposureUniData['Aum']
        exposureUniData['OtherFOShortBeta'] = exposureUniData['OtherFOShortNavBeta']  / exposureUniData['Aum']
        exposureUniData['OtherFOLongNoBeta'] = exposureUniData['OtherFOLongNavNoBeta'] / exposureUniData['Aum']
        exposureUniData['OtherFOShortNoBeta'] = exposureUniData['OtherFOShortNavNoBeta'] / exposureUniData['Aum']
        exposureUniData['Others'] = exposureUniData['OthersNetNav'] / exposureUniData['Aum']
        exposureUniData['EquityIndxFTShort'] = exposureUniData['EquityIndxFTShortNav']  / exposureUniData['Aum']

        exposureUniData['EquityIndxFTShort'] = exposureUniData['EquityIndxFTShort'].fillna(0)



        ### for Fund Level
        exposureUniData['EquityLongBeta'] = np.where((exposureUniData['BookCode']==None) |(exposureUniData['BookCode'].isna()),
                                               exposureUniData['EquityLongBeta'],
                                               exposureUniData['EquityLongBeta'])
        exposureUniData['EquityShortBeta'] = np.where((exposureUniData['BookCode']==None) |(exposureUniData['BookCode'].isna()),
                                               exposureUniData['EquityShortNoBeta'],
                                               exposureUniData['EquityShortBeta'])
        exposureUniData['CategoryEquityFOLongBeta'] = np.where((exposureUniData['BookCode']==None) |(exposureUniData['BookCode'].isna()),
                                               exposureUniData['CategoryEquityFOLongNoBeta'],
                                               exposureUniData['CategoryEquityFOLongBeta'])
        exposureUniData['CategoryEquityFOShortBeta'] = np.where((exposureUniData['BookCode']==None) |(exposureUniData['BookCode'].isna()),
                                               exposureUniData['CategoryEquityFOShortNoBeta'],
                                               exposureUniData['CategoryEquityFOShortBeta'])
        exposureUniData['OtherFOLongBeta'] = np.where((exposureUniData['BookCode']==None) |(exposureUniData['BookCode'].isna()),
                                               exposureUniData['OtherFOLongNoBeta'],
                                               exposureUniData['OtherFOLongBeta'])
        exposureUniData['OtherFOShortBeta'] = np.where((exposureUniData['BookCode']==None) |(exposureUniData['BookCode'].isna()),
                                               exposureUniData['OtherFOShortNoBeta'],
                                               exposureUniData['OtherFOShortBeta'])



        #exposureUniData['NetBeta'] = exposureUniData['EquityLongBeta'] + exposureUniData['EquityShortBeta'] + exposureUniData['FOLongBeta'] + exposureUniData['FOShortBeta']
        exposureUniData['NetBeta'] = np.where((exposureUniData['BookCode'].isin(['W04'])) & (exposureUniData['FundCode'].isin(['CVF'])),
                                              exposureUniData['EquityLongBeta'] + exposureUniData['EquityShortBeta'] + exposureUniData['CategoryEquityFOLongBeta'] + exposureUniData['CategoryEquityFOShortBeta'] + exposureUniData['OtherFOLongBeta'] + exposureUniData['OtherFOShortBeta'],
                                              exposureUniData['EquityLongBeta'] + exposureUniData['EquityShortBeta'] + exposureUniData['CategoryEquityFOLongBeta'] + exposureUniData['CategoryEquityFOShortBeta']
                                              )

        exposureUniData['GrossMargin'] = np.where((exposureUniData['BookCode'].isin(['W07', 'W06', 'W04', 'W09'])) &
                                                  (exposureUniData['FundCode'].isin(['PMSF', 'CVF'])),
                                                  exposureUniData['EquityCategoryGross'] + 0.85 * exposureUniData['EquityIndxFTShort'] + exposureUniData['BondGross'] + 0.15 * (exposureUniData['OtherFOLongBeta'] - exposureUniData['OtherFOShortBeta']),
                                                  np.where(exposureUniData['BookCode'].isin(['T12', 'T34']), exposureUniData['EquityCategoryGrossExcludeINDX_FT'] + 0.15 * exposureUniData['EquityIndxFTShort'].abs() + exposureUniData['BondGross'], exposureUniData['EquityCategoryGross'] + 0.85 * exposureUniData['EquityIndxFTShort'] + exposureUniData['BondGross'])
                                                  )

        #exposureUniData['GrossMargin'] = exposureUniData['EquityCategoryGross'] + Decimal(0.85) * exposureUniData['EquityIndxFTShort'] + exposureUniData['BondGross'] + Decimal(0.15) * (exposureUniData['OtherFOLongBeta'] - exposureUniData['OtherFOShortBeta'])
        exposureUniData['GrossNotnlMV'] = exposureUniData['EquityLongNoBeta'] - exposureUniData['EquityShortNoBeta'] + exposureUniData['FOLongBeta'] - exposureUniData['FOShortBeta'] + exposureUniData['BondGross']
        exposureUniData['HistMDDGrossNotnlMV'] = exposureUniData['Gross']
        exposureUniData['A/B shs'] = exposureUniData['China'] / exposureUniData['Aum'].astype(float)
        exposureUniData['HK'] = exposureUniData['Hong Kong'] / exposureUniData['Aum'].astype(float)
        exposureUniData['US'] = exposureUniData['United States'] / exposureUniData['Aum'].astype(float)
        exposureUniData['Taiwan'] = exposureUniData['Taiwan'] / exposureUniData['Aum'].astype(float)
        exposureUniData['Australia'] = exposureUniData['Australia'] / exposureUniData['Aum'].astype(float)
        exposureUniData['Japan'] = exposureUniData['Japan'] / exposureUniData['Aum'].astype(float)
        exposureUniData['Korea(South)'] = exposureUniData['Korea (South)'] / exposureUniData['Aum'].astype(float)
        exposureUniData['India'] = exposureUniData['India'] / exposureUniData['Aum'].astype(float)
        exposureUniData['Singapore'] = exposureUniData['Singapore'] / exposureUniData['Aum'].astype(float)
        exposureUniData['OthersCountries'] = exposureUniData['Others'] / exposureUniData['Aum'].astype(float)
        #exposureUniData['FOLongBeta-FOShortBeta'] = np.where((exposureUniData['FOLongBeta'] == 0) & (exposureUniData['FOShortBeta'] == 0),0, exposureUniData['FOLongBeta']- exposureUniData['FOShortBeta'])
        exposureUniData['FOLongBeta-FOShortBeta'] = exposureUniData['FOLongBeta'] - exposureUniData['FOShortBeta']
        exposureUniData['FOLongBeta-FOShortBeta'] = exposureUniData['FOLongBeta-FOShortBeta'].astype(float).round(4)
        exposureUniData['GrossMargin'] = exposureUniData['GrossMargin'].astype(float).round(6)
        exposureUniData['DerivativeGross'] = np.where((exposureUniData['GrossMargin'].isna()) | (exposureUniData['GrossMargin'] == 0.00 ), 0, exposureUniData['FOLongBeta-FOShortBeta'] / exposureUniData['GrossMargin'])
        exposureUniData['GrossMargin'] =exposureUniData['GrossMargin']
        exposureUniData['NoOfStock'] = exposureUniData['NumberOfStock']
        exposureUniData['StockLong'] = exposureUniData['StockLongNavBeta'] / exposureUniData['Aum'].astype(float)
        exposureUniData['StockShort'] = exposureUniData['StockShortNavBeta'] / exposureUniData['Aum'].astype(float)


        ##
        #止损要求
        #"风控次數"
        #減至Net状态
        exposureUniData['RiskControlStopLossNetLimitStatus'] = np.where((exposureUniData['RiskControlMDDStatus'] == RiskControlStatus.NEED_RISK_CONTROL.value)
                                                                & ((exposureUniData['NetBeta'] >= exposureUniData['StopLossNetLimitCeiling'])
                                                                | (exposureUniData['NetBeta'] <= exposureUniData['StopLossNetLimitFloor'])),
                                                                RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                RiskControlStatus.PASS.value)

        ## Net 状态1
        exposureUniData['RiskControlHoldingNetLimitStatus'] = np.where(((exposureUniData['HoldingNetLimitCeiling'].isna()) | (exposureUniData['NetBeta'] <= exposureUniData['HoldingNetLimitCeiling']))
                                                                & ((exposureUniData['HoldingNetLimitFloor'].isna()) | (exposureUniData['NetBeta'] >= exposureUniData['HoldingNetLimitFloor'])),
                                                                RiskControlStatus.PASS.value,
                                                                RiskControlStatus.NEED_RISK_CONTROL.value)

        #exposureUniData.to_excel('C:\\temp\\T23_0423.xlsx', sheet_name='result')
        data_max_gross_notional = exposureUniData[exposureUniData['GrossMarginLimitType'] == RiskControlGrossMarginLimitType.DYNAMIC_LIMIT.value].copy()
                                                  #& (exposureUniData['RiskControlTotalNo'].isin([RiskControlNoStatus.RISK_CONTROL_THIRD_TIME.value, RiskControlNoStatus.RISK_CONTROL_SEC_TIME.value, RiskControlNoStatus.RISK_CONTROL_FIRST_TIME.value]))].copy()

        data_max_gross_notional.loc[:, ('FundBookCode')] = np.where((data_max_gross_notional['BookCode']==None) | (data_max_gross_notional['BookCode'].isna()),data_max_gross_notional.loc[:, ('FundCode')]+'-'+data_max_gross_notional.loc[:, ('FundCode')],data_max_gross_notional.loc[:, ('FundCode')]+'-'+data_max_gross_notional.loc[:, ('BookCode')])
        #data_max_gross_notional['FundBookCode'] = data_max_gross_notional['FundCode']+'-'+data_max_gross_notional['BookCode']
        fundBookCodeList = list(data_max_gross_notional['FundBookCode'].unique())
        max_gross_notional_data = self.getMaxGrossNotionalAfterMaxDD(dateStr,fundBookCodeList)
        exposureUniData = pd.merge(exposureUniData, max_gross_notional_data, how='left', on=['FundCode','BookCode'])
        exposureUniData['GrossMarginLimit'] = np.where(exposureUniData['GrossMarginLimitType'] == RiskControlGrossMarginLimitType.HARD_LIMIT.value,
                                                       exposureUniData['GrossMarginLimit'],
                                                       np.where(exposureUniData['RiskControlTotalNo']==RiskControlNoStatus.RISK_CONTROL_THIRD_TIME.value
                                                                ,exposureUniData['MaxGrossNotionalAfterMDD'] * Decimal(0.8**3),
                                                                np.where(exposureUniData['RiskControlTotalNo']==RiskControlNoStatus.RISK_CONTROL_SEC_TIME.value
                                                                         ,exposureUniData['MaxGrossNotionalAfterMDD'] * Decimal(0.8**2),
                                                                         np.where(exposureUniData['RiskControlTotalNo'] == RiskControlNoStatus.RISK_CONTROL_FIRST_TIME.value
                                                                                  ,exposureUniData['MaxGrossNotionalAfterMDD'] * Decimal(0.8),
                                                                                  np.nan)
                                                                         ))
                                                       )

        #exposureUniData.to_excel('C:\\temp\\GrossMarginLimit.xlsx', sheet_name='result')

        ##減至Gross	状态
        exposureUniData['RiskControlGrossMarginStatus'] = np.where(exposureUniData['GrossMarginLimitType'] == RiskControlGrossMarginLimitType.HARD_LIMIT.value,
                                                                   np.where((exposureUniData['RiskControlMDDStatus'] == RiskControlStatus.NEED_RISK_CONTROL.value)
                                                                            & (exposureUniData['GrossMargin'] >= exposureUniData['GrossMarginLimit']),
                                                                            RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                            RiskControlStatus.PASS.value),
                                                                   np.where(exposureUniData['GrossMarginLimitType'] == RiskControlGrossMarginLimitType.DYNAMIC_LIMIT.value,
                                                                            np.where(exposureUniData['GrossMarginLimit'].isna(), RiskControlStatus.PASS.value,
                                                                            np.where(exposureUniData['GrossNotnlMV'] < exposureUniData['GrossMarginLimit'],
                                                                                     RiskControlStatus.PASS.value,
                                                                                     RiskControlStatus.NEED_RISK_CONTROL.value)),RiskControlStatus.NO_APPLICABLE.value)
                                                                   )

        # Gross状态1
        exposureUniData['GrossLimitStatus'] = np.where(exposureUniData['GrossLimit'].isna(), RiskControlStatus.NO_APPLICABLE.value, np.where(exposureUniData['GrossMargin'] < exposureUniData['GrossLimit'], RiskControlStatus.PASS.value, RiskControlStatus.NEED_RISK_CONTROL.value))
        #exposureUniData.to_excel('C:\\temp\\W01.xlsx', sheet_name='result')

        ##A/B, HK, US, Other Countries
        ##2.市场持仓限制 - 净仓位

        exposureUniData['MarketHoldingLimitCN'] = np.where(exposureUniData['BookCode'].isin(['T28']), 0.2, exposureUniData['MarketHoldingLimit'])
        exposureUniData['MarketHoldingLimitStatus'] = np.where(exposureUniData['MarketHoldingLimit'].isna(),RiskControlStatus.NO_APPLICABLE.value,
                                                                np.where((exposureUniData['A/B shs'] < exposureUniData['MarketHoldingLimitCN'])
                                                               & (exposureUniData['HK'] < exposureUniData['MarketHoldingLimit'])
                                                               & (exposureUniData['US'] < exposureUniData['MarketHoldingLimit'])
                                                               & (exposureUniData['OthersCountries'] < exposureUniData['MarketHoldingLimit']),
                                                                RiskControlStatus.PASS.value,
                                                                RiskControlStatus.NEED_RISK_CONTROL.value))
        ## 3. 股票个数
        runDate = datetime.datetime.strptime(dateStr, '%Y-%m-%d')
        effective_date = datetime.datetime.strptime('2020-03-31', '%Y-%m-%d')
        exposureUniData['StockLimitStatus'] = np.where(exposureUniData['StockLimit'].isna(),RiskControlStatus.NO_APPLICABLE.value,
                                                        np.where(exposureUniData['NoOfStock'] < exposureUniData['StockLimit'],
                                                                 RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                 RiskControlStatus.PASS.value))
        ##T41(Yichi) T42(Dhawal) T43(Bryan) T44(Ajit) T46(Jack.Zhuo) T47(Zhou.Zou) T48(Bingliang.Yan) T49(Tiantao.Zheng) T50(Yu Hidema)
        if runDate >= effective_date:
            exposureUniData['StockLimitStatus'] = np.where(exposureUniData['BookCode'].isin(['T41','T42','T43','T44','T46','T47','T48','T49','T50']),
                                                        np.where(exposureUniData['NoOfStock'] < exposureUniData['StockLimit'], RiskControlStatus.NEED_RISK_CONTROL.value, RiskControlStatus.PASS.value),
                                                           exposureUniData['StockLimitStatus'])

        ##4. 债券总持仓
        exposureUniData['BondHoldingLimitStatus'] = np.where(exposureUniData['BondHoldingLimit'].isna(),RiskControlStatus.NO_APPLICABLE.value,
                                                             np.where(exposureUniData['Bond'] < exposureUniData['BondHoldingLimit'],
                                                                      RiskControlStatus.PASS.value,
                                                                      RiskControlStatus.NEED_RISK_CONTROL.value))
        ## 5. 衍生产品占资金比
        exposureUniData['DerivativeCashLimitStatus'] = np.where(exposureUniData['DerivativeCashLimit'].isna(), RiskControlStatus.NO_APPLICABLE.value,
                                                                np.where(exposureUniData['DerivativeGross'] < exposureUniData['DerivativeCashLimit'],
                                                                         RiskControlStatus.PASS.value,
                                                                         RiskControlStatus.NEED_RISK_CONTROL.value))
        ## 6. 单一个股 Long Short

        exposureUniData['StockLongShortLimitStatus'] = np.where(exposureUniData['StockLongLimit'].isna(), RiskControlStatus.NO_APPLICABLE.value,
                                                                np.where(exposureUniData['StockLong'] < exposureUniData['StockLongLimit'],
                                                                RiskControlStatus.PASS.value,
                                                                np.where(exposureUniData['StockShort'] < exposureUniData['StockShortLimit'], RiskControlStatus.PASS.value,RiskControlStatus.NEED_RISK_CONTROL.value)))
        # ((exposureUniData['RiskControlStopLossNetLimitStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (
        #             exposureUniData['RiskControlStopLossNetLimitStatus'] == RiskControlStatus.PASS.value))
        # &
        exposureUniData['RiskExposureTotalStatus'] = np.where(((exposureUniData['RiskControlStopLossNetLimitStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (exposureUniData['RiskControlStopLossNetLimitStatus'] == RiskControlStatus.PASS.value))
                                                                     & ((exposureUniData['RiskControlGrossMarginStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (exposureUniData['RiskControlGrossMarginStatus'] == RiskControlStatus.PASS.value))
                                                                     & ((exposureUniData['RiskControlHoldingNetLimitStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (exposureUniData['RiskControlHoldingNetLimitStatus'] == RiskControlStatus.PASS.value))
                                                                     & ((exposureUniData['GrossLimitStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (exposureUniData['GrossLimitStatus'] == RiskControlStatus.PASS.value))
                                                                     & ((exposureUniData['MarketHoldingLimitStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (exposureUniData['MarketHoldingLimitStatus'] == RiskControlStatus.PASS.value))
                                                                     & ((exposureUniData['StockLimitStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (exposureUniData['StockLimitStatus'] == RiskControlStatus.PASS.value))
                                                                     & ((exposureUniData['BondHoldingLimitStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (exposureUniData['BondHoldingLimitStatus'] == RiskControlStatus.PASS.value))
                                                                     & ((exposureUniData['DerivativeCashLimitStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (exposureUniData['DerivativeCashLimitStatus'] == RiskControlStatus.PASS.value))
                                                                     & ((exposureUniData['StockLongShortLimitStatus'] == RiskControlStatus.NO_APPLICABLE.value) | (exposureUniData['StockLongShortLimitStatus'] == RiskControlStatus.PASS.value)),
                                                                     RiskControlStatus.PASS.value,
                                                                     RiskControlStatus.NEED_RISK_CONTROL.value)

        #exposureUniData.to_excel('C:\\temp\\RiskReportTempData.xlsx', sheet_name='result')

        liquidityData.dropna(subset=['Aum'], how='all', inplace=True)
        liquidityData = liquidityData[liquidityData['Aum']!=0]
        liquidityData['TwoDaysIlliquidLongNavPorp'] = liquidityData['TwoDaysIlliquidLongNav'] / liquidityData['Aum']
        liquidityData['TwoDaysIlliquidShortNavPorp'] = liquidityData['TwoDaysIlliquidShortNav'] / liquidityData['Aum']
        liquidityData['TwoDaysIlliquidNavPorpTotal'] = liquidityData['TwoDaysIlliquidLongNavPorp'] + liquidityData['TwoDaysIlliquidShortNavPorp']

        liquidityData['FiveDaysIlliquidLongNavPorp'] = liquidityData['FiveDaysIlliquidLongNav'] / liquidityData['Aum']
        liquidityData['FiveDaysIlliquidShortNavPorp'] = liquidityData['FiveDaysIlliquidShortNav'] / liquidityData['Aum']
        liquidityData['FiveDaysIlliquidNavPorpTotal'] = liquidityData['FiveDaysIlliquidLongNavPorp'] + liquidityData['FiveDaysIlliquidShortNavPorp']

        liquidityData['REITSAssetNetNavPorp'] = liquidityData['REITSAssetNetNav'] / liquidityData['Aum']
        liquidityData['BondAssetNetNavPorp'] = liquidityData['Bond'] / 2

        liquidityData['TwoDaysIlliquidAssetStatus'] = np.where(liquidityData['TwoDaysIlliquidNavPorpTotal'] > 0.1, RiskControlStatus.NEED_RISK_CONTROL.value, RiskControlStatus.PASS.value)
        liquidityData['FiveDaysIlliquidAssetStatus'] = np.where(liquidityData['FiveDaysIlliquidNavPorpTotal'] > 0.04,
                                                               RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                np.where(liquidityData['FiveDaysIlliquidNavPorpTotal'] > 0, RiskControlStatus.NEED_REPORT.value, RiskControlStatus.PASS.value))
        liquidityData['LiquidityTotalStatus'] = np.where(((liquidityData['TwoDaysIlliquidAssetStatus'] == RiskControlStatus.PASS.value) | (liquidityData['TwoDaysIlliquidAssetStatus'] == RiskControlStatus.WAIVED.value))
                                                         & ((liquidityData['FiveDaysIlliquidAssetStatus'] == RiskControlStatus.NEED_REPORT.value)| (liquidityData['FiveDaysIlliquidAssetStatus'] == RiskControlStatus.PASS.value) | (liquidityData['FiveDaysIlliquidAssetStatus'] == RiskControlStatus.WAIVED.value)),
                                                         RiskControlStatus.PASS.value,
                                                         RiskControlStatus.NEED_RISK_CONTROL.value
                                                         )
        #liquidityData.to_excel('C:\\temp\\RiskReportliquidityTempData.xlsx', sheet_name='result')
        performanceColumns = ['DateStr', 'FundId', 'BookId', 'FundCode', 'BookCode', 'RiskControlStopLossStatus1', 'RiskControlStopLossStatus2','RiskControlStopLossStatus3','RiskControlRecoveryStatus1','RiskControlRecoveryStatus2','RiskControlRecoveryStatus3','RiskControlMDDStatus','RiskControlTotalNo']
        exposureColumns = ['DateStr', 'FundId', 'BookId', 'FundCode', 'BookCode', 'EquityLongBeta', 'EquityShortBeta', 'EquityLongNoBeta', 'EquityShortNoBeta','EquityGross','Bond','BondGross','BondGrossNav','FOLongBeta','FOShortBeta','OtherFOLongBeta','OtherFOShortBeta','Others','EquityIndxFTShort','NetBeta','GrossMargin','GrossNotnlMV','HistMDDGrossNotnlMV','A/B shs','HK','US','Taiwan','Australia','Japan','Korea(South)','India','Singapore','OthersCountries','DerivativeGross','NoOfStock','StockLong','StockShort','RiskControlStopLossNetLimitStatus','RiskControlHoldingNetLimitStatus','GrossMarginLimit','RiskControlGrossMarginStatus','GrossLimitStatus','MarketHoldingLimitStatus','StockLimitStatus','BondHoldingLimitStatus','DerivativeCashLimitStatus','StockLongShortLimitStatus','RiskExposureTotalStatus','MaxGrossNotionalAfterMDD','IsNew']
        liquidityColumns = ['DateStr', 'FundId', 'BookId', 'FundCode', 'BookCode', 'TwoDaysIlliquidLongNavPorp', 'TwoDaysIlliquidShortNavPorp', 'TwoDaysIlliquidNavPorpTotal','FiveDaysIlliquidLongNavPorp','FiveDaysIlliquidShortNavPorp','FiveDaysIlliquidNavPorpTotal','REITSAssetNetNavPorp','BondAssetNetNavPorp','TwoDaysIlliquidAssetStatus','FiveDaysIlliquidAssetStatus','LiquidityTotalStatus']
        #exposureUniData.to_excel('C:\\temp\\Local_exp.xlsx', sheet_name='result')
        uniData['DateStr'] = dateStr
        exposureUniData['DateStr'] = dateStr
        liquidityData['DateStr'] = dateStr

        fundInfoData = self.getFundInfo()
        bookInfoData = self.getBookInfo()
        del uniData['FundId']
        del uniData['BookId']
        commonNumericColumnsList = ['FundId', 'BookId']
        uniData = pd.merge(uniData, fundInfoData, how='left', on=['FundCode'])
        uniData = pd.merge(uniData, bookInfoData, how='left', on=['BookCode'])
        uniData = self.nanToNone(uniData, commonNumericColumnsList)

        uniData.drop_duplicates(subset=['Date','FundCode', 'BookCode'], inplace=True, keep='first')
        performanceRecords = pdUtil.dataFrameToSavableRecords(uniData, performanceColumns)


        exposureFloatColumnsList = [ 'EquityLongBeta', 'EquityShortBeta', 'EquityLongNoBeta', 'EquityShortNoBeta','EquityGross','Bond','BondGross','BondGrossNav','FOLongBeta','FOShortBeta','OtherFOLongBeta','OtherFOShortBeta','Others','EquityIndxFTShort','NetBeta','GrossMargin','GrossNotnlMV','HistMDDGrossNotnlMV','A/B shs','HK','US','Taiwan','Australia','Japan','Korea(South)','India','Singapore','OthersCountries','DerivativeGross','NoOfStock','StockLong','StockShort','GrossMarginLimit','MaxGrossNotionalAfterMDD']
        exposureUniData[exposureFloatColumnsList] = exposureUniData[exposureFloatColumnsList].astype(float)
        exposureUniData[exposureFloatColumnsList] = exposureUniData[exposureFloatColumnsList].round(6)
        exposureUniData = pd.merge(exposureUniData, fundInfoData, how='left', on=['FundCode'])
        exposureUniData = pd.merge(exposureUniData, bookInfoData, how='left', on=['BookCode'])

        exposureUniData = self.nanToNone(exposureUniData, exposureFloatColumnsList)
        exposureUniData = self.nanToNone(exposureUniData, commonNumericColumnsList)

        exposureUniData['FundBook'] = exposureUniData['FundCode'] +'-'+ exposureUniData['BookCode']
        exposureUniData = pd.merge(exposureUniData, new_team_info, how='left', on=['FundBook'])
        exposureUniData['IsNew'] = np.where(exposureUniData['IsNew'].isna() & ~exposureUniData['BookCode'].isna(),1,exposureUniData['IsNew'])
        exposureUniData = self.nanToNone(exposureUniData, ['IsNew'])
        exposureUniData.drop_duplicates(subset=['DateStr','FundBook'], inplace=True, keep='first')
        exposureRecords = pdUtil.dataFrameToSavableRecords(exposureUniData, exposureColumns)


        liquidityFloatColumnList =[ 'TwoDaysIlliquidLongNavPorp', 'TwoDaysIlliquidShortNavPorp', 'TwoDaysIlliquidNavPorpTotal','FiveDaysIlliquidLongNavPorp','FiveDaysIlliquidShortNavPorp','FiveDaysIlliquidNavPorpTotal','REITSAssetNetNavPorp','BondAssetNetNavPorp']
        liquidityData[liquidityFloatColumnList] = liquidityData[liquidityFloatColumnList].astype(float)
        liquidityData[liquidityFloatColumnList] = liquidityData[liquidityFloatColumnList].round(6)
        liquidityData = pd.merge(liquidityData, fundInfoData, how='left', on=['FundCode'])
        liquidityData = pd.merge(liquidityData, bookInfoData, how='left', on=['BookCode'])
        liquidityData = self.nanToNone(liquidityData,liquidityFloatColumnList)
        liquidityData = self.nanToNone(liquidityData, commonNumericColumnsList)

        liquidityData.drop_duplicates(subset=['DateStr','BookCode','FundCode'], inplace=True, keep='first')
        liquidityRecords = pdUtil.dataFrameToSavableRecords(liquidityData, liquidityColumns)

        #liquidityData.to_excel('C:\\temp\\liquidityData.xlsx', sheet_name='result')
        #exposureUniData.to_excel('C:\\temp\\exposureUniData.xlsx', sheet_name='result')

        self.cleanRunDateData(dateStr)
        try:
            self.saveExposureRecords(exposureRecords)
            self.savePerformanceRecords(performanceRecords)
            self.saveLiquidityRecords(liquidityRecords)
        except Exception, e:
            logging.error('error while saving risk report,' + e.message+e.args[1])
            raise Exception('error while saving risk report,' + e.message+e.args[1])

    def cleanRunDateData(self,dateStr):
        try:
            sql1= 'delete from RiskDb.risk.RiskExposureReport where AsOfDate=\''+dateStr+'\''
            sql2= 'delete from RiskDb.risk.RiskPerformanceReport where AsOfDate=\''+dateStr+'\''
            sql3= 'delete from RiskDb.risk.RiskLiquidityReport where AsOfDate=\''+dateStr+'\''
            self.cursor.execute(sql1)
            self.cursor.execute(sql2)
            self.cursor.execute(sql3)
        except Exception, e:
            logging.error('error while cleaning run date data,' + e.message+e.args[1])
            raise Exception('error while cleaning run date data,' + e.message+e.args[1])


    def savePerformanceRecords(self, performanceRecords):
        if performanceRecords:
            sql = 'insert into RiskDb.risk.RiskPerformanceReport(AsOfDate,FundId, BookId, FundCode,BookCode,RiskControlStopLossStatus1,RiskControlStopLossStatus2,RiskControlStopLossStatus3,RiskControlRecoveryStatus1,RiskControlRecoveryStatus2,RiskControlRecoveryStatus3,RiskControlMDDStatus,RiskControlTotalNo) values(?,?,?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql,performanceRecords)
        else:
            logging.warn('savePortfolioConstValues: empty record')


    def nanToNone(self, df_data, columnList):
        for column in columnList:
            df_data[column] = np.where(df_data[column].isna(), None,df_data[column])
        return df_data

    def saveLiquidityRecords(self, liquidityRecords):
        if liquidityRecords:
            sql = 'insert into RiskDb.risk.RiskLiquidityReport(AsOfDate,FundId, BookId, FundCode,BookCode,TwoDaysIlliquidLongNavPorp,TwoDaysIlliquidShortNavPorp,TwoDaysIlliquidNavPorpTotal,FiveDaysIlliquidLongNavPorp,FiveDaysIlliquidShortNavPorp,FiveDaysIlliquidNavPorpTotal,REITSAssetNetNavPorp,BondAssetNetNavPorp,TwoDaysIlliquidAssetStatus,FiveDaysIlliquidAssetStatus,LiquidityTotalStatus) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, liquidityRecords)
        else:
            logging.warn('savePortfolioConstValues: empty record')

    def saveExposureRecords(self, exposureRecords):
        if exposureRecords:
            sql = 'insert into RiskDb.risk.RiskExposureReport(AsOfDate,FundId, BookId,FundCode,BookCode,EquityLongBeta,EquityShortBeta,EquityLongNoBeta,EquityShortNoBeta,EquityGross,Bond,BondGross,BondGrossNav,FOLongBeta,FOShortBeta,OtherFOLongBeta,OtherFOShortBeta,Others,EquityIndxFTShort,NetBeta,GrossMargin,GrossNotnlMV,HistMDDGrossNotnlMV,ChinaMarket,HKMarket,USMarket,TaiwanMarket,AustraliaMarket,JapanMarket,SourthKRMarket,IndiaMarket,SingaporeMarket,OthersMarket,DerivativeGross,NoOfStock,StockLong,StockShort,RiskControlStopLossNetLimitStatus,RiskControlHoldingNetLimitStatus,GrossMarginLimit,RiskControlGrossMarginStatus,GrossLimitStatus,MarketHoldingLimitStatus,StockLimitStatus,BondHoldingLimitStatus,DerivativeCashLimitStatus,StockLongShortLimitStatus,RiskExposureTotalStatus, MaxGrossNotionalAfterMDD, IsNew) '

            sql +='values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, exposureRecords)
        else:
            logging.warn('savePortfolioConstValues: empty record')

    def reportGeneration(self):
        return 0

    def runWithDateRange(self, dateStr):
        self.initSqlServer(self.env)

        '''
        1. 补充 exposure数据: fund level 的Max MV数据
        '''
        riskMaxAbsValue = RiskMaxAbsValue(self.env)
        #riskMaxAbsValue.runWithDateRange(dateStr)

        '''
           2. all teams reports
        '''
        #fundList = ['PCF']
        #teamList = ['W05']
        fundList = []
        teamList = []
        (exposureData, liquidityData, performanceData,new_team_info) = self.getExposureData(dateStr, fundList, teamList)
        self.calcPerformanaExposureLiquidityStatus(dateStr, exposureData, liquidityData, performanceData,new_team_info)
        self.reportGeneration()
        '''
           pmsf cvf compare  - balancing report
        '''
        pmsfCvfPosition = PmsfCvfPosition()
        pmsfCvfPosition.run()

        '''
           1.run specific team reports first 
        '''
        riksControlReportPMSFT05 = RiksControlReportPMSFT05(self.env, 'PMSF', 'T05')
        riksControlReportPMSFT05.run()

        riksControlReportPMSFT22 = RiksControlReportPMSFT22(self.env, 'PMSF', 'T22')
        riksControlReportPMSFT22.run()

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
    riskControlReports = RiskControlReports(env)
    #riskControlReports.initSqlServer(env)
    #riskControlReports.getMaxGrossNotionalAfterMaxDD('2019-01-01','CVF','T34')
    #riskControlReports.closeSqlServerConnection()
    # riskControlReports.initSqlServer('prod')
    # data = riskControlReports.getRiskControlHoldingLimitData('2019-03-11')
    # data2 = riskControlReports.getRiskControlRecoveryLimitData('2019-03-15')
    # data3 = riskControlReports.getRiskControlLossLimitData('2019-03-15')
    # riskControlReports.closeSqlServerConnection()
    riskControlReports.runWithDateRange('2020-09-18')


