# encoding:UTF-8
from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import pandas as pd
import logging
from benchmark.marketdata.MarketDataDownloader import *
from benchmark.base.CommonEnums import RiskControlStatus
from benchmark.base.CommonEnums import RiskCommonReportType
import numpy as np
import datetime
import json
from decimal import *
getcontext().prec = 6

class RiksControlReportPMSFT22(Base):

    def __init__(self, env, fund, book):
        self.env = env
        self.team = book
        self.fund = fund
        LogManager('RiksControlReportPMSFT05')

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

    def getSecificTeamLimit(self, dateStr):
        sql = 'SELECT Id, FundId,BookId,FundCode,BookCode,LimitName,LimitValue,LimitDesc,LimitType,LimitStatus FROM RiskDb.ref.RiskControlSpecificTeamLimitInfo where BeginDate <=\''+dateStr+'\' AND EndDate >=\''+dateStr+'\' and FundCode=\''+self.fund+'\' and BookCode=\''+self.team+'\''
        data = self.selectFromDataBase(sql)
        data['TeamLimitId'] = data['Id']
        del data['Id']
        #test = data['LimitName'].str.split(pat='_')[0]
        data['LimitSection'] = data['LimitName'].str.split(pat = '_').str[0]
        data['LimitName'] = data['LimitSection'] +'_'+data['LimitName'].str.split(pat = '_').str[1]
        groupbyTeamData = data.groupby(['FundId', 'BookId', 'FundCode', 'BookCode', 'LimitName', 'LimitSection']).agg({'LimitValue': 'sum', 'LimitType': 'sum','LimitStatus':'sum'})
        groupbyTeamData = groupbyTeamData.reset_index()
        groupbyTeamData = groupbyTeamData.pivot_table(index=['FundId', 'BookId', 'FundCode', 'BookCode'], columns='LimitName', values='LimitValue', aggfunc='first').reset_index()

        limitNameList = data['LimitName'].tolist()
        return data, groupbyTeamData, limitNameList

    def getPerformanceData(self, dateStr):  # db_performance
        sql = 'EXEC RiskDb.risk.usp_excelGetPerformance @asofdate = \'' + dateStr + '\''
        data = self.selectFromDB(sql)
        data = data[(data['FundCode'] == self.fund) & (data['BookCode'] == self.team)]
        return data

    def getVarData(self, dateStr):  # db_varrpt
        sql = 'select AsOfDate,Fund,FundId,Book,BookId,VaR,ETL,Correlation,ConfidenceLevel from RiskDb.risk.VaRView where AsOfDate = \'' + dateStr + '\''
        data = self.selectFromDB(sql)
        data['FundCode'] = data['Fund']
        data['BookCode'] = data['Book']
        data = data[(data['FundCode'] == self.fund) & (data['BookCode'] == self.team)]
        return data

    def getExposureData(self, dateStr): #db_exposure
        sql='select * from RiskDb.risk.ExposureView where Instrument != \'Sector\' and Instrument != \'Market\' and AsOfDate = \''+dateStr+'\''
        data = self.selectFromDB(sql)
        data['FundCode'] = data['Fund']
        data['BookCode'] = data['Book']
        data = data[(data['FundCode'] == self.fund) & (data['BookCode'] == self.team)]
        return data

    def prepareTeamSpecificExposureData(self, dateStr):
        varData =self.getVarData(dateStr)
        performanceData = self.getPerformanceData(dateStr)
        exposureData = self.getExposureData(dateStr)

        filterNACategoryData = exposureData[~exposureData['Category'].isna()].copy()
        groupedByNotEmtpyCategoryData = filterNACategoryData.groupby(['FundCode', 'BookCode']).agg({'GrossNav': 'sum'})
        groupedByNotEmtpyCategoryData = groupedByNotEmtpyCategoryData.reset_index()
        groupedByNotEmtpyCategoryData['CategoryNotEmptyGrossNAV'] = groupedByNotEmtpyCategoryData['GrossNav']
        del groupedByNotEmtpyCategoryData['GrossNav']

        groupByInstrumentData = exposureData.groupby(['FundCode', 'BookCode', 'Instrument']).agg({'GrossNav': 'sum', 'LongNavBeta': 'sum', 'ShortNavBeta': 'sum', 'NetNav': 'sum'})
        groupByInstrumentData = groupByInstrumentData.reset_index()


        CCYInsData = groupByInstrumentData[groupByInstrumentData['Instrument'] == 'CCY'].copy()
        CCYInsData = CCYInsData.groupby(['FundCode', 'BookCode']).agg({'GrossNav': 'sum', 'NetNav': 'sum'})
        CCYInsData = CCYInsData.reset_index()
        CCYInsData['CCYInstrumentGrossNav'] = CCYInsData['GrossNav']
        CCYInsData['CCYInstrumentNetNav'] = CCYInsData['NetNav']
        del CCYInsData['GrossNav']
        del CCYInsData['NetNav']

        numberOfIssuerData = groupByInstrumentData[groupByInstrumentData['Instrument'] == 'N_Issuer'].copy()
        numberOfIssuerData = numberOfIssuerData.groupby(['FundCode', 'BookCode']).agg({'GrossNav': 'sum'})
        numberOfIssuerData = numberOfIssuerData.reset_index()
        numberOfIssuerData['NoOfIssuer'] = numberOfIssuerData['GrossNav']
        del numberOfIssuerData['GrossNav']

        if not CCYInsData.empty:
            specificTeamRiskReportData = pd.merge(groupedByNotEmtpyCategoryData, CCYInsData[
                ['FundCode', 'BookCode', 'CCYInstrumentGrossNav', 'CCYInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData = groupedByNotEmtpyCategoryData.copy()
            specificTeamRiskReportData['CCYInstrumentGrossNav'] = 0
            specificTeamRiskReportData['CCYInstrumentNetNav'] = 0

        if not numberOfIssuerData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData,numberOfIssuerData[['FundCode', 'BookCode', 'NoOfIssuer']],
                                                  how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['NoOfIssuer'] = 0

        specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, varData[['FundCode', 'BookCode', 'VaR']],
                                              how='left', on=['FundCode', 'BookCode'])
        specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, performanceData[
            ['FundCode', 'BookCode', 'MaxDd', 'CurrDd', 'HistHigh', 'Aum']], how='left', on=['FundCode', 'BookCode'])

        return specificTeamRiskReportData


    def saveSpecificTeamRiskStatus(self, dateStr, specificTeamRiskReportData, teamLimitRawData, teamNameList):
        specificTeamRiskReportData['DateStr'] = dateStr
        specificTeamRiskReportData['ReportType'] = RiskCommonReportType.SPECIFIC_TEAM_REPORT.value
        allRecords = []
        for limitName in teamNameList:
            specificTeamRiskReportData['ColSection'] = limitName.split('_')[0]
            specificTeamRiskReportData['ColName'] = specificTeamRiskReportData['ColSection'] + '_' + limitName.split('_')[1] + '_Value'
            specificTeamRiskReportData['ReportColLimitId'] = teamLimitRawData[(teamLimitRawData['LimitName'] == limitName)]['TeamLimitId'].iloc[0]
            specificTeamRiskReportData[limitName+'_Value'] = specificTeamRiskReportData[limitName+'_Value'].astype(float).round(6)


            recordsColumns = ['DateStr',  'FundId', 'BookId', 'FundCode', 'BookCode', 'ColSection', 'ColName', 'ReportColLimitId', limitName+'_Value', limitName+'_Value_Status','ReportType']
            records = pdUtil.dataFrameToSavableRecords(specificTeamRiskReportData, recordsColumns)
            allRecords += records
        if allRecords:
            self.removeTeamReport(dateStr, 'T22')
            sql = 'insert into RiskDb.risk.RiskCommonReport(AsOfDate,FundId,BookId,FundCode,BookCode,ReportColSection,ReportColName,ReportColLimitId,ReportColValue, ReportColLimitStatus,ReportType) values(?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, allRecords)

    def removeTeamReport(self, dateStr, teamCode):
        sql = 'delete from RiskDb.risk.RiskCommonReport where BookCode=\''+teamCode+'\' and AsOfDate=\''+dateStr+'\' and ReportType='+str(RiskCommonReportType.SPECIFIC_TEAM_REPORT.value)
        self.cursor.execute(sql)

    def calcSpecificTeamRiskStatus(self, dateStr, specificTeamRiskReportData):
        (teamLimitRawData, teamLimitData, limitNameList) = self.getSecificTeamLimit(dateStr)

        specificTeamRiskReportData['MRC_GrossLeverageCeiling_Value'] = specificTeamRiskReportData['CategoryNotEmptyGrossNAV'] / specificTeamRiskReportData['Aum']
        specificTeamRiskReportData['CRC_FXSpotNavCeiling_Value'] = specificTeamRiskReportData['CCYInstrumentGrossNav'] / specificTeamRiskReportData['Aum']
        specificTeamRiskReportData['CRC_FXSpotNetNavCeiling_Value'] = specificTeamRiskReportData['CCYInstrumentNetNav'] / specificTeamRiskReportData['Aum']
        specificTeamRiskReportData['LRC_InvestTotalNo_Value'] = specificTeamRiskReportData['NoOfIssuer']
        specificTeamRiskReportData['SLRC_PosMaxDD_Value'] = specificTeamRiskReportData['MaxDd']
        specificTeamRiskReportData['SLRC_MaxDD_Value'] =specificTeamRiskReportData['CurrDd']

        specificTeamRiskReportData = pd.merge(teamLimitData, specificTeamRiskReportData, how='left', on=['FundCode','BookCode'])
        specificTeamRiskReportData['MRC_GrossLeverageCeiling_Value_Status'] = np.where(specificTeamRiskReportData['MRC_GrossLeverageCeiling_Value'] > specificTeamRiskReportData['MRC_GrossLeverageCeiling'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['CRC_FXSpotNavCeiling_Value_Status'] = np.where(specificTeamRiskReportData['CRC_FXSpotNavCeiling_Value'] > specificTeamRiskReportData['CRC_FXSpotNavCeiling'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['CRC_FXSpotNetNavCeiling_Value_Status'] = np.where(specificTeamRiskReportData['CRC_FXSpotNetNavCeiling_Value'] > specificTeamRiskReportData['CRC_FXSpotNetNavCeiling'],
                                                                                    RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                    RiskControlStatus.PASS.value)
        specificTeamRiskReportData['LRC_InvestTotalNo_Value_Status'] = np.where(specificTeamRiskReportData['LRC_InvestTotalNo_Value'] > specificTeamRiskReportData['LRC_InvestTotalNo'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['SLRC_PosMaxDD_Value_Status'] = np.where(specificTeamRiskReportData['SLRC_PosMaxDD_Value'] < specificTeamRiskReportData['SLRC_PosMaxDD'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['SLRC_MaxDD_Value_Status'] = np.where(specificTeamRiskReportData['SLRC_MaxDD_Value'] < specificTeamRiskReportData['SLRC_MaxDD'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        return (teamLimitRawData, specificTeamRiskReportData, limitNameList)

    def runWithDateRange(self,dateStr):
        self.initSqlServer(self.env)
        specificTeamRiskReportData = self.prepareTeamSpecificExposureData(dateStr)
        (teamLimitRawData, specificTeamRiskReportResultData, limitNameList) = self.calcSpecificTeamRiskStatus(dateStr, specificTeamRiskReportData)
        self.saveSpecificTeamRiskStatus(dateStr, specificTeamRiskReportResultData, teamLimitRawData, limitNameList)
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
    riskControlReports = RiksControlReportPMSFT22(env, 'PMSF', 'T22')
    riskControlReports.runWithDateRange('2019-02-21')