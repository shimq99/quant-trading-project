# encoding:UTF-8
from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import pandas as pd
import logging
from benchmark.tools import PandasDBUtils as pdUtil
from benchmark.marketdata.MarketDataDownloader import *
from benchmark.base.CommonEnums import RiskExceptionSummaryReport
from benchmark.base.CommonEnums import RiskExceptionSummaryReportStatus
from benchmark.base.CommonEnums import RiskCommonReportType
from benchmark.base.CommonEnums import RiskControlStatus
from decimal import *
getcontext().prec = 6
from benchmark.risk_control_2019.RiskMaxAbsValue import *
from analysis.reports.HoldingEquityAnalysis import *
import datetime
from pytz import timezone
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from dateutil import relativedelta
from shutil import copyfile
import glob
from shutil import copyfile
import os

class RiskSummaryReports(Base):
    def __init__(self, env):
        self.env = env
        LogManager('RiskSummaryReports')

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

    def getEquityIPODateInfo(self):
        sql = 'select BbgTicker,CASE WHEN IPODate IS NULL THEN \'1900-01-01 00:00:00.000\' ELSE IPODate END as IPODate from RiskDb.risk.EquityGICSInfo'
        data = self.selectFromDataBase(sql)
        data['IPODate'] = pd.to_datetime(data['IPODate'])
        return data

    def getHoldingPosition(self, fundId, startDateStr, endDateStr):
        sql = 'SELECT S.ExternalInstClass,S.ExternalAssetClass,G2.SubIndustry as UnderlyingSubIndustry,S.UnderlyingBB_TCM,S.BB_TCM as Ticker,G.SubIndustry,PriceLocalStart,QuantityDirection,P.SecurityId,TxnSecurityId,CurrencyId,P.FundId,F.FundCode,P.BookId,B.BookCode,PositionTypeCode,StrategyCode,ExternalContractId,Date FROM Portfolio.pos.Position P ' \
              'left join SecurityMaster.sm.SecurityView S on P.SecurityId=S.SecurityId ' \
              'left join RiskDb.risk.EquityGICSInfo G on G.BbgTicker=S.BB_TCM COLLATE database_default ' \
              'left join RiskDb.risk.EquityGICSInfo G2 on G2.BbgTicker=S.UnderlyingBB_TCM COLLATE database_default ' \
              'left join RiskDb.ref.Book B on B.BookId=P.BookId left join RiskDb.ref.Fund F on F.FundId=P.FundId ' \
              'where Date between \''+startDateStr+'\' and \''+endDateStr+'\' and QuantityDirection in (\'LONG\',\'SHORT\') and P.FundId ='+str(fundId)+' and PositionTypeCode = \'TRD\' and S.ExternalAssetClass in (\'EQTY\',\'FUTURE\',\'OPTION\')'
        data = self.selectFromDataBase(sql)
        return data

    ##RS.RestrictedRuleId=2 先看global restriction
    ##IsExclusive=1 表示禁止做（禁止买入卖空）
    def getRestrictedSecurity(self):
        sql = 'SELECT RS.RestrictedRuleId ,Ticker,RR.Code,RR.AssetClass,RR.Market,RR.Code,RR.IsExclusive,RR.QuantityDirection ' \
              'FROM OMS.dbo.RestrictedSecurity RS left join oms.dbo.RestrictedRule RR on RR.RestrictedRuleId=RS.RestrictedRuleId ' \
              'WHERE RR.IsActive=1 and RS.RestrictedRuleId=2 and RR.IsExclusive=1 '
        data = self.selectFromDataBase(sql)
        return data

    def getGICSDef(self):
        sql = 'SELECT Sector,SectorName,Industry,IndustryName,IndustryGroup,IndustryGroupName,SubIndustry,SubIndustryName ' \
              'FROM RiskDb.ref.IndustrySectorInfo'
        data = self.selectFromDataBase(sql)
        return data

    def loadCountryRestrictionData(self, filePath):
        data = pd.read_csv(filePath)
        data['CountryCode'] = data['Unnamed: 0']
        del data['Unnamed: 0']
        return data

    def loadGICSRestrictionData(self, filePath):
        data = pd.read_csv(filePath)
        data['SubIndustry'] = data['Unnamed: 0']
        del data['Unnamed: 0']
        return data

    def loadExemptionRestrictionData(self, filePath):
        data = pd.read_csv(filePath)
        return data

    def loadGICSInfoFromBBG(self):
        self.initSqlServer(self.env)
        data = pd.read_excel('C:\\devel\\GICSs\\GICS_UK_20190704.xlsx', dtype={'GICS SubInd': str,'GICS Ind':str,'GICS Ind Grp':str,'GICS Sector':str})
        data.dropna(subset=['GICS Sector'], how='all', inplace=True)
        records = pdUtil.dataFrameToSavableRecords(data, ['Ticker', 'GICS SubInd', 'GICS Ind', 'GICS Ind Grp','GICS Sector'])
        sql='insert into RiskDb.risk.EquityGICSInfo(BbgTicker, SubIndustry, Industry, IndustryGroup, Sector)  values(?, ?, ?, ?, ?)'
        self.insertToDatabase(sql,records)
        self.closeSqlServerConnection()

    def getRiskIndexTradableLimit(self):
        sql = 'SELECT Id,Team,IndexCode as TradableIndexCode,IsTradable,TeamType FROM RiskDb.ref.RiskIndexTradableLimit'
        data = self.selectFromDataBase(sql)
        data = data.groupby(['Team']).agg({'TradableIndexCode': lambda x:  list(x),'TeamType':'mean'})
        #data = data.groupby(['Team']).agg({'TradableIndexCode': lambda x:  ','.join(x),'TeamType':'mean'})
        data = data.reset_index()
        #test = data['TradableIndexCode'][1]
        #data['test']= 'SH000905 Index'
        #data['test'] = np.where(data['TradableIndexCode'].str.contains(data['test'].str), 1, 0)
        #data['result'] = data.apply(self.isin,axis=1)
        return data
    #
    # def isin(self,x):
    #     return x['test'] in x.TradableIndexCode

    def getPerformanceData(self, startDateStr, endDatestr, fundCodeList):
        sql='select A.FundId, A.BookId,A.UnitGross,A.Aum,A.QdAum, A.Date, C.FundCode, D.BookCode, A.MtdGrossReturn, A.YtdNetReturn, A.YtdGrossReturn, A.GrossExposure, A.NetExposure,  B.AnnRet, B.AnnVol,B.MaxDd, B.Sharpe ' \
            'from Portfolio.perf.Nav A left join RiskDb.risk.Performance B on A.FundId=B.FundId and (A.BookId=B.BookId or A.BookId IS NULL and B.BookId IS NULL) and A.Date=B.AsOfDate ' \
            'left join RiskDb.ref.Fund C on A.FundId=C.FundId left join RiskDb.ref.Book D on A.BookId=D.BookId  where (A.Source=\'Settlement\' or A.Source=\'IT\' or A.Source=\'Marking\') and Date between \''+startDateStr+'\' and \''+endDatestr+'\' '
        if fundCodeList:
            sql += ' and FundCode in (\'' + ('\',\'').join(fundCodeList)+'\') '
        sql += ' order by FundCode,BookCode'
        data = self.selectFromDataBase(sql)
        return data

    def holdingPosAnalysis(self, fundId, startDateStr, endDateStr, gics_data, exemption_data, country_data, restricted_securities,ignore_data):
        pos_data = self.getHoldingPosition(fundId, startDateStr, endDateStr)
        pos_data['Ticker'] = np.where(pos_data['ExternalAssetClass'].isin(['FUTURE','OPTION']),pos_data['UnderlyingBB_TCM'],pos_data['Ticker'])
        #self.topBottom10Analysis(endDateStr,pos_data)
        tradable_index_data = self.getRiskIndexTradableLimit()
        pos_data['CountryCode'] =  pos_data['Ticker'].str.split(pat=' ').str[1]
        uni_data = pd.merge(pos_data[['Ticker','SubIndustry','CountryCode','FundId','FundCode','BookId','BookCode']], gics_data, how='left', on=['SubIndustry','BookCode'])
        uni_data = uni_data.rename(columns={'Status': 'GICS_Restriction_Status'})

        uni_data = pd.merge(uni_data, exemption_data, how='left', on=['Ticker','BookCode'])
        uni_data = uni_data.rename(columns={'Status': 'Exemption_Status'})

        uni_data = pd.merge(uni_data, country_data, how='left', on=['CountryCode','BookCode'])
        uni_data = uni_data.rename(columns={'Status': 'Country_Restriction_Status'})

        uni_data = pd.merge(uni_data, restricted_securities[['Ticker','Status']], how='left', on=['Ticker'])
        uni_data = uni_data.rename(columns={'Status': 'Ban_Security_Status'})

        #uni_data['BookCode_Prefix'] = uni_data['BookCode'].str.split(pat='').str[0]
        #test = uni_data['BookCode'][0]

        gics_exception_data = uni_data[(uni_data['GICS_Restriction_Status']==0) & (uni_data['Exemption_Status']!=1)]
        country_exception_data = uni_data[(uni_data['Country_Restriction_Status']==0) & (uni_data['Exemption_Status']!=1)]
        global_restrict_exception_data = uni_data[uni_data['Ban_Security_Status']==0]
        savableCols = ['Date', 'FundId', 'FundCode', 'BookId', 'BookCode', 'Ticker', 'CountryCode', 'SubIndustry',
                       'SubIndustryName', 'GICS_Restriction_Status', 'Country_Restriction_Status', 'Exemption_Status',
                       'Ban_Security_Status', 'Reason','Type']
        intCols = ['SubIndustryName','GICS_Restriction_Status', 'Country_Restriction_Status', 'Exemption_Status', 'Ban_Security_Status']
        records = []
        if not gics_exception_data.empty:
            gics_exception_data = pdUtil.nanToNone(gics_exception_data, intCols)
            gics_exception_data.loc[:, ('Date')] = endDateStr
            gics_exception_data.loc[:, ('Reason')]  = 'Industry: ' + gics_exception_data['SubIndustryName']+' not in GICS list'
            gics_exception_data.loc[:, ('Type')] = RiskExceptionSummaryReport.NOT_IN_GICS_LIST.value

            gics_exception_data = pd.merge(gics_exception_data,
                                           ignore_data[['FundCode', 'BookCode', 'Ticker', 'Type', 'IgnoreStatus']],
                                           how='left', on=['FundCode', 'BookCode', 'Ticker', 'Type'])
            gics_exception_data=gics_exception_data[gics_exception_data['IgnoreStatus']!=1]
            records += pdUtil.dataFrameToSavableRecords(gics_exception_data,savableCols)

        if not country_exception_data.empty:
            country_exception_data = pdUtil.nanToNone(country_exception_data, intCols)
            country_exception_data.loc[:, ('Date')] = endDateStr
            country_exception_data.loc[:, ('Reason')] = 'Country: ' + country_exception_data['CountryCode']+' not in country list'
            country_exception_data.loc[:, ('Type')] = RiskExceptionSummaryReport.NOT_IN_COUNTRY_LIST.value

            country_exception_data = pd.merge(country_exception_data,
                                           ignore_data[['FundCode', 'BookCode', 'Ticker', 'Type', 'IgnoreStatus']],
                                           how='left', on=['FundCode', 'BookCode', 'Ticker', 'Type'])
            country_exception_data=country_exception_data[country_exception_data['IgnoreStatus']!=1]
            records += pdUtil.dataFrameToSavableRecords(country_exception_data,savableCols)

        if not global_restrict_exception_data.empty:
            global_restrict_exception_data = pdUtil.nanToNone(global_restrict_exception_data, intCols)
            global_restrict_exception_data.loc[:, ('Date')] = endDateStr
            global_restrict_exception_data.loc[:, ('Reason')] =  'Ticker: ' + global_restrict_exception_data['Ticker']+' in global restricted list'
            global_restrict_exception_data.loc[:, ('Type')] = RiskExceptionSummaryReport.GLOBAL_RESTRICTED.value

            global_restrict_exception_data = pd.merge(global_restrict_exception_data,
                                           ignore_data[['FundCode', 'BookCode', 'Ticker', 'Type', 'IgnoreStatus']],
                                           how='left', on=['FundCode', 'BookCode', 'Ticker', 'Type'])
            global_restrict_exception_data=global_restrict_exception_data[global_restrict_exception_data['IgnoreStatus']!=1]
            records += pdUtil.dataFrameToSavableRecords(global_restrict_exception_data,savableCols)


        index_pos_data = uni_data[(~uni_data['Ticker'].isna()) & (uni_data['Ticker'].str.endswith('Index'))].copy()
        if not index_pos_data.empty:
            fund_tradable_index_data=tradable_index_data[tradable_index_data['TeamType']==0].copy()
            fund_tradable_index_data['FundCode']=fund_tradable_index_data['Team']
            team_tradable_index_data=tradable_index_data[tradable_index_data['TeamType']==1].copy()
            team_tradable_index_data['BookCode'] = team_tradable_index_data['Team']
            team_tradable_index_data['TeamTradableIndexCode'] = team_tradable_index_data['TradableIndexCode']
            match_team_tradable_index_data=tradable_index_data[tradable_index_data['TeamType']==2].copy()
            match_team_tradable_index_data['BookCode_Prefix'] = match_team_tradable_index_data['Team']
            match_team_tradable_index_data['TeamPrefixTradableIndexCode'] = match_team_tradable_index_data['TradableIndexCode']
            index_pos_data['BookCode_Prefix'] = index_pos_data['BookCode'].astype(str).str[0]


            ##检查fund级别是否有可交易的Index
            uni_index_pos_data = pd.merge(index_pos_data, fund_tradable_index_data[['FundCode', 'TradableIndexCode']], how='left', on=['FundCode'])
            uni_index_pos_data['TradableIndexCode'] = np.where(uni_index_pos_data['TradableIndexCode'].isna(),'',uni_index_pos_data['TradableIndexCode'])
            uni_index_pos_data['FundLevelTradableIndexStatus'] = np.where(uni_index_pos_data.apply(lambda x: 'ALL' in x['TradableIndexCode'], axis=1), 1, 0)
            uni_index_pos_data['FundLevelTradableIndexStatus'] = np.where(uni_index_pos_data.apply(lambda x: x['Ticker'] in x['TradableIndexCode'], axis=1),1,uni_index_pos_data['FundLevelTradableIndexStatus'])
            uni_index_pos_data = uni_index_pos_data[uni_index_pos_data['FundLevelTradableIndexStatus']==0]


            if not uni_index_pos_data.empty:
                ##检查book级别是否对于某个字母开头的book有可交易的index： 如W, T
                uni_index_pos_data = pd.merge(uni_index_pos_data, match_team_tradable_index_data[['BookCode_Prefix', 'TeamPrefixTradableIndexCode']], how='left', on=['BookCode_Prefix'])
                uni_index_pos_data['TeamPrefixTradableIndexCode'] = np.where(uni_index_pos_data['TeamPrefixTradableIndexCode'].isna(),'',uni_index_pos_data['TeamPrefixTradableIndexCode'])
                uni_index_pos_data['TeamPrefixLevelTradableIndexStatus'] = np.where(uni_index_pos_data.apply(lambda x: 'ALL' in x['TeamPrefixTradableIndexCode'], axis=1), 1, 0)
                uni_index_pos_data['TeamPrefixLevelTradableIndexStatus'] = np.where(uni_index_pos_data.apply(lambda x: x['Ticker'] in x['TeamPrefixTradableIndexCode'], axis=1),1,uni_index_pos_data['TeamPrefixLevelTradableIndexStatus'])
                uni_index_pos_data = uni_index_pos_data[uni_index_pos_data['TeamPrefixLevelTradableIndexStatus'] == 0]

            if not uni_index_pos_data.empty:
                ##检查具体book是否有可交易的index
                uni_index_pos_data = pd.merge(uni_index_pos_data, team_tradable_index_data[['BookCode', 'TeamTradableIndexCode']], how='left', on=['BookCode'])
                uni_index_pos_data['TeamTradableIndexCode'] = np.where(uni_index_pos_data['TeamTradableIndexCode'].isna(),'',uni_index_pos_data['TeamTradableIndexCode'])
                uni_index_pos_data['TeamLevelTradableIndexStatus'] = np.where(uni_index_pos_data.apply(lambda x: 'ALL' in x['TeamTradableIndexCode'], axis=1), 1, 0)
                uni_index_pos_data['TeamLevelTradableIndexStatus'] = np.where(uni_index_pos_data.apply(lambda x: x['Ticker'] in x['TeamTradableIndexCode'], axis=1), 1, uni_index_pos_data['TeamLevelTradableIndexStatus'])
                uni_index_pos_data = uni_index_pos_data[uni_index_pos_data['TeamLevelTradableIndexStatus'] == 0]

            cols = uni_index_pos_data.columns
            if 'TeamLevelTradableIndexStatus' not in cols:
                uni_index_pos_data['TeamLevelTradableIndexStatus'] = np.nan
            if 'TeamPrefixLevelTradableIndexStatus' not in cols:
                uni_index_pos_data['TeamPrefixLevelTradableIndexStatus'] = np.nan

            invalid_index_pos_data = uni_index_pos_data[(uni_index_pos_data['FundLevelTradableIndexStatus']==0) | (uni_index_pos_data['TeamLevelTradableIndexStatus']==0) | (uni_index_pos_data['TeamPrefixLevelTradableIndexStatus']==0)].copy()


            invalid_index_pos_data['Reason'] = 'Team/Fund not allow to trade on Index:'+invalid_index_pos_data['Ticker']
            invalid_index_pos_data['Type'] = RiskExceptionSummaryReport.INDEX_NOT_ALLOW_TO_TRADE.value
            invalid_index_pos_data['Status'] = RiskExceptionSummaryReportStatus.FAILED.value
            invalid_index_pos_data['Date'] = endDateStr

            invalid_index_pos_data = pd.merge(invalid_index_pos_data,
                                           ignore_data[['FundCode', 'BookCode', 'Ticker', 'Type', 'IgnoreStatus']],
                                           how='left', on=['FundCode', 'BookCode', 'Ticker', 'Type'])
            invalid_index_pos_data=invalid_index_pos_data[invalid_index_pos_data['IgnoreStatus']!=1]

            db_cols = ['AsOfDate', 'FundId', 'FundCode', 'BookId', 'BookCode', 'Ticker', 'Reason', 'Type', 'Status']
            record_cols = ['Date', 'FundId', 'FundCode', 'BookId', 'BookCode', 'Ticker', 'Reason', 'Type', 'Status']
            self.saveRiskExceptionSummaryReportWithDbCol(db_cols, record_cols, invalid_index_pos_data)

        if records:
            sql = 'insert into RiskDb.risk.RiskExceptionSummaryReport(AsOfDate,FundId,FundCode,BookId,BookCode,' \
                  'Ticker,CountryCode,SubIndustry,SubIndustryName,GicsRestrictionStatus, CountryRestrictionStatus, ' \
                  'ExemptionStatus, GlobalRestrictedStatus, Reason,Type) ' \
                  'values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql,records)

    def saveTopBottom10Data(self,data,dbColList,valueColList):
        ##self.saveTopBottom10Data(valid_holding_data,
        ### ['AsOfDate','FundCode','ReportColSection','ReportColName','ReportColValue','ReportType'],
        ### ['Date','Fund (shrt)','ReportColSection','Identifier(Agg)','MVPct','ReportType'])
        fundCodeList = list(data['Fund (shrt)'].unique())
        allRecords = []
        for fundCode in fundCodeList:
            fund_data = data[data['Fund (shrt)']==fundCode].copy()
            fund_data['MVPct'] = fund_data['MVPct'].astype(float).round(6)
            fund_data.sort_values('MVPct', ascending=False, inplace=True)
            top10_data = fund_data.iloc[0:10].copy()
            top10_data['ReportType'] = RiskCommonReportType.TOP_BOTTOM_TEN_EQUITY.value
            allRecords += pdUtil.dataFrameToSavableRecords(top10_data, valueColList)
            bottom10_data = fund_data.iloc[-10:].copy()
            bottom10_data['ReportType'] = RiskCommonReportType.TOP_BOTTOM_TEN_EQUITY.value
            allRecords += pdUtil.dataFrameToSavableRecords(bottom10_data, valueColList)
        if allRecords:
            sql = 'insert into RiskDb.risk.RiskCommonReport(' + (',').join(dbColList) + ') values(' + ('?,' * (len(dbColList)))[:-1] + ')'
            self.insertToDatabase(sql, allRecords)

    def saveRiskExceptionSummaryReport(self,cols, data):
        records = pdUtil.dataFrameToSavableRecords(data, cols)
        if records:
            sql = 'insert into RiskDb.risk.RiskExceptionSummaryReport(AsOfDate,FundId,FundCode,Ticker,Reason,Type,Status) ' \
                  'values(?,?,?,?,?,?,?)'
            self.insertToDatabase(sql,records)

    def removeRiskExceptionSummaryReport(self,dateStr):
        sql = 'delete from RiskDb.risk.RiskExceptionSummaryReport where AsOfDate=\''+dateStr+'\''
        self.cursor.execute(sql)

        reportTypes = [RiskCommonReportType.TOP_BOTTOM_TEN_EQUITY.value,RiskCommonReportType.PMSF_CVF_LEVERAGE.value,RiskCommonReportType.QDII_UNIT_GROSS.value,RiskCommonReportType.A_SHARES_MAX_PCT.value]
        sql2 = 'delete from RiskDb.risk.RiskCommonReport where AsOfDate=\''+dateStr+'\' and ReportType in ('+(',').join(str(x) for x in reportTypes)+')'
        self.cursor.execute(sql2)

    def saveRiskExceptionSummaryReportWithDbCol(self, db_cols, cols, data):
        records = pdUtil.dataFrameToSavableRecords(data, cols)
        if records:
            sql = 'insert into RiskDb.risk.RiskExceptionSummaryReport(' + (',').join(db_cols) + ') values(' + ('?,' * (len(db_cols)))[:-1] + ')'
            self.insertToDatabase(sql,records)

    def saveQDIIReport(self, QDII_data):
        QDII_data['ReportType'] = RiskCommonReportType.QDII_UNIT_GROSS.value
        records = pdUtil.dataFrameToSavableRecords(QDII_data, ['Date', 'FundId', 'FundCode','BookId','BookCode', 'UnitCost', 'UnitGross', 'Status', 'ReportType'])
        self.saveCommonReport(records, ['AsOfDate', 'FundId', 'FundCode', 'BookId', 'BookCode', 'ReportColName', 'ReportColValue', 'ReportColLimitStatus', 'ReportType'])

    def getRiskPosition(self, dateStr):  # db_position
        sql = 'EXEC RiskDb.risk.usp_GetRiskPosition @Date = \'' + dateStr + '\''
        return self.selectFromDataBase(sql)

    def getFund(self):
        sql = 'SELECT FundId, FundCode FROM RiskDb.ref.Fund'
        data = self.selectFromDataBase(sql)
        return data

    def getRealFund(self):
        sql = 'SELECT FundId, FundCode FROM RiskDb.ref.Fund where IsActive=1 and IsReal=1'
        data = self.selectFromDataBase(sql)
        return data

    def getBook(self):
        sql = 'SELECT BookId, BookCode FROM RiskDb.ref.Book'
        data = self.selectFromDataBase(sql)
        return data

    def saveAShareMaxPctStatus(self, data):
        data['FundId'] = 0
        data['FundCode'] = ''
        records = pdUtil.dataFrameToSavableRecords(data, ['Date','FundId','FundCode','Book','Identifier(Agg)','SecurityId','SharePct','ReportType'])
        self.saveCommonReport(records, ['AsOfDate','FundId','FundCode','BookCode','ReportColSection','ReportColName','ReportColValue','ReportType'])

    def savePMSFCVFLeverageStatus(self, data):
        records = pdUtil.dataFrameToSavableRecords(data, ['Date','FundId','FundCode','Aum','Aum_SumOfTeam','Leverage_Status','ReportType'])
        self.saveCommonReport(records,['AsOfDate','FundId','FundCode','ReportColName','ReportColValue','ReportColLimitStatus','ReportType'])

    def saveCommonReport(self, allRecords, dbColList):
        if allRecords:
            sql = 'insert into RiskDb.risk.RiskCommonReport(' + (',').join(dbColList) + ') values(' + ('?,' * (len(dbColList)))[:-1] + ')'
            self.insertToDatabase(sql, allRecords)


    def getShares(self, dateStr):
        sql = 'SELECT * from  [MarketData].[mark].[ufn_latest_analytics](\''+dateStr+'\',7)'
        market_data = self.selectFromDataBase(sql)
        return market_data[['SecurityID','SHARE']]

    '''
    国内监管要求，所有基金占股数不能超过A股流通股的15%
    
    内部10%以上提示
    '''
    def outstandingSharesPct(self, dateStr):
        other_equities_data  = pd.read_excel('./Open-End_Funds.xlsx')
        other_equities = list(other_equities_data['Ticker'].unique())
        pos_data = self.getRiskPosition(dateStr)
        pos_data = pos_data[pos_data['Fund (shrt)'].isin(['PMSF','CVF','DCL','PCF','PLUS','SLHL','ZJNF'])]
        pos_data = pos_data[pos_data['Identifier(Agg)'].str.endswith(' CH Equity')]
        f = lambda x: tuple(x)
        grouped_data = pos_data.groupby(['Identifier(Agg)','SecurityId']).agg({'Posn': 'sum','Book':lambda x: list(x)})
        grouped_data = grouped_data.reset_index()
        grouped_data['HoldingShares'] = grouped_data['Posn']/1000000 ## million
        market_data = self.getShares(dateStr)
        market_data['SecurityId'] = market_data['SecurityID']
        grouped_data = pd.merge(grouped_data, market_data[['SecurityId', 'SHARE']], how='left', on=['SecurityId'])
        grouped_data['SharePct'] = grouped_data['HoldingShares']/grouped_data['SHARE']
        grouped_data=grouped_data[grouped_data['SharePct']>0.10]
        grouped_data['Included'] = np.where(grouped_data[''].isin(other_equities),'NO','YES')
        grouped_data = grouped_data[grouped_data['Included']=='YES']
        grouped_data['ReportType'] = RiskCommonReportType.A_SHARES_MAX_PCT.value
        grouped_data['Date'] = dateStr
        grouped_data['Book'] = grouped_data['Book'].apply(', '.join)
        grouped_data['SharePct'] = grouped_data['SharePct'].astype(float).round(6)
        self.saveAShareMaxPctStatus(grouped_data)

    def topIlliquidAnalysis(self, dateStr, endDateStr):
        pos_data = self.getRiskPosition(dateStr)

        equity_inst_class_code = ['REITS','NON_REITS','PRFD','CFD_EQUITY','DR','EQTY_FT']
        equity_pos_data = pos_data[pos_data['Inst Class (Code)'].isin(equity_inst_class_code)].copy()
        equity_pos_data.sort_values('minLiqVol_102030', ascending=True, inplace=True)

        equity_pos_data['ReportType'] = RiskCommonReportType.ILLIQUID_ASSET.value




        # irsw_inst_class_code = ['IR_SW']
        # irsw_pos_data = pos_data[pos_data['Inst Class (Code)'].isin(irsw_inst_class_code)].copy()
        # test = 'RM_Maturity' - dateStr
        # irsw_pos_data['RM_Maturity'] = pd.to_datetime(irsw_pos_data['RM_Maturity'])
        # irsw_pos_data['Current_Date'] = pd.to_datetime(irsw_pos_data['Current_Date'])
        #
        # irsw_pos_data['C'] = (irsw_pos_data['RM_Maturity'] - irsw_pos_data['Current_Date']).dt.days
        # irsw_pos_data.sort_values('minLiqVol_102030', ascending=True, inplace=True)
        #
        #
        # fwrdfx_inst_class_code = ['FWRD_FX']
        # fwrdfx_pos_data = pos_data[pos_data['Inst Class (Code)'].isin(fwrdfx_inst_class_code)].copy()
        # fwrdfx_pos_data.sort_values('minLiqVol_102030', ascending=True, inplace=True)
        #
        # bondft_inst_class_code = ['BOND_FT']
        # bondft_pos_data = pos_data[pos_data['Inst Class (Code)'].isin(bondft_inst_class_code)].copy()
        # bondft_pos_data.sort_values('minLiqVol_102030', ascending=True, inplace=True)
        #
        # govt_inst_class_code = ['GOVT']
        # govt_pos_data = pos_data[pos_data['Inst Class (Code)'].isin(govt_inst_class_code)].copy()
        # govt_pos_data.sort_values('minLiqVol_102030', ascending=True, inplace=True)
        #
        # irft_inst_class_code = ['IR_FT']
        # irft_pos_data = pos_data[pos_data['Inst Class (Code)'].isin(irft_inst_class_code)].copy()
        # irft_pos_data.sort_values('minLiqVol_102030', ascending=True, inplace=True)




    def saveIlliquidResult(self, data):
        records = pdUtil.dataFrameToSavableRecords(data, ['Date','FundId','FundCode','Aum','Aum_SumOfTeam','Leverage_Status','ReportType'])
        self.saveCommonReport(records,['AsOfDate','FundId','FundCode','ReportColName','ReportColValue','ReportColLimitStatus','ReportType'])

    def topBottom10Analysis(self, dateStr, data):
        pos_data = self.getRiskPosition(dateStr)
        holding_data=pos_data.copy()
        fund_info = self.getFund()
        fund_info['Fund (shrt)'] = fund_info['FundCode']
        validFundList = ['PMSF','CVF','SLHL','PLUS','DCL','ZJNF']
        holding_data = pd.merge(holding_data, fund_info[['Fund (shrt)', 'FundId']], how='left', on=['Fund (shrt)'])
        holding_data = holding_data[(holding_data['Fund (shrt)'].isin(validFundList)) & (holding_data['Identifier(Agg)'].str.endswith('Equity'))]
        performance_data = self.getPerformanceData(dateStr,dateStr,validFundList)
        team_performance_data = performance_data[(~performance_data['BookCode'].isna()) & (~performance_data['BookCode'].isin(['GTJA','GUOTOURUIYIN','GUANGFA']))].copy()
        team_performance_data = team_performance_data.groupby(['FundId','FundCode']).agg({'Aum': 'sum'})
        team_performance_data = team_performance_data.reset_index()
        team_performance_data['Aum_SumOfTeam'] = team_performance_data['Aum']
        del team_performance_data['Aum']
        fund_performance_data = performance_data[performance_data['BookCode'].isna()].copy()
        all_perf_data = pd.merge(team_performance_data, fund_performance_data[['FundCode', 'Aum','QdAum']], how='left', on=['FundCode'])
        all_perf_data['Aum'] =  np.where(all_perf_data['FundCode'].isin(['PMSF','SLHL','PLUS','DCL','ZJNF']), all_perf_data['Aum'].astype(float),
                                                 np.where(all_perf_data['FundCode']=='CVF', all_perf_data['Aum'].astype(float)-all_perf_data['QdAum'].astype(float), 0))
        all_perf_data['Aum_SumOfTeam'] = all_perf_data['Aum_SumOfTeam'].astype(float)
        all_perf_data['Aum_Leverage'] = np.where(all_perf_data['FundCode'].isin(['PMSF','PLUS','DCL','ZJNF']), all_perf_data['Aum']*3.5-all_perf_data['Aum_SumOfTeam'],
                                                 np.where(all_perf_data['FundCode']=='CVF', all_perf_data['Aum']*2.5-all_perf_data['Aum_SumOfTeam'],np.where(all_perf_data['FundCode']=='SLHL', all_perf_data['Aum']*2-all_perf_data['Aum_SumOfTeam'],0)))
        all_perf_data['Aum_Leverage_Ratio'] = all_perf_data['Aum_SumOfTeam']/all_perf_data['Aum']
        all_perf_data['Leverage_Status'] = np.where(all_perf_data['FundCode']=='PMSF', np.where(all_perf_data['Aum_Leverage_Ratio']>3.95,RiskControlStatus.NEED_RISK_CONTROL.value, RiskControlStatus.PASS.value),
                                                 np.where(all_perf_data['FundCode']=='CVF', np.where(all_perf_data['Aum_Leverage_Ratio']>3.45,RiskControlStatus.NEED_RISK_CONTROL.value, RiskControlStatus.PASS.value), RiskControlStatus.UNKNOWN.value))
        all_perf_data['Date'] = dateStr
        all_perf_data['ReportType'] = RiskCommonReportType.PMSF_CVF_LEVERAGE.value
        if not all_perf_data.empty:
            self.savePMSFCVFLeverageStatus(all_perf_data)

        QDII_data = performance_data[(performance_data['FundCode']=='CVF') & (performance_data['BookCode']=='GTJA')].copy()
        if not QDII_data.empty:
            QDII_data['UnitCost'] = 1
            QDII_data['Status'] = np.where(QDII_data['UnitGross']/QDII_data['UnitCost']<0.901, RiskControlStatus.NEED_RISK_CONTROL.value,np.where(QDII_data['UnitGross']/QDII_data['UnitCost']<0.921, RiskControlStatus.NEED_REPORT.value,RiskControlStatus.PASS.value))
            self.saveQDIIReport(QDII_data)


        QDII_data = performance_data[(performance_data['FundCode']=='CVF') & (performance_data['BookCode']=='GUOTOURUIYIN')].copy()
        if not QDII_data.empty:
            QDII_data['UnitCost'] = 1
            QDII_data['Status'] = np.where(QDII_data['UnitGross']/QDII_data['UnitCost']<0.901, RiskControlStatus.NEED_RISK_CONTROL.value,np.where(QDII_data['UnitGross']/QDII_data['UnitCost']<0.921, RiskControlStatus.NEED_REPORT.value,RiskControlStatus.PASS.value))
            self.saveQDIIReport(QDII_data)

        QDII_data = performance_data[(performance_data['FundCode']=='CVF') & (performance_data['BookCode']=='GUANGFA')].copy()
        if not QDII_data.empty:
            QDII_data['UnitCost'] = 1
            QDII_data['Status'] = np.where(QDII_data['UnitGross']/QDII_data['UnitCost']<0.901, RiskControlStatus.NEED_RISK_CONTROL.value,np.where(QDII_data['UnitGross']/QDII_data['UnitCost']<0.921, RiskControlStatus.NEED_REPORT.value,RiskControlStatus.PASS.value))
            self.saveQDIIReport(QDII_data)
        invaliAssetClassCode=['FUTURE']
        validInstClassCode = ['NON_REITS','PRFD','CFD_EQUITY','DR','EQTY_OP']
        valid_holding_data = holding_data[(holding_data['Fund (shrt)'].isin(validFundList)) | (holding_data['Inst Class (Code)'].isin(validInstClassCode))]
        valid_holding_data = valid_holding_data[~valid_holding_data['Asset Class (Code)'].isin(invaliAssetClassCode)]
        valid_holding_data['TeamCode']=valid_holding_data['Book'].str.split(pat='-').str[1]
        valid_holding_data=valid_holding_data[valid_holding_data['TeamCode']!='GTJA']
        valid_holding_data['Identifier(Agg)'] = valid_holding_data['Identifier(Agg)'].str.replace('C1 Equity', 'CH Equity', regex=False)
        valid_holding_data['Identifier(Agg)'] = valid_holding_data['Identifier(Agg)'].str.replace('C2 Equity', 'CH Equity', regex=False)
        valid_holding_data = valid_holding_data.groupby(['FundId','Fund (shrt)', 'Identifier(Agg)']).agg({'Notnl MV (USD)-delta adj-Option': 'sum'})
        valid_holding_data = valid_holding_data.reset_index()
        valid_holding_data['Date'] = dateStr
        valid_holding_data['Aum'] = 0
        for index, data_row in fund_performance_data.iterrows():
            fund = data_row['FundCode']
            aum = data_row['Aum']
            qdAum = data_row['QdAum']
            if fund=='CVF':
                aum = aum - qdAum
            valid_holding_data['Aum'] = np.where(valid_holding_data['Fund (shrt)'] == fund, aum, valid_holding_data['Aum'])
        valid_holding_data = valid_holding_data[valid_holding_data['Aum']!=0]
        valid_holding_data['MVPct'] = valid_holding_data['Notnl MV (USD)-delta adj-Option'] / valid_holding_data['Aum']
        valid_holding_data['RiskStatus'] = np.where(((valid_holding_data['MVPct']>0.035) | (valid_holding_data['MVPct']<-0.025)), RiskControlStatus.NEED_REPORT.value,RiskControlStatus.PASS.value)
        valid_holding_data.sort_values('MVPct', ascending=False, inplace=True)
        valid_holding_data['ReportColSection']='>3.5% / <-2.5% need alert'
        self.saveTopBottom10Data(valid_holding_data,['FundId','AsOfDate','FundCode','ReportColSection','ReportColName','ReportColValue','ReportType','ReportColLimitStatus'],['FundId','Date','Fund (shrt)','ReportColSection','Identifier(Agg)','MVPct','ReportType','RiskStatus'])
        alert_mv_data = valid_holding_data[(valid_holding_data['MVPct']>=0.04) | (valid_holding_data['MVPct']<=-0.025)].copy()
        alert_mv_data['Type']=RiskExceptionSummaryReport.TOPBOTTOM_TEN_ALERT.value
        alert_mv_data['Status'] = RiskExceptionSummaryReportStatus.FAILED.value
        alert_mv_data['Date'] = dateStr
        alert_mv_data['Reason'] = 'MV%:'+(alert_mv_data['MVPct']*100).astype(str)+'% on long/short ticker:'+alert_mv_data['Identifier(Agg)']+' for fund:'+alert_mv_data['Fund (shrt)']+' breach limit'
        self.saveRiskExceptionSummaryReport(['Date','FundId','Fund (shrt)','Identifier(Agg)','Reason','Type','Status'],alert_mv_data)

    def performanceAnalysis(self, currentDateStr):
        startDate = datetime.datetime.strptime(currentDateStr, '%Y-%m-%d')
        weekDay = startDate.date().weekday()
        diff = 1
        if (weekDay == 0):
            diff += 2
        previousDate = startDate - datetime.timedelta(days=diff)
        previousDateStr = previousDate.strftime('%Y-%m-%d')
        performance_data = self.getPerformanceData(previousDateStr,currentDateStr,[])
        performance_data.sort_values(by=['FundCode', 'BookCode'], ascending=True, inplace=True)
        #performance_data = performance_data.groupby(['FundCode', 'BookCode']).agg({'MtdGrossReturn': 'sum', 'YtdGrossReturn':'sum', 'MaxDd':'sum'})
        performance_data.dropna(subset=['FundCode'], how='all', inplace=True)
        performance_data.dropna(subset=['MtdGrossReturn'], how='all', inplace=True)
        performance_data.dropna(subset=['YtdGrossReturn'], how='all', inplace=True)
        count_data = performance_data.groupby(['FundCode', 'BookCode']).count()
        count_data = count_data.reset_index()
        count_data['FundBook'] = count_data['FundCode']+'-'+count_data['BookCode']
        lack_of_data_fundbook = list(count_data[count_data['YtdGrossReturn']<2]['FundBook'].unique())
        performance_data['FundBook'] = performance_data['FundCode'] +'-'+performance_data['BookCode']
        performance_data = performance_data[~performance_data['FundBook'].isin(lack_of_data_fundbook)]
        performance_data = performance_data.groupby(['FundId','BookId','FundCode', 'BookCode'])['MtdGrossReturn','YtdGrossReturn','MaxDd'].agg(lambda x: (x.iloc[-1] - x.iloc[0]))

        performance_data = performance_data.reset_index()

        maxdd_alert_data = performance_data[((performance_data['BookCode'].isna()) & ((performance_data['MaxDd']>=0.02) | (performance_data['MaxDd']<=-0.02)))
                                            | ((~performance_data['BookCode'].isna()) & ((performance_data['MaxDd']>=0.01) | (performance_data['MaxDd']<=-0.01)))].copy()
        maxdd_alert_data['Type']=RiskExceptionSummaryReport.MAXDD_ALERT.value
        maxdd_alert_data['Date'] = currentDateStr
        maxdd_alert_data['Reason'] = 'Fund/Team MaxDD change reach limit, actual:'+maxdd_alert_data['MaxDd'].astype(str)
        maxdd_alert_data['Status'] = RiskExceptionSummaryReportStatus.FAILED.value

        mtd_alert_data = performance_data[(performance_data['MtdGrossReturn']>=0.02) | (performance_data['MtdGrossReturn']<=-0.02)].copy()
        mtd_alert_data['Date'] = currentDateStr
        mtd_alert_data['Type'] = RiskExceptionSummaryReport.MTD_ALERT.value
        mtd_alert_data['Status'] = RiskExceptionSummaryReportStatus.FAILED.value
        mtd_alert_data['Reason'] = 'Fund/Team MTD change reach limit, actual:'+mtd_alert_data['MtdGrossReturn'].astype(str)

        ytd_alert_data = performance_data[(performance_data['YtdGrossReturn']>=0.02) | (performance_data['YtdGrossReturn']<=-0.02)].copy()
        ytd_alert_data['Date'] = currentDateStr
        ytd_alert_data['Type'] = RiskExceptionSummaryReport.YTD_ALERT.value
        ytd_alert_data['Status'] = RiskExceptionSummaryReportStatus.FAILED.value
        ytd_alert_data['Reason'] = 'Fund/Team YTD change > or < 2%, actual:'+ytd_alert_data['YtdGrossReturn'].astype(str)

        if not maxdd_alert_data.empty:
            db_cols = ['AsOfDate',  'FundId', 'FundCode', 'BookId','BookCode', 'Reason', 'Type', 'Status']
            record_cols = ['Date', 'FundId', 'FundCode', 'BookId','BookCode', 'Reason', 'Type', 'Status']
            self.saveRiskExceptionSummaryReportWithDbCol(db_cols, record_cols, maxdd_alert_data)

        if not mtd_alert_data.empty:
            db_cols = ['AsOfDate', 'FundId', 'FundCode', 'BookId', 'BookCode', 'Reason', 'Type', 'Status']
            record_cols = ['Date', 'FundId', 'FundCode', 'BookId', 'BookCode', 'Reason', 'Type', 'Status']
            self.saveRiskExceptionSummaryReportWithDbCol(db_cols, record_cols, mtd_alert_data)

        if not ytd_alert_data.empty:
            db_cols = ['AsOfDate', 'FundId', 'FundCode', 'BookId', 'BookCode', 'Reason', 'Type', 'Status']
            record_cols = ['Date', 'FundId', 'FundCode', 'BookId', 'BookCode', 'Reason', 'Type', 'Status']
            self.saveRiskExceptionSummaryReportWithDbCol(db_cols, record_cols, ytd_alert_data)


    def isinWithColName(self, data, col):
        return data[col] in data['TradableIndexCode']

    def isinWithValue(self, x, value):
        return value in x.TradableIndexCode

    def furtherProcessData(self, df, pivotCol):
        df.index=df[pivotCol]
        del df[pivotCol]
        ready_data = df.stack().reset_index()
        ready_data['BookCode'] =ready_data['level_1']
        ready_data['Status'] =ready_data[0]
        del ready_data['level_1']
        del ready_data[0]
        return ready_data

    def downloadIPODate(self, tickers):
        print tickers

    def checkAndFixBetaOverwrite(self, dateStr):
        data = self.getRiskPosition(dateStr)
        ipo_info = self.getEquityIPODateInfo()
        ipo_info['Identifier(Agg)'] = ipo_info['BbgTicker']
        data = data[data['Identifier(Agg)'].str.endswith('Equity')]
        data['Identifier(Agg)'] = np.where(data['Asset Class (Code)'].isin(['FUTURE','OPTION']),data['Uly Ident (BB_TCM)'],data['Identifier(Agg)'])
        data['Identifier(Agg)'] = data['Identifier(Agg)'].str.replace('C1 Equity', 'CH Equity', regex=False)
        data['Identifier(Agg)'] = data['Identifier(Agg)'].str.replace('C2 Equity', 'CH Equity', regex=False)
        uni_data = pd.merge(data, ipo_info, how='left', on=['Identifier(Agg)'])
        tickerlist = list(uni_data[uni_data['IPODate'].isna()]['Identifier(Agg)'].unique())
        if tickerlist:
            logging.info('tickers for IPO Date:'+'.'.join(tickerlist))
            mdService = MarketDataDownloader(self.env)
            mdService.initSqlServer(self.env)
            status = mdService.getIPODateFromBBG(tickerlist)
            #status = 'SUCCESS'
            mdService.closeSqlServerConnection()
            if status=='SUCCESS':
                new_ipo_info = self.getEquityIPODateInfo()
                new_ipo_info['Identifier(Agg)'] = new_ipo_info['BbgTicker']
                new_uni_data = pd.merge(data, new_ipo_info, how='left', on=['Identifier(Agg)'])
                self.fixBeta(new_uni_data)
            else:
                logging.error('ERROR： can not download IPO Date info from Bloomberg')
        else:
            self.fixBeta(uni_data)

    def getBetaOverwriteInfo(self):
        sql = 'SELECT A.SecurityID,ValueTypeCode,Value,FromDate,ToDate,Bottom,Discount,S.BB_TCM,s.UnderlyingBB_TCM  FROM MarketData.mark.AnalyticOverwrite A  left join SecurityMaster.sm.SecurityView S on A.SecurityID=S.SecurityId'
        return self.selectFromDataBase(sql)

    def getSecurityID(self, tickers):
        sql = 'select SecurityID,BB_TCM as [Identifier(Agg)] from SecurityMaster.sm.SecurityView where BB_TCM in (\'' + ('\',\'').join(tickers)+'\')'
        return self.selectFromDataBase(sql)

    def fixBeta(self, data):
        currentDate = datetime.datetime.now()
        data['BetaOverwriteStatus'] = np.where(data.apply(lambda x: ((x['mod_beta']>1) | (x['mod_beta']<-2)) & (relativedelta.relativedelta(currentDate, x['IPODate']).years==0) & (relativedelta.relativedelta(currentDate, x['IPODate']).months<3), axis=1),'SHOULD_OVERWRITE','SHOULD_NOT_OVERWRITE')
        existing_overwrite_data = self.getBetaOverwriteInfo()
        existing_tickers = list(existing_overwrite_data['BB_TCM'].unique())
        existing_tickers += list(existing_overwrite_data['UnderlyingBB_TCM'].unique())

        beta_need_overwrite = data[(data['BetaOverwriteStatus']=='SHOULD_OVERWRITE') & (data.apply(lambda x: x['Identifier(Agg)'] not in existing_tickers, axis=1))]
        if not beta_need_overwrite.empty:
            beta_need_overwrite.drop_duplicates(subset=['Identifier(Agg)'], inplace=True, keep='first')

            beta_need_overwrite['IPODate_Overwrite_Until'] = beta_need_overwrite['IPODate'] + pd.offsets.MonthOffset(3)
            beta_need_overwrite['Beta_Overwrite_Value'] = 2.0
            beta_need_overwrite['Beta_Overwrite_Bottom_Value'] = 1.0
            beta_need_overwrite['ValueTypeCode'] = 'BETA_MANUAL'
            security_id_info = self.getSecurityID(list(beta_need_overwrite['Identifier(Agg)'].unique()))
            beta_need_overwrite = pd.merge(beta_need_overwrite, security_id_info, how='left', on=['Identifier(Agg)'])
            beta_need_overwrite['SecurityID'] = beta_need_overwrite['SecurityID'].astype(int)
            beta_need_overwrite['Beta_Overwrite_Value'] = beta_need_overwrite['Beta_Overwrite_Value'].astype(float)
            beta_need_overwrite['Beta_Overwrite_Bottom_Value'] = beta_need_overwrite['Beta_Overwrite_Bottom_Value'].astype(float)
            beta_need_overwrite['IPODate'] =beta_need_overwrite['IPODate'].dt.strftime('%Y-%m-%d')
            beta_need_overwrite['IPODate_Overwrite_Until'] = beta_need_overwrite['IPODate_Overwrite_Until'].dt.strftime('%Y-%m-%d')

            records = pdUtil.dataFrameToSavableRecords(beta_need_overwrite,['SecurityID','ValueTypeCode','Beta_Overwrite_Value','IPODate','IPODate_Overwrite_Until','Beta_Overwrite_Bottom_Value'])
            self.saveBetaOverwriteInfo(records)

            # beta_need_overwrite['Reason'] = 'Resolve New IPO Beta'
            # beta_need_overwrite['Type'] = RiskExceptionSummaryReport.IPO_BETA_OVERWRITE.value
            # beta_need_overwrite['Status'] = RiskExceptionSummaryReportStatus.RESOLVED.value
            # beta_need_overwrite['FundCode'] = beta_need_overwrite['Book'].str.split(pat='-').str[0]
            # beta_need_overwrite['BookCode'] = beta_need_overwrite['Book'].str.split(pat='-').str[1]
            # db_cols = ['AsOfDate',  'FundCode',  'BookCode', 'Ticker', 'Reason', 'Type', 'Status']
            # record_cols = ['Date',  'FundCode',  'BookCode', 'Identifier(Agg)', 'Reason', 'Type', 'Status']
            # self.saveRiskExceptionSummaryReportWithDbCol(db_cols, record_cols, beta_need_overwrite)

    def saveBetaOverwriteInfo(self, records):
        for record in records:
            print record
            sql = 'insert into MarketData.mark.AnalyticOverwrite (SecurityID,ValueTypeCode,Value,FromDate,ToDate,Bottom) Values (?,?,?,?,?,?)'
            self.insertToDatabase(sql, [record])

    def loadStockIPOInfo(self):
        data = pd.read_excel('C:\\devel\\stock_ipo_date\\Sweden.xlsx')
        data['IPO Dt'] = pd.to_datetime(data['IPO Dt'], format='%m/%d/%Y')
        data['Ticker'] = data['Ticker'].astype(str)
        records = pdUtil.dataFrameToSavableRecords(data, ['Ticker','IPO Dt'])
        for index, data_row in data.iterrows():
            ticker = data_row['Ticker']
            ipo_date = data_row['IPO Dt']
            if pd.isnull(ipo_date):
                ipo_date = ''
            else:
                ipo_date = ipo_date.strftime('%Y-%m-%d')
            sql = 'if exists (select * from [RiskDb].[risk].[EquityGICSInfo] where BbgTicker=?) select 1 else select 0';
            self.cursor.execute(sql, (ticker))
            existFlag = False
            for row in self.cursor.fetchall():
                if (row[0] == 1):
                    existFlag = True
                    break

            if (existFlag):
                sql = 'update [RiskDb].[risk].[EquityGICSInfo] set IPODate=? where [BbgTicker]=?'
                self.cursor.execute(sql, (ipo_date, ticker))
            else:
                sql = 'insert into [RiskDb].[risk].[EquityGICSInfo] (IPODate, BbgTicker) '
                sql += 'values(?,?)'
                self.cursor.execute(sql, (ipo_date, ticker))

    def runCounterPartyRisk(self,endDateStr):
        print 'test'

    def copyAndOverwriteRestrictionFiles(self, sourceFilePath, destFilePath):
        fileNameList = os.listdir(sourceFilePath)
        for fileName in fileNameList:
            if ('.csv' in fileName):
                copyfile(sourceFilePath + fileName, destFilePath + fileName)

    def getIgnoreList(self):
        sql = 'SELECT FundCode,BookCode,Ticker,Type,Status as IgnoreStatus FROM RiskDb.risk.RiskExceptionIgnoreList'
        ignore_data = self.selectFromDataBase(sql)
        ignore_data['IgnoreStatus'] = ignore_data['IgnoreStatus'].astype(int)
        ignore_data['Type'] = ignore_data['Type'].astype(int)
        return ignore_data

    def run(self, startDateStr, endDateStr):
        sourceFilePath = '\\\\192.168.200.3\\system\\compliance\\input\\'
        destFilePath = 'C:\\Deployments\\prs-scripts\\benchmark\\risk_control_2019\\restriction\\'
        self.copyAndOverwriteRestrictionFiles(sourceFilePath,destFilePath)
        raw_gics_data = self.loadGICSRestrictionData(destFilePath+'GICS.csv')
        gics_data = self.furtherProcessData(raw_gics_data,'SubIndustry')
        gics_data.loc[:, ('SubIndustry')] = gics_data['SubIndustry'].astype(str)
        raw_exemption_data = self.loadExemptionRestrictionData(destFilePath+'Exemption.csv')
        exemption_data = self.furtherProcessData(raw_exemption_data,'Ticker')
        exemption_data.loc[:, ('Ticker')] = exemption_data['Ticker']+' Equity'
        raw_country_data = self.loadCountryRestrictionData(destFilePath+'CNTRY.csv')
        country_data = self.furtherProcessData(raw_country_data,'CountryCode')
        self.initSqlServer(self.env)
        real_fund = self.getRealFund()
        fundIdList = list(real_fund['FundId'].unique())
        restricted_securities = self.getRestrictedSecurity()
        restricted_securities.loc[:, ('Status')] = 0
        gics_def_data = self.getGICSDef()
        gics_data = pd.merge(gics_data, gics_def_data[['SubIndustry','SubIndustryName']], how='left', on=['SubIndustry'])
        self.removeRiskExceptionSummaryReport(endDateStr)
        ignore_data = self.getIgnoreList()
        for fundId in fundIdList:
            logging.info('running on '+str(fundId))
            self.holdingPosAnalysis(fundId, startDateStr, endDateStr, gics_data, exemption_data, country_data, restricted_securities,ignore_data)
        self.performanceAnalysis(endDateStr)
        self.topBottom10Analysis(startDateStr,endDateStr)
        self.outstandingSharesPct(endDateStr)
        self.checkAndFixBetaOverwrite(endDateStr)
        #self.topIlliquidAnalysis(startDateStr,endDateStr)
        #self.runCounterPartyRisk(endDateStr)
        self.closeSqlServerConnection()

    def runToday(self):
        currentDate = datetime.datetime.now()
        weekDay = currentDate.date().weekday()
        diff = 1
        if (weekDay == 5 or weekDay == 6):
            return
        elif (weekDay == 0):
            diff += 2
        runDay = currentDate - datetime.timedelta(days=diff)
        runDayStr = runDay.strftime('%Y-%m-%d')
        self.run(runDayStr,runDayStr)
        holdingEquityAnalysis = HoldingEquityAnalysis(self.env)
        holdingEquityAnalysis.runToday()

if __name__ == '__main__':
    env = 'prod'
    riskSummaryReports = RiskSummaryReports(env)
    #riskSummaryReports.loadGICSRestrictionData('C:\\Users\\patrick.lo\\Desktop\\restriction\\GICS.csv')

    riskSummaryReports.run('2019-12-03', '2019-12-03')
    #holdingEquityAnalysis = HoldingEquityAnalysis(envall_perf_data['Aum'].astype(float)-)
    # riskSummaryReports.initSqlServer(env)
    # riskSummaryReports.loadStockIPOInfo()
    # riskSummaryReports.closeSqlServerConnection()

    #riskSummaryReports.run([12], '2019-07-02', '2019-07-02')

    #riskSummaryReports.loadGICSInfoFromBBG()

    #riskSummaryReports.runWithDateRange('2019-06-26')


