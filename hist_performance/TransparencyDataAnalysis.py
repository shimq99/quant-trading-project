#encoding:UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import datetime
import logging
from benchmark.base.Base import Base
import pandas as pd
pd.set_option('display.max_columns', 10)
import decimal
decimal.getcontext().prec = 6
from  decimal import Decimal
import benchmark.tools.PandasDBUtils as pdUtil
import numpy as np
import math
import pyodbc
from datetime import timedelta
import xlrd
from benchmark.base.CommonEnums import RiskFundMonthlyReportExternalNetValueType
from dateutil import relativedelta
import calendar
from enum import Enum

class BondCashFlag(Enum):
    Bond_Cash = 1
    NO_Bond_Cash = 2

class TransparencyReportType(Enum):
    EOPOSURE_ANALYSIS=1
    SECOTR_ANALYSIS=2
    MARKET_CAP_ANALYSIS=3

class TransparencyDataAnalysis(Base):
    def __init__(self):
        print 'init'

    def insertToDatabase(self, sql, data):
        if data:
            try:
                self.cursor.executemany(sql, data)
                # for record in data:
                #     try:
                #         self.cursor.execute(sql, (record))
                #     except Exception, e:
                #         logging.error(record)
                #         logging.error('data:'+e.args[1])
                #         raise Exception('error')
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

    def getFXData(self,dateStr):
        sql = 'select * from MarketData.mark.ufn_lastest_fxrate(\''+dateStr+'\',7) where ToCurrencyCode=\'USD\''
        return self.selectDataFromDb(sql)

    def getMarketInfo(self, dateStr):
        sql = 'select * from MarketData.mark.ufn_latest_analytics(\''+dateStr+'\',7)'
        return self.selectDataFromDb(sql)

    def getFundNavView(self, startDateStr, endDateStr, fundCode):
        sql = 'SELECT Fund,FundId,Book,BookId,Date,Source,Aum,QdAum,MgmtFee,Currency,DtdGrossReturn,MtdGrossReturn,YtdGrossReturn,DtdNetReturn,MtdNetReturn,YtdNetReturn,NextDayStartAum ' \
              'FROM Portfolio.perf.NavView WHERE Fund=\''+fundCode+'\' and BookId is null and Date between \''+startDateStr+'\' and \''+endDateStr+'\''
        return self.selectDataFromDb(sql)

    def getRiskPositionData(self, dateStr):
        sql = 'EXEC RiskDb.risk.usp_GetRiskPosition @Date = \''+dateStr+'\''
        return self.selectDataFromDb(sql)

    def getPositionView(self,dateStr, fundId):
        sql = 'SELECT s.PrimarySecurityID as SecurityID,s.PrimaryBB_TCM as BB_TCM,s.PrimaryPnlCurrencyCode as Currency,s.PrimaryExternalInstClass as InstClass,' \
              's.UnderlyingExternalInstClass) as UnderInstClass,s.UnderlyingSecurityId,Security,Fund,FundIsReal,Book,' \
              'FundId,BookId,PositionTypeCode,Date,' \
              'NotnlMktValBook,MtdPnlTotal,QuantityEnd as Quantity,MktValBook' \
              'TotalAccrualsBook,TotalAccrualsLocal,FxExposureLocal,FxExposureLocalStart FROM Portfolio.pos.PositionView p'
        sql += 'left join IMS.sm.Security s on p.SecurityID=s.SecurityId'
        sql += ' where p.Date=\''+dateStr+'\' and FundId='+fundId +' and QuantityDirection in (\'Long\',\'Short\',\'FLAT In Month\') and PositionTypeCode not in (\'NOTNL_CSH\',\'FundManagementFee\')'

        return self.selectDataFromDb(sql)

    def getPositionSecurityInfo(self, dateStr, fundList):
        sql = 'SELECT S.ExternalAssetClass,P.Security,P.Fund,P.FundIsReal,P.Book,P.QuantityDirection,P.SecurityId,P.Currency,s.PrimarySecurityID as PriSecurityID,	' \
              'P.FundId,P.BookId,P.PositionTypeCode,P.Strategy,P.Date,P.QuantityEnd,P.MktValBook,P.NotnlMktValBook,P.MtdPnlTotal,' \
              'S.BB_TCM AS Ticker, S.ExternalInstClass AS InstrumentClass, S.UnderlyingSecurityId, S.UnderlyingDisplayCode AS UnderlyingSecurityCode,s.PrimaryExternalInstClass as PriInstClass, ' \
              'S.UnderlyingBB_TCM AS UnderlyingTicker, S.UnderlyingISIN AS UnderlyingIsin, S.UnderlyingExternalInstClass AS UnderlyingInstrumentClass, S.ExchangeCode '
        sql += ' FROM Portfolio.pos.PositionView P'
        sql += ' LEFT JOIN SecurityMaster.sm.SecurityView S on P.SecurityId=S.SecurityId'
        sql += ' LEFT JOIN MarketData.mark.Price K on K.Date=P.Date and P.SecurityId=K.SecurityId'
        sql += ' LEFT JOIN MarketData.mark.Price U on U.Date=P.Date and S.UnderlyingSecurityId=U.SecurityId'
        sql += ' where P.FundIsReal = 1 AND P.QuantityDirection in (\'Long\',\'Short\',\'FLAT In Month\') AND P.PositionTypeCode not in (\'NOTNL_CSH\',\'PmManagementFee\')'
        sql += ' AND P.Date=\''+dateStr+'\''
        if fundList:
            sql += ' and P.Fund in (\'' + ('\',\'').join(fundList)+'\') '
        data = self.selectDataFromDb(sql)
        data['UnderlyingSecurityId'] = data['UnderlyingSecurityId'].fillna(0)
        data['UnderlyingSecurityId'] = data['UnderlyingSecurityId'].astype(int)
        return data


    def getFIMPosHistData(self, datestr, days, pos_data):
        pos_data = pos_data[pos_data['QuantityDirection'].isin(['FLAT IN MONTH'])].copy()
        pos_data['Key'] = pos_data['FundId'].astype('str') + '-' + pos_data['BookId'].astype('str') + '-' + pos_data['PriSecurityID'].astype('str')
        fundList = list(pos_data['FundId'].unique())
        bookList = list(pos_data['BookId'].unique())
        securityIdList = list(pos_data['PriSecurityID'].unique())
        sql = 'SELECT P.FundId, P.BookId, P.SecurityId,s.PrimarySecurityID as PriSecurityID,P.QuantityDirection '
        sql += ' FROM Portfolio.pos.Position P'
        sql += ' LEFT JOIN SecurityMaster.sm.SecurityView S on P.SecurityId=S.SecurityId'
        sql += ' where P.QuantityDirection in (\'Long\',\'Short\') AND P.PositionTypeCode not in (\'NOTNL_CSH\',\'PmManagementFee\',\'STLN\')'
        sql += ' AND P.Date<\'' + datestr + '\' and P.Date > DATEADD(day, '+str(days)+', \''+datestr+'\')'
        sql += ' and P.FundId in (' + (',').join(str(x) for x in fundList) + ') '
        sql += ' and P.BookId in (' + (',').join(str(x) for x in bookList) + ') '
        sql += ' and s.PrimarySecurityID in (' + (',').join(str(x) for x in securityIdList) + ') '
        data = self.selectDataFromDb(sql)
        data['QuantityDirection'] = data['QuantityDirection'].str.upper()
        return data

    def getFIMDirection(self, pos_data):
        pos_data = pos_data[~pos_data['QuantityDirection'].isin(['FLAT IN MONTH'])]
        pos_data = pos_data.groupby(['FundId', 'BookId','PriSecurityID']).agg({'QuantityDirection': 'max'})
        pos_data = pos_data.reset_index()
        pos_data['FIMQuantityDirection'] = pos_data['QuantityDirection']
        pos_data['Key'] = pos_data['FundId'].astype('str') + '-' + pos_data['BookId'].astype('str') + '-' + pos_data['PriSecurityID'].astype('str')
        del pos_data['QuantityDirection']
        return pos_data

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

    def getPreviousMonth(self, dateMonthStr):
        run_date = pd.to_datetime(dateMonthStr, format='%Y-%m')
        previous_1m_date = run_date + relativedelta.relativedelta(months=-1)
        return previous_1m_date.strftime('%Y-%m')

    def getFundUnitPrice(self,start_date, end_date,fundCode):
        sql = 'SELECT N.FundId,N.BookId,F.FundCode,B.BookCode,Date,UnitTotal as NetUnit FROM Portfolio.perf.Nav N left join ref.Fund F on F.FundId=N.FundId  left join ref.Book B on B.BookId=N.BookId where Date between \''+start_date+'\' and \''+end_date+'\' and F.FundCode=\''+fundCode+'\' and N.BookId is null'
        return self.selectDataFromDb(sql)

    def nanToNone(self, df_data, columnList):
        for column in columnList:
            df_data[column] = np.where(df_data[column].isna(), None, df_data[column])
        return df_data


    def getMarketMapping(self):
        sql = 'SELECT OriginMarket,Market FROM RiskDb.ref.MarketMapping'
        data = self.selectDataFromDb(sql)
        return data

    def getTeamStrat(self):
        sql = 'SELECT Team as Book,Strategy as Strat,Fund FROM RiskDb.ref.TeamStrategyMapping'
        data = self.selectDataFromDb(sql)
        return data

    def processData(self, runMonthStr, fundCode):
        last_business_day_of_month_str = self.getLastBussinessOnCurrentMonth(runMonthStr)
        last_b_day_pre_month_str = self.getLastBussinessOnCurrentMonth(self.getPreviousMonth(runMonthStr))
        market_data = self.getMarketInfo(last_business_day_of_month_str)
        market_data['SecIDForMktCap'] = market_data['SecurityID']
        fx_data = self.getFXData(last_b_day_pre_month_str)
        fx_data['Currency'] = fx_data['FromCurrencyCode']
        market_map_data = self.getMarketMapping()
        market_map_data['ExchangeCode'] = market_map_data['OriginMarket']
        market_map_data['Origin_Market'] = market_map_data['Market']
        team_strat = self.getTeamStrat()
        position_data = self.getPositionSecurityInfo(last_business_day_of_month_str,[fundCode])
        '''
        P.MktValBook,P.NotnlMktValBook,P.MtdPnlTotal,' 
        
        Net Market value (notional) 
             (1) all currency pos, except CDS,IRS,FRA, = 0
             (2) Stock loan = 0
        
        Gross Market value (notional) - Notional gross market value of net position in base currency
             (1) all currency pos, except CDS,IRS,FRA, = 0
             (2) Stock loan = 0
        Net Market Value - 
             (1) all currency pos, except CDS,IRS,FRA, = 0
             (2) Stock loan = 0
             
        '''
        position_data['NotnlMktValBook'] = np.where((position_data['PositionTypeCode'].isin(['STLN','TRD_CSH']) | (position_data['InstrumentClass'].isin(['CCY']))),0,position_data['NotnlMktValBook'])
        position_data['SecIDForMktCap'] =  np.where(position_data['PriInstClass'].isin(['EQTY_OP']),position_data['UnderlyingSecurityId'],position_data['PriSecurityID'])
        position_data = pd.merge(position_data, market_data[['SecIDForMktCap', 'CUR_MKT_CAP']], how='left', on=['SecIDForMktCap'])
        position_data = pd.merge(position_data, fx_data[['Currency', 'Last']], how='left', on=['Currency'])
        position_data = pd.merge(position_data, market_map_data[['ExchangeCode', 'Origin_Market']], how='left', on=['ExchangeCode'])
        position_data = pd.merge(position_data, team_strat, how='left', on=['Fund','Book'])
        position_data['Last'] = np.where(position_data['Last'].isna(),1,position_data['Last'])
        run_date_nav_data = self.getFundNavView(last_business_day_of_month_str,last_business_day_of_month_str,fundCode)
        position_data = pd.merge(position_data, run_date_nav_data[['Fund','MtdGrossReturn','MtdNetReturn']], how='left', on=['Fund'])
        Aum_Current_Month = run_date_nav_data['Aum'][0]
        run_date_nav_data_before_1M = self.getFundNavView(last_b_day_pre_month_str,last_b_day_pre_month_str,fundCode)
        run_date_nav_data_before_1M['AumLastMonth'] = np.where(run_date_nav_data_before_1M['NextDayStartAum'].isna(),run_date_nav_data_before_1M['Aum'],run_date_nav_data_before_1M['NextDayStartAum'])
        Aum_Last_Month = run_date_nav_data_before_1M['AumLastMonth'][0]
        position_data = position_data[position_data['Fund'].isin([fundCode])]
        position_data['AumCurrentMonth'] = Aum_Current_Month
        position_data['AumLastMonth'] = Aum_Last_Month
        position_data['Book'] = np.where(position_data['PositionTypeCode'].isin(['TRD_CSH']), 'CASH', position_data['Book'])

        position_data['Performance_Fee'] = Decimal(0.9)
        position_data['Provision_Fee'] = Decimal(0.0026)
        '''MV_T'''
        position_data['Market_Value_Total'] = position_data['NotnlMktValBook']/position_data['AumCurrentMonth']
        '''MV_T_ABS'''
        position_data['Market_Value_Total_ABS'] = position_data['Market_Value_Total'].abs()
        position_data['No_Bond_Cash_Flag'] = np.where(position_data['InstrumentClass'].isin(['FX_FT', 'CCY', 'FWRD_FX', 'FX_OP', 'CONVBND', 'BOND', 'Cash', 'IR_FT', 'BOND_FT', 'GOVT', 'CD_SW', 'CMDTY_FT', 'CMDTY_OP']),
                                                           BondCashFlag.Bond_Cash.value, BondCashFlag.NO_Bond_Cash.value)
        '''MV_NBC'''
        position_data['Market_Value_No_Bond_Cash'] = np.where(position_data['No_Bond_Cash_Flag'] == BondCashFlag.Bond_Cash.value, 0, position_data['Market_Value_Total'])
        '''MV_NBC_ABS'''
        position_data['Market_Value_No_Bond_Cash_ABS'] = position_data['Market_Value_No_Bond_Cash'].abs()


        MV_T_G_SUM = Decimal(position_data['Market_Value_Total_ABS'].astype(float).sum())
        MV_T_N_SUM = position_data['Market_Value_Total'].astype(float).sum()
        MV_NBC_G_SUM = Decimal(position_data['Market_Value_No_Bond_Cash_ABS'].astype(float).sum())
        MV_NBC_N_SUM = position_data['Market_Value_No_Bond_Cash'].astype(float).sum()

        '''PL_T'''
        position_data['PnL_Total'] = position_data['MtdPnlTotal']/position_data['AumLastMonth']
        '''PL_T_F'''
        position_data['PnL_Total_With_Fee'] = (position_data['PnL_Total'] - position_data['Market_Value_Total_ABS']/MV_T_G_SUM*position_data['Provision_Fee'])*position_data['Performance_Fee']
        '''PL_NBC'''
        position_data['PnL_No_Bond_Cash'] = np.where(position_data['No_Bond_Cash_Flag'] == BondCashFlag.Bond_Cash.value, 0, position_data['PnL_Total'])
        '''PL_NBC_F'''
        position_data['PnL_No_Bond_Cash_With_Fee'] = np.where(position_data['No_Bond_Cash_Flag'] == BondCashFlag.Bond_Cash.value, 0, (position_data['PnL_Total']-position_data['Market_Value_No_Bond_Cash_ABS']/MV_NBC_G_SUM*position_data['Provision_Fee'])*position_data['Performance_Fee'])

        position_data['QuantityDirection'] = position_data['QuantityDirection'].str.upper()


        position_data['Market_Transparency'] = np.where(position_data['InstrumentClass'].isin(['BOND','CONVBND']),'Bond',position_data['Origin_Market'])
        position_data['Mkt_Idty'] = position_data['Market_Transparency'] + '-' + position_data['QuantityDirection']
        '''Market Cap Identity'''
        position_data['Market_Cap'] = np.where(position_data['PriInstClass'].isin(['DR', 'ORD','EQTY_OP']), position_data['CUR_MKT_CAP']*position_data['Last'], np.nan)
        position_data['Market_Cap_Idty'] = np.where(position_data['PriInstClass'].isin(['EQINDX_FT', 'Indx']), 'Index',
                                                    np.where(position_data['Market_Cap'] >= 10000, '>10bn',
                                                             np.where(position_data['Market_Cap'] >= 5000,'5-10bn',
                                                                      np.where(position_data['Market_Cap'] >= 1000, '1-5bn',
                                                                               np.where(position_data['Market_Cap'] >= 500, '500m-1bn',
                                                                                        np.where(position_data['Market_Cap'].isna(), 'No Equities', '<500m'))))))
        '''Flat In Month'''
        FIM_Hist_Data = self.getFIMPosHistData(last_business_day_of_month_str,-48,position_data)
        FIM_Direction = self.getFIMDirection(FIM_Hist_Data)
        position_data = pd.merge(position_data, FIM_Direction, how='left', on=['FundId', 'BookId','PriSecurityID'])
        position_data['Quantity'] = np.where(position_data['PositionTypeCode'].isin(['TRD_CSH']),1,
                                             np.where(position_data['PositionTypeCode'].isin(['STLN']), -1, position_data['QuantityEnd']))
        position_data['LS_Indic'] = np.where(position_data['QuantityDirection'].isin(['FLAT IN MONTH']),position_data['FIMQuantityDirection'],
                                             np.where(position_data['Quantity'] > 0, 'LONG',
                                                      np.where(position_data['Quantity']<0, 'SHORT','Unkown Direction')))
        return last_business_day_of_month_str,position_data

    def saveRiskTransparencyReport(self, records):
        if records:
            sql4 = 'insert into RiskDb.risk.RiskTransparencyReport (AsOfDate,Fund,Type,ReportName,LongValue,ShortValue,GrossValue,NetValue,PNLLongValue,PNLShortValue,PNLNetValue) values (?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql4,records)


    def analysis(self, asOfDateStr, position_data,fund):
        records = []
        exposure_data = position_data.copy()
        ''' *************************** Exposure ****************************************'''
        exposure_LS_result = exposure_data.groupby(['LS_Indic','Market_Transparency']).agg({'Market_Value_Total': 'sum','Market_Value_No_Bond_Cash_ABS':'sum','PnL_Total_With_Fee': 'sum','Market_Value_No_Bond_Cash':'sum','MtdGrossReturn':'max','MtdNetReturn':'max','PnL_No_Bond_Cash_With_Fee':'sum'})
        exposure_LS_result = exposure_LS_result.reset_index()
        exposure_LS_result_pivoted = exposure_LS_result.pivot_table(index=['Market_Transparency'], columns='LS_Indic', values=['Market_Value_No_Bond_Cash', 'PnL_No_Bond_Cash_With_Fee'], aggfunc='first').reset_index()

        exposure_GN_result = exposure_LS_result.groupby(['Market_Transparency']).agg({'Market_Value_No_Bond_Cash': 'sum', 'Market_Value_No_Bond_Cash_ABS': 'sum', 'PnL_No_Bond_Cash_With_Fee': 'sum'})
        exposure_GN_result = exposure_GN_result.reset_index()
        exposure_GN_result['GeoExposureNet'] = exposure_GN_result['Market_Value_No_Bond_Cash']
        exposure_GN_result['GeoExposureGross'] = exposure_GN_result['Market_Value_No_Bond_Cash_ABS']
        exposure_GN_result['GeoPnLAttrNet'] = exposure_GN_result['PnL_No_Bond_Cash_With_Fee']

        exposure_LSGN_result = pd.merge(exposure_LS_result_pivoted, exposure_GN_result[['Market_Transparency', 'GeoExposureNet', 'GeoExposureGross','GeoPnLAttrNet']], how='left', on=['Market_Transparency'])
        exposure_LSGN_result['ReportType'] = TransparencyReportType.EOPOSURE_ANALYSIS.value
        exposure_LSGN_result['Name'] = 'Exposure'
        exposure_LSGN_result['AsOfDate'] = asOfDateStr
        exposure_LSGN_result['Fund'] = fund
        exposure_LSGN_result.columns = ['-'.join(col) if type(col) is tuple else col for col in exposure_LSGN_result.columns.values]

        exposure_LSGN_result[['Market_Value_No_Bond_Cash-LONG','Market_Value_No_Bond_Cash-SHORT','GeoExposureGross','GeoExposureNet','PnL_No_Bond_Cash_With_Fee-LONG','PnL_No_Bond_Cash_With_Fee-SHORT','GeoPnLAttrNet']] = exposure_LSGN_result[['Market_Value_No_Bond_Cash-LONG','Market_Value_No_Bond_Cash-SHORT','GeoExposureGross','GeoExposureNet','PnL_No_Bond_Cash_With_Fee-LONG','PnL_No_Bond_Cash_With_Fee-SHORT','GeoPnLAttrNet']].fillna(0)
        records += pdUtil.dataFrameToSavableRecords(exposure_LSGN_result, ['AsOfDate','Fund','ReportType','Market_Transparency','Market_Value_No_Bond_Cash-LONG','Market_Value_No_Bond_Cash-SHORT',
                                                                           'GeoExposureGross','GeoExposureNet','PnL_No_Bond_Cash_With_Fee-LONG','PnL_No_Bond_Cash_With_Fee-SHORT','GeoPnLAttrNet'])



        ''' *************************** Team Sector ****************************************'''
        team_sector_data = position_data.copy()
        team_sector_result = team_sector_data.groupby(['Strat', 'LS_Indic']).agg({'Market_Value_Total': 'sum','Market_Value_Total_ABS':'sum','PnL_Total_With_Fee':'sum'})
        team_sector_result = team_sector_result.reset_index()
        team_sector_result_pivoted = team_sector_result.pivot_table(index=['Strat'], columns='LS_Indic', values=['Market_Value_Total','PnL_Total_With_Fee'], aggfunc='first').reset_index()

        team_sector_GN_result = team_sector_result.groupby(['Strat']).agg({'Market_Value_Total': 'sum','Market_Value_Total_ABS':'sum','PnL_Total_With_Fee':'sum'})
        team_sector_GN_result = team_sector_GN_result.reset_index()
        team_sector_GN_result['SectorGross'] = team_sector_GN_result['Market_Value_Total_ABS']
        team_sector_GN_result['SectorNet'] = team_sector_GN_result['Market_Value_Total']
        team_sector_GN_result['SectorPnLAttrNet'] = team_sector_GN_result['PnL_Total_With_Fee']

        team_sector_LSLN_result = pd.merge(team_sector_result_pivoted, team_sector_GN_result[['Strat','SectorGross','SectorNet','SectorPnLAttrNet']], how='left', on=['Strat'])
        team_sector_LSLN_result['ReportType'] = TransparencyReportType.SECOTR_ANALYSIS.value
        #team_sector_LSLN_result['Name'] = 'Sector'
        team_sector_LSLN_result['AsOfDate'] = asOfDateStr
        team_sector_LSLN_result['Fund'] = fund
        team_sector_LSLN_result.columns = ['-'.join(col) if type(col) is tuple else col for col in team_sector_LSLN_result.columns.values]

        team_sector_LSLN_result[['Market_Value_Total-LONG','Market_Value_Total-SHORT','SectorGross','SectorNet','PnL_Total_With_Fee-LONG','PnL_Total_With_Fee-SHORT','SectorPnLAttrNet']] = team_sector_LSLN_result[['Market_Value_Total-LONG','Market_Value_Total-SHORT','SectorGross','SectorNet','PnL_Total_With_Fee-LONG','PnL_Total_With_Fee-SHORT','SectorPnLAttrNet']].fillna(0)

        records += pdUtil.dataFrameToSavableRecords(team_sector_LSLN_result, ['AsOfDate','Fund','ReportType','Strat','Market_Value_Total-LONG','Market_Value_Total-SHORT',
                                                                           'SectorGross','SectorNet','PnL_Total_With_Fee-LONG','PnL_Total_With_Fee-SHORT','SectorPnLAttrNet'])

        ''' *************************** Market Cap ****************************************'''
        market_cap_data = position_data.copy()
        market_cap_data_result = market_cap_data.groupby(['Market_Cap_Idty','LS_Indic']).agg({'Market_Value_No_Bond_Cash': 'sum','Market_Value_No_Bond_Cash_ABS':'sum','PnL_No_Bond_Cash':'sum','PnL_No_Bond_Cash_With_Fee':'sum'})
        market_cap_data_result = market_cap_data_result.reset_index()
        market_cap_result_pivoted = market_cap_data_result.pivot_table(index=['Market_Cap_Idty'], columns='LS_Indic', values=['Market_Value_No_Bond_Cash','PnL_No_Bond_Cash_With_Fee'], aggfunc='first').reset_index()

        market_cap_data_GN_result = market_cap_data_result.groupby(['Market_Cap_Idty']).agg({'Market_Value_No_Bond_Cash': 'sum','Market_Value_No_Bond_Cash_ABS':'sum','PnL_No_Bond_Cash_With_Fee':'sum'})
        market_cap_data_GN_result = market_cap_data_GN_result.reset_index()
        market_cap_data_GN_result['MktCapGross'] = market_cap_data_GN_result['Market_Value_No_Bond_Cash_ABS']
        market_cap_data_GN_result['MktCapNet'] = market_cap_data_GN_result['Market_Value_No_Bond_Cash']
        market_cap_data_GN_result['MktCapPnLNet'] = market_cap_data_GN_result['PnL_No_Bond_Cash_With_Fee']

        market_cap_LSLN_result = pd.merge(market_cap_result_pivoted, market_cap_data_GN_result[['Market_Cap_Idty','MktCapGross','MktCapNet','MktCapPnLNet']], how='left', on=['Market_Cap_Idty'])
        market_cap_LSLN_result['ReportType'] = TransparencyReportType.MARKET_CAP_ANALYSIS.value
        market_cap_LSLN_result['Name'] = 'Market Cap'
        market_cap_LSLN_result['AsOfDate'] = asOfDateStr
        market_cap_LSLN_result['Fund'] = fund
        market_cap_LSLN_result.columns = ['-'.join(col) if type(col) is tuple else col for col in market_cap_LSLN_result.columns.values]
        market_cap_LSLN_result[['Market_Value_No_Bond_Cash-LONG','Market_Value_No_Bond_Cash-SHORT','MktCapGross','MktCapNet','PnL_No_Bond_Cash_With_Fee-LONG','PnL_No_Bond_Cash_With_Fee-SHORT','MktCapPnLNet']] = market_cap_LSLN_result[['Market_Value_No_Bond_Cash-LONG','Market_Value_No_Bond_Cash-SHORT','MktCapGross','MktCapNet','PnL_No_Bond_Cash_With_Fee-LONG','PnL_No_Bond_Cash_With_Fee-SHORT','MktCapPnLNet']].fillna(0)

        records += pdUtil.dataFrameToSavableRecords(market_cap_LSLN_result, ['AsOfDate','Fund','ReportType','Market_Cap_Idty','Market_Value_No_Bond_Cash-LONG','Market_Value_No_Bond_Cash-SHORT',
                                                                           'MktCapGross','MktCapNet','PnL_No_Bond_Cash_With_Fee-LONG','PnL_No_Bond_Cash_With_Fee-SHORT','MktCapPnLNet'])

        self.saveRiskTransparencyReport(records)

    def calc(self, dateStr):
        processed_data = self.processData(dateStr)


if __name__ == '__main__':
    transparency_data_analysis = TransparencyDataAnalysis()
    transparency_data_analysis.initSqlServer('prod')
    last_business_day_of_month_str,processed_data = transparency_data_analysis.processData('2019-08','PMSF')
    transparency_data_analysis.analysis(last_business_day_of_month_str,processed_data,'PMSF')
    transparency_data_analysis.closeSqlServerConnection()




