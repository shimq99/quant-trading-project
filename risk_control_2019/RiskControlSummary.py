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
from datetime import timedelta
from enum import Enum
from dateutil import relativedelta
import calendar
import datetime
import holidays
from benchmark.tools import PandasDBUtils as pdUtil

class TransparencyReportType(Enum):
    EOPOSURE_ANALYSIS=1
    SECOTR_ANALYSIS=2
    MARKET_CAP_ANALYSIS=3

class RiskControlSummary(Base):
    def __init__(self):
       self.text = 'dataquery'

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

    def selectDataFromDb(self, sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def getFundNav(self,dateStrList,fundList):
        sql = 'SELECT Fund,Date as AsOfDate__,Aum,QdAum,MtdGrossReturn ' \
              ' from Portfolio.perf.NavView where Date in (\''+ ('\',\'').join(dateStrList) + '\') and Fund in (\'' + ('\',\'').join(fundList)+'\') and BookId is null'
        data = self.selectDataFromDb(sql)
        return data
    def getRiskLiquidityData(self, startDateStr, endDateStr):
        sql = 'SELECT AsOfDate,FundId,BookId,FundCode,BookCode,TwoDaysIlliquidLongNavPorp,TwoDaysIlliquidShortNavPorp,TwoDaysIlliquidNavPorpTotal,FiveDaysIlliquidLongNavPorp,FiveDaysIlliquidShortNavPorp,FiveDaysIlliquidNavPorpTotal,REITSAssetNetNavPorp,BondAssetNetNavPorp,TwoDaysIlliquidAssetStatus,FiveDaysIlliquidAssetStatus,LiquidityTotalStatus ' \
              'FROM RiskDb.risk.RiskLiquidityReport where AsOfDate between\''+startDateStr+'\' and \''+endDateStr+'\''
        liquidity_data = self.selectDataFromDb(sql)
        return liquidity_data

    def getRiskExposureData(self, startDateStr, endDateStr):
        sql = 'SELECT B.MaxDd, B.CurrDd, E.AsOfDate,E.FundId, E.BookId,FundCode,BookCode,EquityLongBeta,EquityShortBeta,EquityLongNoBeta,EquityShortNoBeta,EquityGross,Bond,BondGross,BondGrossNav,FOLongBeta,FOShortBeta,OtherFOLongBeta,OtherFOShortBeta,Others,EquityIndxFTShort,NetBeta,GrossMargin,GrossNotnlMV,HistMDDGrossNotnlMV,ChinaMarket,HKMarket,USMarket,TaiwanMarket,AustraliaMarket,JapanMarket,SourthKRMarket,IndiaMarket,SingaporeMarket,OthersMarket,DerivativeGross,NoOfStock,StockLong,StockShort,RiskControlStopLossNetLimitStatus,RiskControlHoldingNetLimitStatus,GrossMarginLimit,RiskControlGrossMarginStatus,GrossLimitStatus,MarketHoldingLimitStatus,StockLimitStatus,BondHoldingLimitStatus,DerivativeCashLimitStatus,StockLongShortLimitStatus,RiskExposureTotalStatus,MaxGrossNotionalAfterMDD,IsNew ' \
              ' FROM RiskDb.risk.RiskExposureReport E ' \
              ' left join RiskDb.risk.Performance B on E.FundId=B.FundId and E.BookId=B.BookId and E.AsOfDate=B.AsOfDate where E.AsOfDate between\''+startDateStr+'\' and \''+endDateStr+'\''
        exposure_data = self.selectDataFromDb(sql)
        return exposure_data

    def next_business_day(self, current_date):
        ONE_DAY = datetime.timedelta(days=1)
        next_day = current_date + ONE_DAY
        while next_day.weekday() in holidays.WEEKEND:
            next_day += ONE_DAY
        return next_day

    def analysis(self,startDateStr,endDateStr,riskControlActionDaysLimit):
        LIMIT_STATUS_COLS= ['RiskControlStopLossNetLimit','RiskControlHoldingNetLimit','RiskControlGrossMargin','GrossLimit','MarketHoldingLimit','StockLimit','BondHoldingLimit','DerivativeCashLimit','StockLongShortLimit']
        liquidity_data = self.getRiskLiquidityData(startDateStr,endDateStr)

        exposure_data = self.getRiskExposureData(startDateStr,endDateStr)
        exposure_data['FundBookCode'] = exposure_data['FundCode']+'-'+exposure_data['BookCode']
        exposure_data = exposure_data[exposure_data['FundCode']=='PMSF']
        exposure_data = exposure_data[exposure_data['BookCode']=='T23']
        fundBookCodeList = list(exposure_data['FundBookCode'].unique())
        total_result = pd.DataFrame(columns=['FundCode', 'BookCode', 'StartDate', 'EndDate', 'LimitDes','ActionDays','ValueBeforeControl','ValueAfterControl','Level1Flag','Level2Flag'])
        for fundBookCode in fundBookCodeList:
            fundbook_exposure_data=exposure_data[exposure_data['FundBookCode']==fundBookCode].copy()
            fundbook_exposure_data['AsOfDate'] = pd.to_datetime(fundbook_exposure_data['AsOfDate'])
            fundbook_exposure_data.index = fundbook_exposure_data['AsOfDate']
            fundbook_exposure_data.sort_index(ascending=True, inplace=True)
            #fundbook_expdata_tc[['RiskControlHoldingNetLimitStatus','GrossMarginLimit','RiskControlGrossMarginStatus','GrossLimitStatus','MarketHoldingLimitStatus','StockLimitStatus','BondHoldingLimitStatus','DerivativeCashLimitStatus','StockLongShortLimitStatus']] = fundbook_expdata_tc[['RiskControlHoldingNetLimitStatus','GrossMarginLimit','RiskControlGrossMarginStatus','GrossLimitStatus','MarketHoldingLimitStatus','StockLimitStatus','BondHoldingLimitStatus','DerivativeCashLimitStatus','StockLongShortLimitStatus']].rolling(RISK_CONTROL_WITHIN_DAYS).sum()
            for risk_limit_name in LIMIT_STATUS_COLS:
                risk_limit_name_col=risk_limit_name+'Status'
                fundbook_expdata_tc01 = fundbook_exposure_data[fundbook_exposure_data[risk_limit_name_col] != 0].copy()
                fundbook_expdata_tc = fundbook_exposure_data[fundbook_exposure_data[risk_limit_name_col] != 0].copy()
                ''' ***** for normal working day'''
                dt = fundbook_expdata_tc['AsOfDate']
                day = pd.Timedelta('1d')
                # in_block = ((dt-dt.shift(-1)).abs() == day) | (dt.diff() == day)
                # filt = fundbook_expdata_tc.loc[in_block]
                # breaks = filt['AsOfDate'].diff() != day
                breaks = fundbook_expdata_tc['AsOfDate'].diff() != day
                groups = breaks.cumsum()
                for _, frame in fundbook_expdata_tc.groupby(groups):
                    '''valid data'''
                    frame = frame[['AsOfDate','FundBookCode']]
                    last_date = frame['AsOfDate'].iloc[-1]
                    next_b_day = self.next_business_day(last_date)
                    action_days=frame.shape[0]
                    level1_action_flag = 1 if risk_limit_name_col !='RiskControlStopLossNetLimitStatus' else 0
                    level2_action_flag = 1-level1_action_flag
                    frame = frame.append({'AsOfDate': next_b_day, 'FundBookCode': fundBookCode}, ignore_index=True)
                    frame['AsOfDate'] = pd.to_datetime(frame['AsOfDate'])
                    merged_data = pd.merge(frame, fundbook_exposure_data, how='left', on=['AsOfDate','FundBookCode'])
                    FundCode = fundBookCode.split('-')[0]
                    BookCode = fundBookCode.split('-')[1]
                    value_after_control, value_before_control = self.value_before_after_control(merged_data, risk_limit_name_col)

                    total_result = total_result.append({'FundCode':FundCode, 'BookCode':BookCode,'StartDate':merged_data['AsOfDate'].iloc[0],'EndDate':merged_data['AsOfDate'].iloc[-1],'LimitDes':risk_limit_name_col,'ActionDays':action_days,'ValueBeforeControl':value_before_control,'ValueAfterControl':value_after_control,
                                                        'Level1Flag':level1_action_flag, 'Level2Flag':level2_action_flag},ignore_index=True)

                ''' ***** for days cross a weekend'''
                weekend_data = fundbook_expdata_tc01[(fundbook_expdata_tc['AsOfDate'].dt.dayofweek==4) | (fundbook_expdata_tc['AsOfDate'].dt.dayofweek==0) | (fundbook_expdata_tc['AsOfDate'].dt.dayofweek==3)].copy()
                dt = weekend_data['AsOfDate']
                weekend_day = pd.Timedelta('3d')
                in_weekend_block = ((dt-dt.shift(-1)).abs() == weekend_day) | (dt.diff() == weekend_day)
                filt_weekend = weekend_data.loc[in_weekend_block]
                breaks_weekend = filt_weekend['AsOfDate'].diff() != weekend_day
                groups_weekend = breaks_weekend.cumsum()
                for _, frame in filt_weekend.groupby(groups_weekend):
                   '''only get last day is Monday's data'''
                   is_monday = frame['AsOfDate'].iloc[-1].weekday()
                   if is_monday==0:
                       frame = frame[['AsOfDate', 'FundBookCode']]
                       last_date = frame['AsOfDate'].iloc[-1]
                       next_b_day = self.next_business_day(last_date)
                       action_days = frame.shape[0]
                       frame = frame.append({'AsOfDate': next_b_day, 'FundBookCode': fundBookCode}, ignore_index=True)
                       frame['AsOfDate'] = pd.to_datetime(frame['AsOfDate'])
                       merged_data = pd.merge(frame, fundbook_exposure_data, how='left', on=['AsOfDate','FundBookCode'])
                       FundCode = fundBookCode.split('-')[0]
                       BookCode = fundBookCode.split('-')[1]
                       value_after_control, value_before_control = self.value_before_after_control(merged_data, risk_limit_name_col)
                       total_result = total_result.append({'FundCode': FundCode, 'BookCode': BookCode,
                                                           'StartDate': merged_data['AsOfDate'].iloc[0],
                                                            'EndDate': merged_data['AsOfDate'].iloc[-1],
                                                            'LimitDes': risk_limit_name_col,
                                                            'ActionDays': action_days, 'ValueBeforeControl':value_before_control, 'ValueAfterControl':value_after_control,
                                                            'Level1Flag': level1_action_flag, 'Level2Flag': level2_action_flag}, ignore_index=True)
            total_result['RiskControlCompleteStatus'] = np.where(total_result['ActionDays']>riskControlActionDaysLimit,0,1)
            total_result[['ValueBeforeControl','ValueAfterControl','RiskControlCompleteStatus']] = total_result[['ValueBeforeControl','ValueAfterControl','RiskControlCompleteStatus']].fillna('')
            total_result[['ActionDays','Level1Flag', 'Level2Flag','RiskControlCompleteStatus']] = total_result[['ActionDays','Level1Flag', 'Level2Flag','RiskControlCompleteStatus']].astype(int)
            records = pdUtil.dataFrameToSavableRecords(total_result, ['FundCode', 'BookCode', 'StartDate','EndDate', 'LimitDes', 'ActionDays','ValueBeforeControl','ValueAfterControl','Level1Flag', 'Level2Flag','RiskControlCompleteStatus'])
            self.saveResult(records)

    def saveResult(self, reocrds):
        if reocrds:
            logging.info('saving risk Control Summary')
            sql = 'insert into RiskDb.risk.RiskControlSummaryReport (FundCode,BookCode,StartDate,EndDate,LimitDes,ActionDays,ValueBeforeControl,ValueAfterControl,Level1Flag,Level2Flag,RiskControlCompleteStatus) values (?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, reocrds)
        else:
            logging.warn('saveAlphaBeta: empty record')
    def value_before_after_control(self, merged_data, risk_limit_name_col):
        if risk_limit_name_col == 'RiskControlHoldingNetLimitStatus':
            val_col_name = 'NetBeta'
            value_before_control = self.formatAsPercent(merged_data[val_col_name].iloc[0])
            value_after_control = self.formatAsPercent(merged_data[val_col_name].iloc[-1])
        elif risk_limit_name_col == 'RiskControlGrossMarginStatus':
            val_col_name = 'GrossMargin'
            value_before_control = self.formatAsPercent(merged_data[val_col_name].iloc[0]) + ' / ' + \
                                                        self.formatAsPercent(merged_data['GrossNotnlMV'].iloc[0])
            value_after_control = self.formatAsPercent(merged_data[val_col_name].iloc[-1]) + ' / ' + \
                                                       self.formatAsPercent(merged_data['GrossNotnlMV'].iloc[-1])
        elif risk_limit_name_col == 'GrossLimitStatus':
            val_col_name = 'GrossMargin'
            value_before_control = self.formatAsPercent(merged_data[val_col_name].iloc[0])
            value_after_control = self.formatAsPercent(merged_data[val_col_name].iloc[-1])
        elif risk_limit_name_col == 'MarketHoldingLimitStatus':
            val_col_name = 'ChinaMarket'
            value_before_control = 'CN:' + self.formatAsPercent(merged_data[val_col_name].iloc[0]) + '/'
            value_after_control = 'CN:' + self.formatAsPercent(merged_data[val_col_name].iloc[-1]) + '/'
            val_col_name = 'HKMarket'
            value_before_control += 'HK:' + self.formatAsPercent(merged_data[val_col_name].iloc[0]) + '/'
            value_after_control += 'HK:' + self.formatAsPercent(merged_data[val_col_name].iloc[-1]) + '/'
            val_col_name = 'USMarket'
            value_before_control += 'US:' + self.formatAsPercent(merged_data[val_col_name].iloc[0]) + '/'
            value_after_control += 'US:' + self.formatAsPercent(merged_data[val_col_name].iloc[-1]) + '/'
            val_col_name = 'OthersMarket'
            value_before_control += 'Other:' + self.formatAsPercent(merged_data[val_col_name].iloc[0]) + '/'
            value_after_control += 'Other:' + self.formatAsPercent(merged_data[val_col_name].iloc[-1]) + '/'
        elif risk_limit_name_col == 'StockLimitStatus':
            val_col_name = 'NoOfStock'
            value_before_control = merged_data[val_col_name].iloc[0]
            value_after_control = merged_data[val_col_name].iloc[-1]
        elif risk_limit_name_col == 'BondHoldingLimitStatus':
            val_col_name = 'Bond'
            value_before_control = self.formatAsPercent(merged_data[val_col_name].iloc[0])
            value_after_control = self.formatAsPercent(merged_data[val_col_name].iloc[-1])
        elif risk_limit_name_col == 'DerivativeCashLimitStatus':
            val_col_name = 'DerivativeGross'
            value_before_control = self.formatAsPercent(merged_data[val_col_name].iloc[0])
            value_after_control = self.formatAsPercent(merged_data[val_col_name].iloc[-1])
        elif risk_limit_name_col == 'StockLongShortLimitStatus':
            val_col_name = 'StockLong'
            value_before_control = val_col_name + ':' + self.formatAsPercent(merged_data[val_col_name].iloc[0]) + '/'
            value_after_control = val_col_name + ':' + self.formatAsPercent(merged_data[val_col_name].iloc[-1]) + '/'

            val_col_name = 'StockShort'
            value_before_control += val_col_name + ':' + self.formatAsPercent(merged_data[val_col_name].iloc[0]) + '/'
            value_after_control += val_col_name + ':' + self.formatAsPercent(merged_data[val_col_name].iloc[-1]) + '/'
        elif risk_limit_name_col=='RiskControlStopLossNetLimitStatus':
            val_col_name = 'NetBeta'
            value_before_control = val_col_name+':'+self.formatAsPercent(merged_data[val_col_name].iloc[0])+ '/'
            value_after_control = val_col_name+':'+self.formatAsPercent(merged_data[val_col_name].iloc[-1])+ '/'

            val_col_name = 'MaxDd'
            value_before_control += val_col_name+':'+self.formatAsPercent(merged_data[val_col_name].iloc[0])+ '/'
            value_after_control += val_col_name+':'+self.formatAsPercent(merged_data[val_col_name].iloc[-1])+ '/'

            val_col_name = 'CurrDd'
            value_before_control += val_col_name+':'+self.formatAsPercent(merged_data[val_col_name].iloc[0])+ '/'
            value_after_control += val_col_name+':'+self.formatAsPercent(merged_data[val_col_name].iloc[-1])+ '/'

        return value_after_control, value_before_control

    def formatAsPercent(self,value):
        return "{:.2%}".format(value)
    def run(self, startDateStr, endDateStr,riskControlActionDaysLimit):
        self.initSqlServer('prod')
        self.analysis(startDateStr,endDateStr,riskControlActionDaysLimit)
        self.closeSqlServerConnection()


if __name__ == '__main__':
    riskControlSummary = RiskControlSummary()
    riskControlSummary.run('2019-01-01','2019-10-31',2)





