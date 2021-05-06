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

class RiksControlReportPMSFT05(Base):

    def __init__(self, env, fund, team):
        self.env = env
        self.team= team
        self.fund= fund
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
        sql = 'SELECT Id, FundId,BookId,FundCode,BookCode,LimitName,LimitValue,LimitDesc,LimitType,LimitStatus FROM RiskDb.ref.RiskControlSpecificTeamLimitInfo where BeginDate <=\''+dateStr+'\' AND EndDate >=\''+dateStr+'\''
        data = self.selectFromDataBase(sql)
        data['TeamLimitId'] = data['Id']
        del data['Id']
        data = data[(data['FundCode']==self.fund) & (data['BookCode']==self.team)]
        groupbyTeamData = data.groupby(['FundId', 'BookId', 'FundCode', 'BookCode', 'LimitName']).agg({'LimitValue': 'sum', 'LimitType': 'sum','LimitStatus':'sum'})
        groupbyTeamData = groupbyTeamData.reset_index()
        groupbyTeamData = groupbyTeamData.pivot_table(index=['FundId', 'BookId', 'FundCode', 'BookCode'], columns='LimitName', values='LimitValue', aggfunc='first').reset_index()
        return data,groupbyTeamData

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

        summaryInstrumentData = exposureData.groupby(['FundCode', 'BookCode', 'Instrument']).agg({'GrossNav': 'sum', 'LongNavBeta': 'sum', 'ShortNavBeta': 'sum', 'NetNav': 'sum'})
        summaryInstrumentData = summaryInstrumentData.reset_index()

        summaryIRSWInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'IR_SW'].copy()
        summaryIRSWInsData = summaryIRSWInsData.groupby(['FundCode', 'BookCode']).agg({'GrossNav': 'sum'})
        summaryIRSWInsData = summaryIRSWInsData.reset_index()
        summaryIRSWInsData['IR_SWInstrumentGrossNav'] = summaryIRSWInsData['GrossNav']
        del summaryIRSWInsData['GrossNav']

        summaryPV01InsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'PV01'].copy()
        summaryPV01InsData = summaryPV01InsData.groupby(['FundCode', 'BookCode']).agg({'GrossNav': 'sum'})
        summaryPV01InsData = summaryPV01InsData.reset_index()
        summaryPV01InsData['PV01InstrumentGrossNav'] = summaryPV01InsData['GrossNav']
        del summaryPV01InsData['GrossNav']

        summaryCCYInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'CCY'].copy()
        summaryCCYInsData = summaryCCYInsData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryCCYInsData = summaryCCYInsData.reset_index()
        summaryCCYInsData['CCYInstrumentNetNav'] = summaryCCYInsData['NetNav']
        del summaryCCYInsData['NetNav']

        summaryCCY_CNHInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'CCY_CNH'].copy()
        summaryCCY_CNHInsData = summaryCCY_CNHInsData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryCCY_CNHInsData = summaryCCY_CNHInsData.reset_index()
        summaryCCY_CNHInsData['CCY_CNHInstrumentNetNav'] = summaryCCY_CNHInsData['NetNav']
        del summaryCCY_CNHInsData['NetNav']

        summaryCCY_CNYInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'CCY_CNY'].copy()
        summaryCCY_CNYInsData = summaryCCY_CNYInsData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryCCY_CNYInsData = summaryCCY_CNYInsData.reset_index()
        summaryCCY_CNYInsData['CCY_CNYInstrumentNetNav'] = summaryCCY_CNYInsData['NetNav']
        del summaryCCY_CNYInsData['NetNav']

        summaryInstrumentInstrumentyTypeData = exposureData.groupby(
            ['FundCode', 'BookCode', 'Instrument', 'InstrumentType']).agg({'NetNav': 'sum'})
        summaryInstrumentInstrumentyTypeData = summaryInstrumentInstrumentyTypeData.reset_index()
        summaryFX_FTInsFOData = summaryInstrumentInstrumentyTypeData[
            (summaryInstrumentInstrumentyTypeData['Instrument'] == 'FX_FT') &
            (summaryInstrumentInstrumentyTypeData['InstrumentType'] == 'FO')].copy()
        summaryFX_FTInsFOData = summaryFX_FTInsFOData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryFX_FTInsFOData = summaryFX_FTInsFOData.reset_index()
        summaryFX_FTInsFOData['FX_FTFOInstrumentNetNav'] = summaryFX_FTInsFOData['NetNav']
        del summaryFX_FTInsFOData['NetNav']

        summaryFX_FT_CNHInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'FX_FT_CNH'].copy()
        summaryFX_FT_CNHInsData = summaryFX_FT_CNHInsData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryFX_FT_CNHInsData = summaryFX_FT_CNHInsData.reset_index()
        summaryFX_FT_CNHInsData['FX_FT_CNHInstrumentNetNav'] = summaryFX_FT_CNHInsData['NetNav']
        del summaryFX_FT_CNHInsData['NetNav']

        summaryFX_FT_CNYInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'FX_FT_CNY'].copy()
        summaryFX_FT_CNYInsData = summaryFX_FT_CNYInsData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryFX_FT_CNYInsData = summaryFX_FT_CNYInsData.reset_index()
        summaryFX_FT_CNYInsData['FX_FT_CNYInstrumentNetNav'] = summaryFX_FT_CNYInsData['NetNav']
        del summaryFX_FT_CNYInsData['NetNav']

        summaryFX_OPInsFOData = summaryInstrumentInstrumentyTypeData[
            (summaryInstrumentInstrumentyTypeData['Instrument'] == 'FX_OP') & (
                    summaryInstrumentInstrumentyTypeData['InstrumentType'] == 'FO')].copy()
        summaryFX_OPInsFOData = summaryFX_OPInsFOData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryFX_OPInsFOData = summaryFX_OPInsFOData.reset_index()
        summaryFX_OPInsFOData['FX_OPFOInstrumentNetNav'] = summaryFX_OPInsFOData['NetNav']
        del summaryFX_OPInsFOData['NetNav']

        summaryFX_OP_CNYInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'FX_OP_CNY'].copy()
        summaryFX_OP_CNYInsData = summaryFX_OP_CNYInsData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryFX_OP_CNYInsData = summaryFX_OP_CNYInsData.reset_index()
        summaryFX_OP_CNYInsData['FX_OP_CNYInstrumentNetNav'] = summaryFX_OP_CNYInsData['NetNav']
        del summaryFX_OP_CNYInsData['NetNav']

        summaryFX_OP_CNHInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'FX_OP_CNH'].copy()
        summaryFX_OP_CNHInsData = summaryFX_OP_CNHInsData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryFX_OP_CNHInsData = summaryFX_OP_CNHInsData.reset_index()
        summaryFX_OP_CNHInsData['FX_OP_CNHInstrumentNetNav'] = summaryFX_OP_CNHInsData['NetNav']
        del summaryFX_OP_CNHInsData['NetNav']

        summaryFWRD_FXInsFOData = summaryInstrumentInstrumentyTypeData[
            (summaryInstrumentInstrumentyTypeData['Instrument'] == 'FWRD_FX') &
            (summaryInstrumentInstrumentyTypeData['InstrumentType'] == 'FO')].copy()
        summaryFWRD_FXInsFOData = summaryFWRD_FXInsFOData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryFWRD_FXInsFOData = summaryFWRD_FXInsFOData.reset_index()
        summaryFWRD_FXInsFOData['FWRD_FXFOInstrumentNetNav'] = summaryFWRD_FXInsFOData['NetNav']
        del summaryFWRD_FXInsFOData['NetNav']

        summaryFWRD_FX_CNHInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'FWRD_FX_CNH'].copy()
        summaryFWRD_FX_CNHInsData = summaryFWRD_FX_CNHInsData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryFWRD_FX_CNHInsData = summaryFWRD_FX_CNHInsData.reset_index()
        summaryFWRD_FX_CNHInsData['FWRD_FX_CNHInstrumentNetNav'] = summaryFWRD_FX_CNHInsData['NetNav']
        del summaryFWRD_FX_CNHInsData['NetNav']

        summaryFWRD_FX_CNYInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'FWRD_FX_CNY'].copy()
        summaryFWRD_FX_CNYInsData = summaryFWRD_FX_CNYInsData.groupby(['FundCode', 'BookCode']).agg({'NetNav': 'sum'})
        summaryFWRD_FX_CNYInsData = summaryFWRD_FX_CNYInsData.reset_index()
        summaryFWRD_FX_CNYInsData['FWRD_FX_CNYInstrumentNetNav'] = summaryFWRD_FX_CNYInsData['NetNav']
        del summaryFWRD_FX_CNYInsData['NetNav']

        summaryFWRD_FXInsData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'FWRD_FX'].copy()
        summaryFWRD_FXInsData = summaryFWRD_FXInsData.groupby(['FundCode', 'BookCode']).agg({'GrossNav': 'sum'})
        summaryFWRD_FXInsData = summaryFWRD_FXInsData.reset_index()
        summaryFWRD_FXInsData['FWRD_FXInstrumentGrossNav'] = summaryFWRD_FXInsData['GrossNav']
        del summaryFWRD_FXInsData['GrossNav']

        summaryInstrumentLiqudityData = exposureData.groupby(['FundCode', 'BookCode', 'Instrument', 'Liqudity']).agg(
            {'GrossNav': 'sum'})
        summaryInstrumentLiqudityData = summaryInstrumentLiqudityData.reset_index()

        summaryBOND_LIQDTYIns10YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'BOND_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 10)].copy()
        summaryBOND_LIQDTYIns10YData = summaryBOND_LIQDTYIns10YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryBOND_LIQDTYIns10YData = summaryBOND_LIQDTYIns10YData.reset_index()
        summaryBOND_LIQDTYIns10YData['BOND_LIQDTYIns10YGrossNav'] = summaryBOND_LIQDTYIns10YData['GrossNav']
        del summaryBOND_LIQDTYIns10YData['GrossNav']

        summaryIR_LIQDTYIns10YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'IR_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 10)].copy()
        summaryIR_LIQDTYIns10YData = summaryIR_LIQDTYIns10YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryIR_LIQDTYIns10YData = summaryIR_LIQDTYIns10YData.reset_index()
        summaryIR_LIQDTYIns10YData['IR_LIQDTYIns10YGrossNav'] = summaryIR_LIQDTYIns10YData['GrossNav']
        del summaryIR_LIQDTYIns10YData['GrossNav']

        summaryGOVT_LIQDTYIns10YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'GOVT_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 10)].copy()
        summaryGOVT_LIQDTYIns10YData = summaryGOVT_LIQDTYIns10YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryGOVT_LIQDTYIns10YData = summaryGOVT_LIQDTYIns10YData.reset_index()
        summaryGOVT_LIQDTYIns10YData['GOVT_LIQDTYIns10YGrossNav'] = summaryGOVT_LIQDTYIns10YData['GrossNav']
        del summaryGOVT_LIQDTYIns10YData['GrossNav']

        summaryIRSW_LIQDTYIns10YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'IRSW_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 10)].copy()
        summaryIRSW_LIQDTYIns10YData = summaryIRSW_LIQDTYIns10YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryIRSW_LIQDTYIns10YData = summaryIRSW_LIQDTYIns10YData.reset_index()
        summaryIRSW_LIQDTYIns10YData['IRSW_LIQDTYIns10YGrossNav'] = summaryIRSW_LIQDTYIns10YData['GrossNav']
        del summaryIRSW_LIQDTYIns10YData['GrossNav']

        summaryBOND_LIQDTYIns5YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'BOND_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 5)].copy()
        summaryBOND_LIQDTYIns5YData = summaryBOND_LIQDTYIns5YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryBOND_LIQDTYIns5YData = summaryBOND_LIQDTYIns5YData.reset_index()
        summaryBOND_LIQDTYIns5YData['BOND_LIQDTYIns5YGrossNav'] = summaryBOND_LIQDTYIns5YData['GrossNav']
        del summaryBOND_LIQDTYIns5YData['GrossNav']

        summaryIR_LIQDTYIns5YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'IR_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 5)].copy()
        summaryIR_LIQDTYIns5YData = summaryIR_LIQDTYIns5YData.groupby(['FundCode', 'BookCode']).agg({'GrossNav': 'sum'})
        summaryIR_LIQDTYIns5YData = summaryIR_LIQDTYIns5YData.reset_index()
        summaryIR_LIQDTYIns5YData['IR_LIQDTYIns5YGrossNav'] = summaryIR_LIQDTYIns5YData['GrossNav']
        del summaryIR_LIQDTYIns5YData['GrossNav']

        summaryGOVT_LIQDTYIns5YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'GOVT_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 5)].copy()
        summaryGOVT_LIQDTYIns5YData = summaryGOVT_LIQDTYIns5YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryGOVT_LIQDTYIns5YData = summaryGOVT_LIQDTYIns5YData.reset_index()
        summaryGOVT_LIQDTYIns5YData['GOVT_LIQDTYIns5YGrossNav'] = summaryGOVT_LIQDTYIns5YData['GrossNav']
        del summaryGOVT_LIQDTYIns5YData['GrossNav']

        summaryIRSW_LIQDTYIns5YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'IRSW_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 5)].copy()
        summaryIRSW_LIQDTYIns5YData = summaryIRSW_LIQDTYIns5YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryIRSW_LIQDTYIns5YData = summaryIRSW_LIQDTYIns5YData.reset_index()
        summaryIRSW_LIQDTYIns5YData['IRSW_LIQDTYIns5YGrossNav'] = summaryIRSW_LIQDTYIns5YData['GrossNav']
        del summaryIRSW_LIQDTYIns5YData['GrossNav']

        summaryBOND_LIQDTYIns1YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'BOND_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 1)].copy()
        summaryBOND_LIQDTYIns1YData = summaryBOND_LIQDTYIns1YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryBOND_LIQDTYIns1YData = summaryBOND_LIQDTYIns1YData.reset_index()
        summaryBOND_LIQDTYIns1YData['BOND_LIQDTYIns1YGrossNav'] = summaryBOND_LIQDTYIns1YData['GrossNav']
        del summaryBOND_LIQDTYIns1YData['GrossNav']

        summaryIR_LIQDTYIns1YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'IR_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 1)].copy()
        summaryIR_LIQDTYIns1YData = summaryIR_LIQDTYIns1YData.groupby(['FundCode', 'BookCode']).agg({'GrossNav': 'sum'})
        summaryIR_LIQDTYIns1YData = summaryIR_LIQDTYIns1YData.reset_index()
        summaryIR_LIQDTYIns1YData['IR_LIQDTYIns1YGrossNav'] = summaryIR_LIQDTYIns1YData['GrossNav']
        del summaryIR_LIQDTYIns1YData['GrossNav']
        summaryGOVT_LIQDTYIns1YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'GOVT_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 1)].copy()
        summaryGOVT_LIQDTYIns1YData = summaryGOVT_LIQDTYIns1YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryGOVT_LIQDTYIns1YData = summaryGOVT_LIQDTYIns1YData.reset_index()
        summaryGOVT_LIQDTYIns1YData['GOVT_LIQDTYIns1YGrossNav'] = summaryGOVT_LIQDTYIns1YData['GrossNav']
        del summaryGOVT_LIQDTYIns1YData['GrossNav']
        summaryIRSW_LIQDTYIns1YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'IRSW_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 1)].copy()
        summaryIRSW_LIQDTYIns1YData = summaryIRSW_LIQDTYIns1YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryIRSW_LIQDTYIns1YData = summaryIRSW_LIQDTYIns1YData.reset_index()
        summaryIRSW_LIQDTYIns1YData['IRSW_LIQDTYIns1YGrossNav'] = summaryIRSW_LIQDTYIns1YData['GrossNav']
        del summaryIRSW_LIQDTYIns1YData['GrossNav']

        summaryFWRDFX_LIQDTYIns1YData = summaryInstrumentLiqudityData[
            (summaryInstrumentLiqudityData['Instrument'] == 'FWRDFX_LIQDTY') &
            (summaryInstrumentLiqudityData['Liqudity'] == 1)].copy()
        summaryFWRDFX_LIQDTYIns1YData = summaryFWRDFX_LIQDTYIns1YData.groupby(['FundCode', 'BookCode']).agg(
            {'GrossNav': 'sum'})
        summaryFWRDFX_LIQDTYIns1YData = summaryFWRDFX_LIQDTYIns1YData.reset_index()
        summaryFWRDFX_LIQDTYIns1YData['FWRDFX_LIQDTYIns1YGrossNav'] = summaryFWRDFX_LIQDTYIns1YData['GrossNav']
        del summaryFWRDFX_LIQDTYIns1YData['GrossNav']

        filterNACategoryData = exposureData[~exposureData['Category'].isna()].copy()
        filterOnlyEquityCategoryData = exposureData[exposureData['Category'] == 'Equity'].copy()
        groupedByCategoryData = filterNACategoryData.groupby(['FundCode', 'BookCode']).agg({'GrossNav': 'sum'})
        groupedByCategoryData = groupedByCategoryData.reset_index()
        groupedByCategoryData['TeamNotNACategoryGrossNAV'] = groupedByCategoryData['GrossNav']

        groupedByEquityCategoryData = filterOnlyEquityCategoryData.groupby(['FundCode', 'BookCode']).agg(
            {'NetNav': 'sum'})
        groupedByEquityCategoryData = groupedByEquityCategoryData.reset_index()
        groupedByEquityCategoryData['TeamEquityCategoryNetNAV'] = groupedByEquityCategoryData['NetNav']
        del groupedByEquityCategoryData['NetNav']

        summaryNumberOfIssuerData = summaryInstrumentData[summaryInstrumentData['Instrument'] == 'N_Issuer'].copy()
        summaryNumberOfIssuerData['NoOfIssuer'] = summaryNumberOfIssuerData['GrossNav']
        del summaryNumberOfIssuerData['GrossNav']

        if not summaryIRSWInsData.empty:
            specificTeamRiskReportData = pd.merge(groupedByCategoryData, summaryIRSWInsData[
                ['FundCode', 'BookCode', 'IR_SWInstrumentGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData = groupedByCategoryData.copy()
            specificTeamRiskReportData['IR_SWInstrumentGrossNav'] = 0

        if not summaryCCYInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData,summaryCCYInsData[['FundCode', 'BookCode', 'CCYInstrumentNetNav']],
                                                  how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['CCYInstrumentNetNav'] = 0

        if not summaryCCY_CNHInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryCCY_CNHInsData[['FundCode', 'BookCode', 'CCY_CNHInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['CCY_CNHInstrumentNetNav'] = 0

        if not summaryCCY_CNYInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryCCY_CNYInsData[
                ['FundCode', 'BookCode', 'CCY_CNYInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['CCY_CNYInstrumentNetNav'] = 0

        if not summaryFX_FTInsFOData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFX_FTInsFOData[
                ['FundCode', 'BookCode', 'FX_FTFOInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FX_FTFOInstrumentNetNav'] = 0

        if not summaryFX_FT_CNHInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFX_FT_CNHInsData[
                ['FundCode', 'BookCode', 'FX_FT_CNHInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FX_FT_CNHInstrumentNetNav'] = 0

        if not summaryFX_FT_CNYInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFX_FT_CNYInsData[
                ['FundCode', 'BookCode', 'FX_FT_CNYInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FX_FT_CNYInstrumentNetNav'] = 0

        if not summaryFX_OPInsFOData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFX_OPInsFOData[
                ['FundCode', 'BookCode', 'FX_OPFOInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FX_OPFOInstrumentNetNav'] = 0

        if not summaryFX_OP_CNYInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFX_OP_CNYInsData[
                ['FundCode', 'BookCode', 'FX_OP_CNYInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FX_OP_CNYInstrumentNetNav'] = 0

        if not summaryFX_OP_CNHInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFX_OP_CNHInsData[
                ['FundCode', 'BookCode', 'FX_OP_CNHInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FX_OP_CNHInstrumentNetNav'] = 0

        if not summaryFWRD_FXInsFOData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFWRD_FXInsFOData[
                ['FundCode', 'BookCode', 'FWRD_FXFOInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FWRD_FXFOInstrumentNetNav'] = 0

        if not summaryFWRD_FX_CNHInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFWRD_FX_CNHInsData[
                ['FundCode', 'BookCode', 'FWRD_FX_CNHInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FWRD_FX_CNHInstrumentNetNav'] = 0

        if not summaryFWRD_FX_CNYInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFWRD_FX_CNYInsData[
                ['FundCode', 'BookCode', 'FWRD_FX_CNYInstrumentNetNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FWRD_FX_CNYInstrumentNetNav'] = 0

        if not summaryPV01InsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryPV01InsData[
                ['FundCode', 'BookCode', 'PV01InstrumentGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['PV01InstrumentGrossNav'] = 0

        if not groupedByEquityCategoryData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, groupedByEquityCategoryData[
                ['FundCode', 'BookCode', 'TeamEquityCategoryNetNAV']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['TeamEquityCategoryNetNAV'] = 0

        if not summaryFWRD_FXInsData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFWRD_FXInsData[
                ['FundCode', 'BookCode', 'FWRD_FXInstrumentGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FWRD_FXInstrumentGrossNav'] = 0

        if not summaryBOND_LIQDTYIns10YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryBOND_LIQDTYIns10YData[
                ['FundCode', 'BookCode', 'BOND_LIQDTYIns10YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['BOND_LIQDTYIns10YGrossNav'] = 0

        if not summaryIR_LIQDTYIns10YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryIR_LIQDTYIns10YData[
                ['FundCode', 'BookCode', 'IR_LIQDTYIns10YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['IR_LIQDTYIns10YGrossNav'] = 0

        if not summaryGOVT_LIQDTYIns10YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryGOVT_LIQDTYIns10YData[
                ['FundCode', 'BookCode', 'GOVT_LIQDTYIns10YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['GOVT_LIQDTYIns10YGrossNav'] = 0

        if not summaryIRSW_LIQDTYIns10YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryIRSW_LIQDTYIns10YData[
                ['FundCode', 'BookCode', 'IRSW_LIQDTYIns10YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['IRSW_LIQDTYIns10YGrossNav'] = 0

        if not summaryBOND_LIQDTYIns5YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryBOND_LIQDTYIns5YData[
                ['FundCode', 'BookCode', 'BOND_LIQDTYIns5YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['BOND_LIQDTYIns5YGrossNav'] = 0

        if not summaryIR_LIQDTYIns5YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryIR_LIQDTYIns5YData[
                ['FundCode', 'BookCode', 'IR_LIQDTYIns5YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['IR_LIQDTYIns5YGrossNav'] = 0

        if not summaryGOVT_LIQDTYIns5YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryGOVT_LIQDTYIns5YData[
                ['FundCode', 'BookCode', 'GOVT_LIQDTYIns5YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['GOVT_LIQDTYIns5YGrossNav'] = 0

        if not summaryIRSW_LIQDTYIns5YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryIRSW_LIQDTYIns5YData[
                ['FundCode', 'BookCode', 'IRSW_LIQDTYIns5YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['IRSW_LIQDTYIns5YGrossNav'] = 0

        if not summaryBOND_LIQDTYIns1YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryBOND_LIQDTYIns1YData[
                ['FundCode', 'BookCode', 'BOND_LIQDTYIns1YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['BOND_LIQDTYIns1YGrossNav'] = 0

        if not summaryIR_LIQDTYIns1YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryIR_LIQDTYIns1YData[
                ['FundCode', 'BookCode', 'IR_LIQDTYIns1YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['IR_LIQDTYIns1YGrossNav'] = 0

        if not summaryGOVT_LIQDTYIns1YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryGOVT_LIQDTYIns1YData[
                ['FundCode', 'BookCode', 'GOVT_LIQDTYIns1YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['GOVT_LIQDTYIns1YGrossNav'] = 0

        if not summaryIRSW_LIQDTYIns1YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryIRSW_LIQDTYIns1YData[
                ['FundCode', 'BookCode', 'IRSW_LIQDTYIns1YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['IRSW_LIQDTYIns1YGrossNav'] = 0

        if not summaryFWRDFX_LIQDTYIns1YData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, summaryFWRDFX_LIQDTYIns1YData[
                ['FundCode', 'BookCode', 'FWRDFX_LIQDTYIns1YGrossNav']], how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['FWRDFX_LIQDTYIns1YGrossNav'] = 0

        if not summaryNumberOfIssuerData.empty:
            specificTeamRiskReportData = pd.merge(specificTeamRiskReportData,
                                                  summaryNumberOfIssuerData[['FundCode', 'BookCode', 'NoOfIssuer']],
                                                  how='left', on=['FundCode', 'BookCode'])
        else:
            specificTeamRiskReportData['NoOfIssuer'] = 0

        specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, varData[['FundCode', 'BookCode', 'VaR']],
                                              how='left', on=['FundCode', 'BookCode'])
        specificTeamRiskReportData = pd.merge(specificTeamRiskReportData, performanceData[
            ['FundCode', 'BookCode', 'MaxDd', 'CurrDd', 'HistHigh', 'Aum']], how='left', on=['FundCode', 'BookCode'])

        return specificTeamRiskReportData


    def saveSpecificTeamRiskStatus(self, dateStr, specificTeamRiskReportData, teamLimitRawData):
        colsDict = dict({
            'MRCGrossLeverage':'MRCGrossLeverageCeilingLimit','MRCPV01':'MRCPV01Limit',
            'MRCNetNav':'MRCNetNavLimit',    'MRCVaRAt95Level':'MRCVaRAt95LevelLimit',
            'CRCFXSpotNavCeiling':'CRCFXSpotNavCeilingLimit',
            'CRCCNYFXSpotNavCeiling':'CRCCNYFXSpotNavCeilingLimit',
            'RRCBondMatureG10Y':'RRCBondMatureG10YLimit',   'RRCBondMatureLE10YG5Y':'RRCBondMatureLE10YG5YLimit',
            'RRCBondMatureLE5YG1Y':'RRCBondMatureLE5YG1YLimit',    'RRCFXSwapG1Y':'RRCFXSwapG1YLimit',
            'RRCFXSwapLE1Y':'RRCFXSwapLE1YLimit',     'LRCInvestTotalNo':'LRCInvestTotalNoLimit',
            'SLRCPosMaxDD':'SLRCPosMaxDDLimit', 'SLRCMaxDD':'SLRCMaxDDLimit'})

        specificTeamRiskReportData['DateStr'] = dateStr
        specificTeamRiskReportData['ReportType'] = RiskCommonReportType.SPECIFIC_TEAM_REPORT.value
        allRecords = []
        for col, limitName in colsDict.items():
            specificTeamRiskReportData['ColSection'] = col[:3]
            specificTeamRiskReportData['ColName'] = col+'Value'
            specificTeamRiskReportData[col+'Value'] = specificTeamRiskReportData[col+'Value'].astype(float).round(6)
            specificTeamRiskReportData['ReportColLimitId'] = teamLimitRawData[(teamLimitRawData['LimitName'] == limitName)]['TeamLimitId'].iloc[0]
            recordsColumns = ['DateStr',  'FundId', 'BookId', 'FundCode', 'BookCode','ColSection', 'ColName', 'ReportColLimitId', col+'Value', col+'Value'+'Status','ReportType']
            records = pdUtil.dataFrameToSavableRecords(specificTeamRiskReportData, recordsColumns)
            allRecords += records
        if allRecords:
            self.removeTeamReport(dateStr, 'T05')
            sql = 'insert into RiskDb.risk.RiskCommonReport(AsOfDate,FundId,BookId,FundCode,BookCode,ReportColSection,ReportColName,ReportColLimitId,ReportColValue, ReportColLimitStatus,ReportType) values(?,?,?,?,?,?,?,?,?,?,?)'
            self.insertToDatabase(sql, allRecords)

    def removeTeamReport(self, dateStr, teamCode):
        sql = 'delete from RiskDb.risk.RiskCommonReport where BookCode=\''+teamCode+'\' and AsOfDate=\''+dateStr+'\' and ReportType='+str(RiskCommonReportType.SPECIFIC_TEAM_REPORT.value)
        self.cursor.execute(sql)

    def calcSpecificTeamRiskStatus(self, dateStr, specificTeamRiskReportData):
        specificTeamRiskReportData['MRCGrossLeverageValue'] = (specificTeamRiskReportData['TeamNotNACategoryGrossNAV'] / specificTeamRiskReportData['Aum']) - (specificTeamRiskReportData['IR_SWInstrumentGrossNav'] / specificTeamRiskReportData['Aum'] / 2)
        specificTeamRiskReportData['MRCPV01Value'] = specificTeamRiskReportData['PV01InstrumentGrossNav'] / specificTeamRiskReportData['Aum']
        specificTeamRiskReportData['MRCNetNavValue'] = specificTeamRiskReportData['TeamEquityCategoryNetNAV'] / specificTeamRiskReportData['Aum']
        specificTeamRiskReportData['MRCVaRAt95LevelValue'] = specificTeamRiskReportData['VaR'] / specificTeamRiskReportData['Aum']
        specificTeamRiskReportData['CRCFXSpotNavCeilingValue'] = ((specificTeamRiskReportData['CCYInstrumentNetNav'] - specificTeamRiskReportData['CCY_CNYInstrumentNetNav'] - specificTeamRiskReportData['CCY_CNHInstrumentNetNav']) / specificTeamRiskReportData['Aum']) + \
                                                                 ((specificTeamRiskReportData['FX_FTFOInstrumentNetNav'] - specificTeamRiskReportData['FX_FT_CNHInstrumentNetNav'] - specificTeamRiskReportData['FX_FT_CNYInstrumentNetNav']) / specificTeamRiskReportData['Aum']) + \
                                                                 ((specificTeamRiskReportData['FX_OPFOInstrumentNetNav'] - specificTeamRiskReportData['FX_OP_CNYInstrumentNetNav']-specificTeamRiskReportData['FX_OP_CNHInstrumentNetNav']) / specificTeamRiskReportData['Aum'] / 2) + \
                                                                  ((specificTeamRiskReportData['FWRD_FXFOInstrumentNetNav'] - specificTeamRiskReportData['FWRD_FX_CNHInstrumentNetNav'] - specificTeamRiskReportData['FWRD_FX_CNYInstrumentNetNav']) / specificTeamRiskReportData['Aum'] / 2)
        specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValueAbsComp1'] = (specificTeamRiskReportData['CCY_CNHInstrumentNetNav'] + specificTeamRiskReportData['FX_FT_CNHInstrumentNetNav'] + specificTeamRiskReportData['FWRD_FX_CNHInstrumentNetNav'] + specificTeamRiskReportData['FX_OP_CNHInstrumentNetNav']).abs()
        specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValueAbsComp2'] = (specificTeamRiskReportData['CCY_CNYInstrumentNetNav'] + specificTeamRiskReportData['FX_FT_CNYInstrumentNetNav'] + specificTeamRiskReportData['FWRD_FX_CNYInstrumentNetNav'] + specificTeamRiskReportData['FX_OP_CNYInstrumentNetNav']).abs()
        specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValue'] = np.where(specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValueAbsComp1']<specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValueAbsComp2'], specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValueAbsComp1']/specificTeamRiskReportData['Aum'], specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValueAbsComp2']/specificTeamRiskReportData['Aum'])
        specificTeamRiskReportData['RRCBondMatureG10YValue'] = specificTeamRiskReportData['BOND_LIQDTYIns10YGrossNav'] / specificTeamRiskReportData['Aum'] + specificTeamRiskReportData['IR_LIQDTYIns10YGrossNav'] / specificTeamRiskReportData['Aum'] + specificTeamRiskReportData['GOVT_LIQDTYIns10YGrossNav'] / specificTeamRiskReportData['Aum'] + specificTeamRiskReportData['IRSW_LIQDTYIns10YGrossNav'] / specificTeamRiskReportData['Aum'] / 2
        specificTeamRiskReportData['RRCBondMatureLE10YG5YValue'] = (specificTeamRiskReportData['BOND_LIQDTYIns5YGrossNav'] + specificTeamRiskReportData['IR_LIQDTYIns5YGrossNav'] + specificTeamRiskReportData['GOVT_LIQDTYIns5YGrossNav']) / specificTeamRiskReportData['Aum'] + specificTeamRiskReportData['IRSW_LIQDTYIns5YGrossNav'] / specificTeamRiskReportData['Aum'] / 2 - specificTeamRiskReportData['RRCBondMatureG10YValue']
        specificTeamRiskReportData['RRCBondMatureLE5YG1YValue'] = (specificTeamRiskReportData['BOND_LIQDTYIns1YGrossNav'] + specificTeamRiskReportData['IR_LIQDTYIns1YGrossNav'] + specificTeamRiskReportData['GOVT_LIQDTYIns1YGrossNav']) / specificTeamRiskReportData['Aum'] + specificTeamRiskReportData['IRSW_LIQDTYIns1YGrossNav'] / specificTeamRiskReportData['Aum'] / 2 - specificTeamRiskReportData['RRCBondMatureLE10YG5YValue']
        specificTeamRiskReportData['RRCFXSwapG1YValue'] = specificTeamRiskReportData['FWRDFX_LIQDTYIns1YGrossNav'] / specificTeamRiskReportData['Aum'] / 2
        specificTeamRiskReportData['RRCFXSwapLE1YValue'] = (specificTeamRiskReportData['FWRD_FXInstrumentGrossNav'] - specificTeamRiskReportData['FWRDFX_LIQDTYIns1YGrossNav']) / specificTeamRiskReportData['Aum'] /2 - specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValue']*2
        # specificTeamRiskReportData['LRCInvestInEachExchFloorValue'] =
        specificTeamRiskReportData['LRCInvestTotalNoValue'] = specificTeamRiskReportData['NoOfIssuer']
        specificTeamRiskReportData['SLRCPosMaxDDValue'] = specificTeamRiskReportData['CurrDd']
        specificTeamRiskReportData['SLRCMaxDDValue'] =specificTeamRiskReportData['MaxDd']

        (teamLimitRawData, teamLimitData) = self.getSecificTeamLimit(dateStr)
        specificTeamRiskReportData = pd.merge(teamLimitData, specificTeamRiskReportData, how='left', on=['FundCode','BookCode'])
        specificTeamRiskReportData['MRCGrossLeverageValueStatus'] = np.where(specificTeamRiskReportData['MRCGrossLeverageValue'] > specificTeamRiskReportData['MRCGrossLeverageCeilingLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['MRCPV01ValueStatus'] = np.where(specificTeamRiskReportData['MRCPV01Value'] > specificTeamRiskReportData['MRCPV01Limit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['MRCNetNavValueStatus'] = np.where(specificTeamRiskReportData['MRCNetNavValue'] > specificTeamRiskReportData['MRCNetNavLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['MRCVaRAt95LevelValueStatus'] = np.where(specificTeamRiskReportData['MRCVaRAt95LevelValue'] > specificTeamRiskReportData['MRCVaRAt95LevelLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['CRCFXSpotNavCeilingValueStatus'] = np.where(specificTeamRiskReportData['CRCFXSpotNavCeilingValue'] > specificTeamRiskReportData['CRCFXSpotNavCeilingLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValueStatus'] = np.where(specificTeamRiskReportData['CRCCNYFXSpotNavCeilingValue'] > specificTeamRiskReportData['CRCCNYFXSpotNavCeilingLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['RRCBondMatureG10YValueStatus'] = np.where(specificTeamRiskReportData['RRCBondMatureG10YValue'] > specificTeamRiskReportData['RRCBondMatureG10YLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['RRCBondMatureLE10YG5YValueStatus'] = np.where(specificTeamRiskReportData['RRCBondMatureLE10YG5YValue'] > specificTeamRiskReportData['RRCBondMatureLE10YG5YLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['RRCBondMatureLE5YG1YValueStatus'] = np.where(specificTeamRiskReportData['RRCBondMatureLE5YG1YValue'] > specificTeamRiskReportData['RRCBondMatureLE5YG1YLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['RRCFXSwapG1YValueStatus'] = np.where(specificTeamRiskReportData['RRCFXSwapG1YValue'] > specificTeamRiskReportData['RRCFXSwapG1YLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['RRCFXSwapLE1YValueStatus'] = np.where(specificTeamRiskReportData['RRCFXSwapLE1YValue'] > specificTeamRiskReportData['RRCFXSwapLE1YLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['LRCInvestTotalNoValueStatus'] = np.where(specificTeamRiskReportData['LRCInvestTotalNoValue'] > specificTeamRiskReportData['LRCInvestTotalNoLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['SLRCPosMaxDDValueStatus'] = np.where(specificTeamRiskReportData['SLRCPosMaxDDValue'] < specificTeamRiskReportData['SLRCPosMaxDDLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        specificTeamRiskReportData['SLRCMaxDDValueStatus'] = np.where(specificTeamRiskReportData['SLRCMaxDDValue'] < specificTeamRiskReportData['SLRCMaxDDLimit'],
                                                                                  RiskControlStatus.NEED_RISK_CONTROL.value,
                                                                                  RiskControlStatus.PASS.value)
        return (teamLimitRawData, specificTeamRiskReportData)

    def runWithDateRange(self,dateStr):
        self.initSqlServer(self.env)
        specificTeamRiskReportData = self.prepareTeamSpecificExposureData(dateStr)
        (teamLimitRawData, specificTeamRiskReportResultData) = self.calcSpecificTeamRiskStatus(dateStr, specificTeamRiskReportData)
        self.saveSpecificTeamRiskStatus(dateStr, specificTeamRiskReportResultData, teamLimitRawData)
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
    riskControlReports = RiksControlReportPMSFT05(env, 'PMSF', 'T05')
    riskControlReports.runWithDateRange('2019-05-07')
