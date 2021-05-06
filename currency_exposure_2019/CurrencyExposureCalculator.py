# encoding:UTF-8
from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import pandas as pd
import logging
from benchmark.marketdata.MarketDataDownloader import *
from benchmark.currency_exposure_2019.MarketValueCalculator import *
from benchmark.base.CommonEnums import RiskCommonReportType
import FXRateChecker
import datetime
from decimal import *
import statsmodels.api as sm
getcontext().prec = 32
import sys

class CurrencyExposureCalculator(Base):
    def __init__(self,env):
        self.env = env
        self.currency_pos_detail_float_cols = ['Beta', 'DeltaNotnlMktValLocal',  'Exposure',
                                            'QuantityEnd', 'PriceLocal', 'PriceBook', 'FxRateLocalBook', 'MktValLocal', 'MktValBook', 'NotnlMktValLocal', 'NotnlMktValBook',
                                               'TotalAccrualsBook', 'TotalAccrualsLocal',
                                           'ContractSize','UnderlyingPricingMultiplier', 'UnderlyingContractSize','ConversionRatio','CostLocal']
        self.currency_pos_detail_cols = self.currency_pos_detail_float_cols + ['UnderlyingPnlCcyCode','Ticker','ExposureName', 'SecurityId', 'Date', 'FundId', 'BookId', 'Fund', 'Book', 'PositionTypeCode',
                                                                               'UnderlyingSecurityId', 'UnderlyingTicker', 'PrincipalCcyCode', 'PnlCcyCode', 'InstrumentClass',
                                                                               'UnderlyingInstrumentClass',  'UnderlyingPrincipalCcyCode','PositionId','ExposureType']
        self.currency_pos_detail_db_cols = ['Beta', 'DeltaNotnlMktValLocal', 'ExposureValue',
                                            'QuantityEnd', 'PriceLocal', 'PriceBook', 'FxRateLocalBook', 'MktValLocal', 'MktValBook', 'NotnlMktValLocal',
                                            'NotnlMktValBook', 'TotalAccrualsBook', 'TotalAccrualsLocal',
                                            'ContractSize','UnderlyingPricingMultiplier', 'UnderlyingContractSize', 'ConversionRatio', 'CostLocal',

                                            'UnderlyingPnlCcyCode', 'Ticker','ExposureName', 'SecurityId', 'AsOfDate', 'FundId', 'BookId', 'FundCode',
                                            'BookCode', 'PositionTypeCode','UnderlyingSecurityId', 'UnderlyingTicker', 'PrincipalCcyCode',
                                           'PnlCcyCode', 'InstrumentClass','UnderlyingInstrumentClass',  'UnderlyingPrincipalCcyCode','PositionId','ExposureType']
        LogManager('CurrencyExposureCalculator')
        self.groupbyCols=['Fund','Book','FundId','BookId']

    def insertToDatabase(self,sql,data):
        if data:
            self.cursor.executemany(sql, data)
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def selectFromDataBase(self,sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def querySecurityBeta(self, asOfDateStr):
        sql = '''
              select cp.Last AS EndPriceLocal, cp.SecurityId, (COALESCE(
                            ca.BETA,
                            CASE WHEN (ca.BETA_OVERRIDABLE > 3.9) THEN 3.9
                            WHEN ca.BETA_OVERRIDABLE < -2.5 THEN -2.5
                            ELSE ca.BETA_OVERRIDABLE
                            END,
                            ca.BETA_6_MONTH)
                    +
                    COALESCE(
                            ca.BETA_6_MONTH,
                            ca.BETA,
                            CASE WHEN ca.BETA_OVERRIDABLE > 3.9 THEN 3.9
                            WHEN ca.BETA_OVERRIDABLE < -2.5 THEN -2.5
                            ELSE ca.BETA_OVERRIDABLE
                            END)
                ) * 0.5 AS Beta, ca.Delta from MarketData.mark.ufn_latest_prices (?, 5) cp LEFT JOIN MarketData.mark.ufn_latest_analytics(?, 5) ca ON cp.SecurityId = ca.SecurityID
              '''
        self.cursor.execute(sql, (asOfDateStr,asOfDateStr))
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def getSecurityInfo(self):
        sql = 'SELECT s.SecurityId, s.DisplayCode AS SecurityCode, s.SecurityDesc, s.BB_TCM AS Ticker, s.ISIN AS Isin, s.ExternalInstClass AS InstrumentClass, s.PricingMultiplier, s.ContractSize, s.ExchangeCode, s.IssuerCode, s.PrincipalCurrencyCode AS PrincipalCcyCode, s.PnlCurrencyCode AS PnlCcyCode, ISNULL(s.QuotedAsMinorCurrency, 0) AS QuotedAsMinorCurrency, s.UnderlyingSecurityId, s.UnderlyingDisplayCode AS UnderlyingSecurityCode, s.UnderlyingBB_TCM AS UnderlyingTicker, s.UnderlyingISIN AS UnderlyingIsin, s.UnderlyingExternalInstClass AS UnderlyingInstrumentClass, s.UnderlyingPricingMultiplier, s.UnderlyingContractSize, s.UnderlyingPrincipalCurrencyCode AS UnderlyingPrincipalCcyCode, s.UnderlyingPnlCurrencyCode AS UnderlyingPnlCcyCode, ISNULL(s.UnderlyingQuotedAsMinorCurrency, 0) AS UnderlyingQuotedAsMinorCurrency FROM SecurityMaster.sm.SecurityView s where 1 = 1'

    def getFxRateData(self,dateStr):
        sql = 'SELECT FxRateId,Date,FromCurrencyId,FromCurrencyCode,ToCurrencyId,ToCurrencyCode,Last FROM MarketData.mark.FxRateView where Date=\''+dateStr+'\''
        return self.selectFromDataBase(sql)

    def getFxEODPrice(self, dateStr, currencyTickers):
        sql = 'SELECT A.CurrencyCode,TradeDate,ClosePrice,B.PriceSize FROM RiskDb.bench.CurrencyEodPrice A left join RiskDb.ref.CurrencyInfo B on B.CurrencyCode=A.CurrencyCode where A.TradeDate=\''+dateStr+'\''
        if currencyTickers:
            sql += ' and A.CurrencyCode in (\'' + ('\',\'').join(currencyTickers)+'\')'
        data = self.selectFromDataBase(sql)
        tickerWOMDList = [item for item in currencyTickers if item not in list(data['CurrencyCode'].unique())]
        tickerWOQuoteFactorList = list(data[data['PriceSize'].isna()]['CurrencyCode'].unique())
        self.getCurrencyTickerMD(dateStr, tickerWOMDList, tickerWOQuoteFactorList)

        readyData = self.selectFromDataBase(sql)
        readyData['ClosePrice'] = readyData['ClosePrice'] / readyData['PriceSize']
        return readyData

    def getFXQuoteFactor(self, tickers):
        sql = 'SELECT CurrencyCode,PriceSize as QuotationFactor FROM RiskDb.ref.CurrencyInfo '
        if tickers:
            sql += 'where CurrencyCode in (\'' + ('\',\'').join(tickers)+'\')'
        return self.selectFromDataBase(sql)

    def getCurrencyTickerMD(self, datestr, currencyTickerList, tickerWOQuoteFactorList):
        tickerQuoteFactorsData = self.getFXQuoteFactor(currencyTickerList+tickerWOQuoteFactorList)
        tickerWOQuoteFactorList = [item for item in currencyTickerList if item not in list(set(tickerQuoteFactorsData['CurrencyCode'].unique()))]
        mdService = MarketDataDownloader(self.env)
        mdService.initSqlServer(self.env)
        mdService.getFXQuoteFactorFromBBG(tickerWOQuoteFactorList)
        mdService.getCurrencEodPrice(datestr, datestr, currencyTickerList)
        mdService.closeSqlServerConnection()

    def getPositionSecurityInfo(self, dateStr, fundList, bookList):
        sql = 'SELECT P.CostLocal, S.ExternalAssetClass, P.AvgCostLocal,P.Security,P.Currency,P.Fund,P.FundIsReal,P.BaseCurrency,P.Book,P.QuantityDirection,P.SecurityId,P.TxnSecurityId,P.CurrencyId,P.FundId,P.BookId,P.PositionTypeCode,P.Strategy,P.ExternalContractId,P.Date,P.QuantityStart,P.QuantityEnd,P.PriceLocal,P.PriceBook,P.PriceLocalStart,P.PriceBookStart,P.FxRateLocalBook,P.FxRateLocalBookStart,P.MktValLocal,P.MktValBook,P.MktValLocalStart,P.MktValBookStart,P.NotnlMktValLocal,P.NotnlMktValBook,P.NotnlMktValLocalStart,P.NotnlMktValBookStart,P.TotalAccrualsBook,P.TotalAccrualsLocal, S.DisplayCode AS SecurityCode, S.BB_TCM AS Ticker, S.ExternalInstClass AS InstrumentClass, S.PricingMultiplier, S.ContractSize, S.ExchangeCode, S.IssuerCode, S.PrincipalCurrencyCode AS PrincipalCcyCode, S.PnlCurrencyCode AS PnlCcyCode, ISNULL(S.QuotedAsMinorCurrency, 0) AS QuotedAsMinorCurrency, S.UnderlyingSecurityId, S.UnderlyingDisplayCode AS UnderlyingSecurityCode, S.UnderlyingBB_TCM AS UnderlyingTicker, S.UnderlyingISIN AS UnderlyingIsin, S.UnderlyingExternalInstClass AS UnderlyingInstrumentClass, S.UnderlyingPricingMultiplier, S.UnderlyingContractSize, S.UnderlyingPrincipalCurrencyCode AS UnderlyingPrincipalCcyCode, S.UnderlyingPnlCurrencyCode AS UnderlyingPnlCcyCode, ISNULL(S.UnderlyingQuotedAsMinorCurrency, 0) AS UnderlyingQuotedAsMinorCurrency, K.Last, S.ConversionRatio, U.Last as UnderlyingLast FROM Portfolio.pos.PositionView P'
        sql += ' LEFT JOIN SecurityMaster.sm.SecurityView S on P.SecurityId=S.SecurityId'
        sql += ' LEFT JOIN MarketData.mark.Price K on K.Date=P.Date and P.SecurityId=K.SecurityId'
        sql += ' LEFT JOIN MarketData.mark.Price U on U.Date=P.Date and S.UnderlyingSecurityId=U.SecurityId'
        sql += ' where P.FundIsReal = 1 AND P.QuantityDirection not like \'FLAT%\' AND P.PositionTypeCode not in (\'STLN\', \'NOTNL_CSH\',\'PmManagementFee\')'
        sql += ' AND P.Date=\''+dateStr+'\''
        if fundList:
            sql += ' and P.Fund in (\'' + ('\',\'').join(fundList)+'\') '
        if bookList:
            sql += ' and P.Book in (\'' + ('\',\'').join(bookList) + '\') '

        data = self.selectFromDataBase(sql)
        data['UnderlyingSecurityId'] = data['UnderlyingSecurityId'].fillna(0)
        data['UnderlyingSecurityId'] = data['UnderlyingSecurityId'].astype(int)
        return data

    def processData(self, dateStr, fundList, teamList, fundTeamList):
        if fundTeamList:
            fundList = []
            teamList = []
            for fundTeam in fundTeamList:
                fundList.append(fundTeam.split('-')[0])
                teamList.append(fundTeam.split('-')[1])

        posAndSecdata = self.getPositionSecurityInfo(dateStr, fundList, teamList)
        fxRateData = self.getFxRateData(dateStr)
        securityBetaData = self.querySecurityBeta(dateStr)
        dataFundList = list(posAndSecdata['Fund'].unique())
        dataBookList = list(posAndSecdata['Book'].unique())
        if fundList:
            standbyFundPosAndSecData = posAndSecdata[posAndSecdata['Fund'].isin(fundList)].copy()
        else:
            standbyFundPosAndSecData = posAndSecdata.copy()

        securityBetaData.drop_duplicates(subset=['SecurityId'], inplace=True)
        fundPosAndSecData = pd.merge(standbyFundPosAndSecData, securityBetaData[['SecurityId', 'Beta', 'Delta']], how='left', on=['SecurityId'])
        underlyingSecurityBetaData = securityBetaData.copy()
        underlyingSecurityBetaData['UnderlyingSecurityId'] = underlyingSecurityBetaData['SecurityId']
        underlyingSecurityBetaData['UnderlyingBeta'] = underlyingSecurityBetaData['Beta']
        underlyingSecurityBetaData['UnderlyingDelta'] = underlyingSecurityBetaData['Delta']
        underlyingSecurityBetaData['UnderlyingEndPriceLocal'] = underlyingSecurityBetaData['EndPriceLocal']
        del underlyingSecurityBetaData['SecurityId']
        del underlyingSecurityBetaData['Beta']
        del underlyingSecurityBetaData['Delta']
        del underlyingSecurityBetaData['EndPriceLocal']
        underlyingSecurityBetaData.drop_duplicates(subset=['UnderlyingSecurityId'], inplace=True)
        fundPosAndSecData = pd.merge(fundPosAndSecData, underlyingSecurityBetaData, how='left', on=['UnderlyingSecurityId'])

        defaultCalculator = DefaultCalculator()
        fundPosAndSecData  = defaultCalculator.calcFXRate(fundPosAndSecData, fxRateData,dateStr)
        ##defaultCalculator.calcNotnlMarketValue(fundPosAndSecData)
        defaultCalculator.calcBetaNotnlMarketValue(fundPosAndSecData)
        #defaultCalculator.calcMarketValue(fundPosAndSecData)
        if fundTeamList:
            for fund in dataFundList:
                for book in dataBookList:
                    fundBookPosAndSecData = fundPosAndSecData[(fundPosAndSecData['Fund'] == fund) & (fundPosAndSecData['Book'] == book)].copy()
                    if not fundBookPosAndSecData.empty:
                        logging.info('calcCurrencyExposure fund=' + fund + ', book=' + book)
                        reportResult,posDetailRecords = self.calcCurrencyExposure(fundBookPosAndSecData, fxRateData, dateStr)
                        self.saveExposureReport(reportResult)
        elif teamList:
            #self.groupbyCols = ['Book','BookId']
            for book in dataBookList:
                fundBookPosAndSecData = fundPosAndSecData[fundPosAndSecData['Book'] == book].copy()
                fundList = list(fundBookPosAndSecData['Fund'].unique())
                for fund in fundList:
                    logging.info('calcCurrencyExposure book=' + book+', and fund='+fund)
                    fundBookPosAndSecData = fundBookPosAndSecData[fundBookPosAndSecData['Fund']==fund].copy()
                    if not fundBookPosAndSecData.empty:
                        reportResult,posDetailRecords = self.calcCurrencyExposure(fundBookPosAndSecData, fxRateData, dateStr)
                        self.saveExposureReport(reportResult)
                        try:
                            self.saveCurrencyExposurePosDetails(posDetailRecords)
                        except Exception,e:
                            logging.error(e.args[1])
                            raise Exception('fail to save pos details')
        elif fundList:
            self.groupbyCols = ['Fund','FundId']
            for fund in dataFundList:
                fundBookPosAndSecData = fundPosAndSecData[fundPosAndSecData['Fund'] == fund].copy()
                if not fundBookPosAndSecData.empty:
                    logging.info('calcCurrencyExposure fund='+fund)
                    reportResult,posDetailRecords = self.calcCurrencyExposure(fundBookPosAndSecData, fxRateData, dateStr)
                    self.saveExposureReport(reportResult)

    def saveExposureReport(self, data_frame):
        if not data_frame.empty:
            data_frame['ReportType'] = RiskCommonReportType.CURRENCY_EXPOSURE_REPORT.value
            data_frame['DateStr'] = pd.to_datetime(data_frame['Date']).dt.strftime('%Y-%m-%d')
            data_frame['Exposure'] = data_frame['Exposure'].astype(float).round(6)
            records = pdUtil.dataFrameToSavableRecords(data_frame, self.groupbyCols+['DateStr', 'ExposureName', 'Exposure', 'ReportType'])
            columsList = [x+'Code' if (x == 'Fund') or (x == 'Book') else x for x in self.groupbyCols]
            sql = 'insert into RiskDb.risk.RiskCommonReport('+(',').join(columsList)+', AsOfDate, ReportColName, ReportColValue, ReportType) values('+('?,'*(len(columsList)+4))[:-1]+')'
            self.insertToDatabase(sql, records)

    def saveCurrencyExposurePosDetails(self, records):
        if records:
            sql = 'insert into RiskDb.risk.RiskCurrencyExposurePos(' + (',').join(self.currency_pos_detail_db_cols) + ') values(' + ('?,' * (len(self.currency_pos_detail_db_cols)))[:-1] + ')'
            self.insertToDatabase(sql, records)
            # for record in records:
            #     try:
            #         self.cursor.execute(sql, (record))
            #     except Exception, e:
            #         logging.error(record)

    def calcCurrencyExposure(self, data, fxRateData, dateStr):
        data['PositionId'] = np.where(data['Ticker'].isna(),data['UnderlyingTicker'],data['Ticker'])
        baseCurrency = data['BaseCurrency'].iloc[0]

        ###Equity Cash
        equityCashData = data[(data['ExternalAssetClass'] == 'EQTY') & (data['InstrumentClass'].isin(['DR', 'ORD'])) & (data['PositionTypeCode']=='TRD')].copy()
        ###Swap Data
        swapData = data[(data['ExternalAssetClass'] == 'SWAP')].copy()
        CFDSwapData = swapData[swapData['InstrumentClass'].isin(['CFD', 'EQ_SW'])].copy()
        equityAndBondSwapData = swapData[swapData['InstrumentClass'].isin(['EQINDX_SW'])].copy()
        #bondSwapData = swapData[swapData['InstrumentClass']=='BOND_SW'].copy()
        ###bond Data
        bondData = data[(data['ExternalAssetClass'] == 'FIXED_INCOME') & (data['InstrumentClass'] == 'BOND')].copy()
        ###OPTION Data
        optionData = data[data['ExternalAssetClass'] == 'OPTION'].copy()
        vanillaOptionData = optionData[optionData['InstrumentClass'].isin(['EQINDX_OP', 'EQTY_OP'])]  ###FX_OP??? OP_IRFT???


        ###Future Data
        futureData = data[data['ExternalAssetClass'] == 'FUTURE'].copy()   #####BOND_FT, CMDTY_FT, EQINDX_FT, EQTY_FT, IR_FT
        ##bondFutureData =futureData[futureData['InstrumentClass'] == 'BOND_FT'].copy()
        vanillaBondAndFutureData = futureData[futureData['InstrumentClass'].isin(['EQINDX_FT', 'EQTY_FT', 'BOND_FT'])].copy()

        convtBondData = data[(data['ExternalAssetClass'] == 'FIXED_INCOME') & (data['InstrumentClass'] == 'CONVBND')].copy()

        fxOptionData = optionData[optionData['InstrumentClass'].isin(['FX_OP'])]


        irsAndFxSwapData = swapData[swapData['InstrumentClass'].isin(['IR_SW', 'CDS'])].copy()

        ###FX Data
        fxData = data[data['ExternalAssetClass'] == 'CCY'].copy()
        fxData_TRD_CSH=fxData[(fxData['PositionTypeCode']=='TRD_CSH') | (fxData['PositionTypeCode'].isin(['FWRD_HEDGE', 'FWRD_PROP', 'FX_PROP'])) & (fxData['InstrumentClass'] == 'CCY')].copy()


        otherData = data.copy()
        otherData = otherData.append(equityCashData)
        otherData = otherData.append(CFDSwapData)
        otherData = otherData.append(equityAndBondSwapData)
        otherData = otherData.append(bondData)
        otherData = otherData.append(vanillaOptionData)
        otherData = otherData.append(vanillaBondAndFutureData)
        otherData = otherData.append(convtBondData)
        otherData = otherData.append(fxOptionData)
        otherData = otherData.append(irsAndFxSwapData)
        otherData = otherData.append(fxData)
        otherData = otherData.drop_duplicates(keep=False)
        if not otherData.empty:
            print 'other DAta is not emtpy'+dateStr
        posDetailRecords=[]

        '''1. 普通 Equity : cash'''
        equityCashData.loc[:, ('ExposureName')] = equityCashData['Currency'] +' Exposure'
        equityCashData.loc[:, ('Exposure')] = equityCashData['MktValLocal']
        if not equityCashData.empty:
            equityCashData.loc[:, ('ExposureType')] = 'Market Value(local)'

        posDetailRecords = self.join_pos_deatails(equityCashData, posDetailRecords)

        ####SecurityCurrency to BookCurrency
        equityCashDataBaseCurrency = equityCashData.copy()
        equityCashDataBaseCurrency.loc[:, ('ExposureName')] = equityCashDataBaseCurrency['ExposureName'] + ' (Base)'
        equityCashDataBaseCurrency.loc[:, ('Exposure')] = equityCashDataBaseCurrency['Exposure'] * equityCashDataBaseCurrency['FxRateLocalBook']
        if not equityCashDataBaseCurrency.empty:
            equityCashDataBaseCurrency.loc[:, ('ExposureType')] = 'Market Value(local) * FX'

        # '''2. Equity/Bond Swap except margin(PositionTypeCode=NOTNL_CSH)： Asset Currency == Settlement Currency'''
        # eqCurrnyBwSettleUndlAssetData = equityAndBondSwapData[equityAndBondSwapData['Currency'] == equityAndBondSwapData['UnderlyingPnlCcyCode']].copy()
        # eqCurrnyBwSettleUndlAssetData.loc[:, ('ExposureName')] = eqCurrnyBwSettleUndlAssetData['Currency'] +' Exposure'
        # #eqCurrnyBwSettleUndlAssetData.loc[:, ('Exposure')] = eqCurrnyBwSettleUndlAssetData['MarketValueLocal'] - eqCurrnyBwSettleUndlAssetData['BeginMarketValueLocal']
        # eqCurrnyBwSettleUndlAssetData.loc[:, ('Exposure')] = eqCurrnyBwSettleUndlAssetData['NotnlMktValLocal'] - eqCurrnyBwSettleUndlAssetData['MktValLocalStart']
        #
        # posDetailRecords = self.join_pos_deatails(eqCurrnyBwSettleUndlAssetData, posDetailRecords)
        #
        # eqCurrnyBwSettleUndlAssetDataBaseCurrency = eqCurrnyBwSettleUndlAssetData.copy()
        # if not eqCurrnyBwSettleUndlAssetDataBaseCurrency.empty:
        #     eqCurrnyBwSettleUndlAssetDataBaseCurrency.loc[:, ('ExposureName')] = eqCurrnyBwSettleUndlAssetDataBaseCurrency['ExposureName'] + ' (Base)'
        #     eqCurrnyBwSettleUndlAssetDataBaseCurrency.loc[:, ('Exposure')] = eqCurrnyBwSettleUndlAssetDataBaseCurrency['Exposure'] * equityCashDataBaseCurrency['FxRateLocalBook']
        #

        '''3. Equity/Bond Swap except margin(PositionTypeCode=NOTNL_CSH)： Asset Currency != Settlement Currency'''
        ineqCurrnyBwSettleUndlAssetData = equityAndBondSwapData.copy()
        fxRateData['UnderlyingPnlCcyCode'] = fxRateData['FromCurrencyCode']
        fxRateData['Currency'] = fxRateData['ToCurrencyCode']
        fxRateData['FXLast'] = fxRateData['Last']

        fxRateData.drop_duplicates(subset=['UnderlyingPnlCcyCode', 'Currency'], inplace=True)
        ineqCurrnyBwSettleUndlAssetData = pd.merge(ineqCurrnyBwSettleUndlAssetData, fxRateData[['UnderlyingPnlCcyCode', 'Currency', 'FXLast']], how='left', on=['UnderlyingPnlCcyCode', 'Currency'])

        ineqCurrnyBwSettleUndlAssetData.loc[:, ('ExposureName')] = ineqCurrnyBwSettleUndlAssetData['Currency'] + ' Exposure'
        ineqCurrnyBwSettleUndlAssetData.loc[:, ('Exposure')] = ineqCurrnyBwSettleUndlAssetData['MktValLocal']
        if not ineqCurrnyBwSettleUndlAssetData.empty:
            ineqCurrnyBwSettleUndlAssetData.loc[:, ('ExposureType')] = 'Market Value(local)'

        posDetailRecords = self.join_pos_deatails(ineqCurrnyBwSettleUndlAssetData, posDetailRecords)

        ineqCurrnyBwSettleUndlAssetDataBaseCurrency = ineqCurrnyBwSettleUndlAssetData.copy()
        if not ineqCurrnyBwSettleUndlAssetDataBaseCurrency.empty:
            ineqCurrnyBwSettleUndlAssetDataBaseCurrency.loc[:, ('ExposureName')] = ineqCurrnyBwSettleUndlAssetDataBaseCurrency['ExposureName'] + ' (Base)'
            ineqCurrnyBwSettleUndlAssetDataBaseCurrency.loc[:, ('Exposure')] = ineqCurrnyBwSettleUndlAssetDataBaseCurrency['Exposure'] * ineqCurrnyBwSettleUndlAssetDataBaseCurrency['FxRateLocalBook']
            ineqCurrnyBwSettleUndlAssetDataBaseCurrency.loc[:, ('ExposureType')] = 'Market Value(local) * FX'

        # ineqCurrnySettleData = ineqCurrnyBwSettleUndlAssetData.copy()
        #
        # ineqCurrnySettleData = FXRateChecker.calFXRateWithColNames(ineqCurrnySettleData, fxRateData, 'Currency', 'UnderlyingPnlCcyCode')
        #
        # ineqCurrnySettleData.loc[:, ('ExposureName')] = ineqCurrnySettleData['UnderlyingPnlCcyCode']+' Exposure'
        # #ineqCurrnySettleData.loc[:, ('Exposure')] = ineqCurrnySettleData['MarketValueBook'] - ineqCurrnySettleData['BeginMarketValueBook']
        # fxColName = 'CurrencyToUnderlyingPnlCcyCodeLast'
        # ineqCurrnySettleData.loc[:, ('Exposure')] = (ineqCurrnySettleData['NotnlMktValLocal'] - ineqCurrnySettleData['NotnlMktValLocalStart']) * ineqCurrnySettleData[fxColName]
        #
        # posDetailRecords = self.join_pos_deatails(ineqCurrnySettleData, posDetailRecords)
        #
        # ineqCurrnySettleDataBaseCurrency = ineqCurrnySettleData.copy()
        # if not ineqCurrnySettleDataBaseCurrency.empty:
        #     ineqCurrnySettleDataBaseCurrency.loc[:, ('ExposureName')] = ineqCurrnySettleDataBaseCurrency['ExposureName'] + ' (Base)'
        #     ineqCurrnySettleDataBaseCurrency.loc[:, ('Exposure')] = ineqCurrnySettleDataBaseCurrency['Exposure'] * ineqCurrnySettleDataBaseCurrency['FxRateLocalBook']


        ##TBC:PrincipalCcyCode：本金货币 PnlCcyCode: 利息货币???
        '''4. 普通bond: USD Bond?  计价和计息货币一致的? 两个取其一？'''
        vanillaUSDBondData = bondData.copy()
        vanillaUSDBondData.loc[:, ('ExposureName')] = vanillaUSDBondData['Currency'] + ' Exposure'
        vanillaUSDBondData.loc[:, ('Exposure')] = vanillaUSDBondData['MktValLocal']
        if not vanillaUSDBondData.empty:
            vanillaUSDBondData.loc[:, ('ExposureType')] = 'Market Value(local)'

        posDetailRecords = self.join_pos_deatails(vanillaUSDBondData, posDetailRecords)

        vanillaUSDBondDataBaseCurrency = vanillaUSDBondData.copy()
        if not vanillaUSDBondDataBaseCurrency.empty:
            vanillaUSDBondDataBaseCurrency.loc[:, ('ExposureName')] = vanillaUSDBondDataBaseCurrency['ExposureName'] + ' (Base)'
            vanillaUSDBondDataBaseCurrency.loc[:, ('Exposure')] = vanillaUSDBondDataBaseCurrency['Exposure'] * vanillaUSDBondDataBaseCurrency['FxRateLocalBook']
            vanillaUSDBondDataBaseCurrency.loc[:, ('ExposureType')] = 'Market Value(local) * FX'


        '''5. Option'''  ###TBC: 计算方法待确定
        vanillaOptionData.loc[:, ('ExposureName')] = vanillaOptionData['Currency'] +' Exposure'
        vanillaOptionData.loc[:, ('Exposure')] = vanillaOptionData['MktValLocal']
        if not vanillaOptionData.empty:
            vanillaOptionData.loc[:, ('ExposureType')] = 'Market Value(local)'

        posDetailRecords = self.join_pos_deatails(vanillaOptionData, posDetailRecords)

        vanillaOptionDataBaseCurrency = vanillaOptionData.copy()
        if not vanillaOptionDataBaseCurrency.empty:
            vanillaOptionDataBaseCurrency.loc[:, ('ExposureName')] = vanillaOptionDataBaseCurrency['ExposureName'] + ' (Base)'
            vanillaOptionDataBaseCurrency.loc[:, ('Exposure')] = vanillaOptionDataBaseCurrency['Exposure'] * vanillaOptionDataBaseCurrency['FxRateLocalBook']
            vanillaOptionDataBaseCurrency.loc[:, ('ExposureType')] = 'Market Value(local) * FX'

        '''6. Future: Equity Future and Bond Future'''
        vanillaBondAndFutureData.loc[:, ('ExposureName')] = vanillaBondAndFutureData['Currency'] + ' Exposure'
        vanillaBondAndFutureData.loc[:, ('Exposure')] = vanillaBondAndFutureData['MktValLocal']  #NotnlMktValLocal-Cost  local  （leo提醒直接用MktValLocal较准确）
        if not vanillaBondAndFutureData.empty:
            vanillaBondAndFutureData.loc[:, ('ExposureType')] = 'Market Value(local)'

        posDetailRecords = self.join_pos_deatails(vanillaBondAndFutureData, posDetailRecords)

        vanillaBondAndFutureDataBaseCurrency = vanillaBondAndFutureData.copy()
        #vanillaBondAndFutureDataBaseCurrency.to_excel('C:\\temp\\vanillaBondAndFutureDataBaseCurrency1.xlsx')
        if not vanillaBondAndFutureDataBaseCurrency.empty:
            vanillaBondAndFutureDataBaseCurrency.loc[:, ('ExposureName')] = vanillaBondAndFutureDataBaseCurrency['ExposureName'] + ' (Base)'
            vanillaBondAndFutureDataBaseCurrency.loc[:, ('Exposure')] = vanillaBondAndFutureDataBaseCurrency['Exposure'] * vanillaBondAndFutureDataBaseCurrency['FxRateLocalBook']
            vanillaBondAndFutureDataBaseCurrency.loc[:, ('ExposureType')] = 'Market Value(local) * FX'

        #vanillaBondAndFutureDataBaseCurrency.to_excel('C:\\temp\\vanillaBondAndFutureDataBaseCurrency2.xlsx')
        ''' 7. Convertible bond'''   ####数据库无conversion ration 信息
        eqCurrnyConvtBondData = convtBondData[convtBondData['Currency'] == convtBondData['UnderlyingPnlCcyCode']].copy()
        eqCurrnyConvtBondData.loc[:, ('ExposureName')] = eqCurrnyConvtBondData['Currency'] + ' Exposure'
        eqCurrnyConvtBondData.loc[:, ('Exposure')] = eqCurrnyConvtBondData['MktValLocal']
        if not eqCurrnyConvtBondData.empty:
            eqCurrnyConvtBondData.loc[:, ('ExposureType')] = 'Market Value(local)'

        posDetailRecords = self.join_pos_deatails(eqCurrnyConvtBondData, posDetailRecords)

        eqCurrnyConvtBondDataBaseCurrency = eqCurrnyConvtBondData.copy()
        if not eqCurrnyConvtBondDataBaseCurrency.empty:
            eqCurrnyConvtBondDataBaseCurrency.loc[:, ('ExposureName')] = eqCurrnyConvtBondDataBaseCurrency['ExposureName'] + ' (Base)'
            eqCurrnyConvtBondDataBaseCurrency.loc[:, ('Exposure')] = eqCurrnyConvtBondDataBaseCurrency['Exposure'] *  eqCurrnyConvtBondDataBaseCurrency['FxRateLocalBook']
            eqCurrnyConvtBondDataBaseCurrency.loc[:, ('ExposureType')] = 'Market Value(local) * FX'


        ineqCurrnyConvtBondData1 = convtBondData[convtBondData['Currency'] != convtBondData['UnderlyingPnlCcyCode']].copy()
        ineqCurrnyConvtBondData1.loc[:, ('ExposureName')] = ineqCurrnyConvtBondData1['Currency'] + ' Exposure'
        ineqCurrnyConvtBondData1.loc[:, ('Exposure')] = ineqCurrnyConvtBondData1['MktValLocal'] - (ineqCurrnyConvtBondData1['QuantityEnd'] * ineqCurrnyConvtBondData1['ConversionRatio'] * ineqCurrnyConvtBondData1['Delta'] * ineqCurrnyConvtBondData1['UnderlyingLast'])#ineqCurrnyConvtBondData1['UnderlyingEndPriceLocal'])
        if not ineqCurrnyConvtBondData1.empty:
            ineqCurrnyConvtBondData1.loc[:, ('ExposureType')] = 'Market Value(local) - (Qty*ConvRatio*Delta*UnderlyingPx)'


        posDetailRecords = self.join_pos_deatails(ineqCurrnyConvtBondData1, posDetailRecords)

        ineqCurrnyConvtBondData1BaseCurrency = ineqCurrnyConvtBondData1.copy()
        if not ineqCurrnyConvtBondData1BaseCurrency.empty:
            ineqCurrnyConvtBondData1BaseCurrency.loc[:, ('ExposureName')] = ineqCurrnyConvtBondData1BaseCurrency['ExposureName'] + ' (Base)'
            ineqCurrnyConvtBondData1BaseCurrency.loc[:, ('Exposure')] = ineqCurrnyConvtBondData1BaseCurrency['Exposure'] * ineqCurrnyConvtBondData1BaseCurrency['FxRateLocalBook']
            ineqCurrnyConvtBondData1BaseCurrency.loc[:,('ExposureType')] = '(Market Value(local) - (Qty*ConvRatio*Delta*UnderlyingPx)) * FX'

        ineqCurrnyConvtBondData2 = convtBondData[convtBondData['Currency'] != convtBondData['UnderlyingPnlCcyCode']].copy()
        ineqCurrnyConvtBondData2['ExposureName'] = ineqCurrnyConvtBondData2['UnderlyingPnlCcyCode'] + ' Exposure'
        ineqCurrnyConvtBondData2['Exposure'] = ineqCurrnyConvtBondData2['QuantityEnd'] * ineqCurrnyConvtBondData2['ConversionRatio'] * ineqCurrnyConvtBondData2['Delta'] * ineqCurrnyConvtBondData2['UnderlyingLast']#ineqCurrnyConvtBondData2['UnderlyingEndPriceLocal']
        if not ineqCurrnyConvtBondData2.empty:
            ineqCurrnyConvtBondData2.loc[:, ('ExposureType')] = 'Qty*ConvRatio*Delta*UnderlyingPx'

        posDetailRecords = self.join_pos_deatails(ineqCurrnyConvtBondData2, posDetailRecords)

        ineqCurrnyConvtBondData2BaseCurrency = ineqCurrnyConvtBondData2.copy()
        if not ineqCurrnyConvtBondData2BaseCurrency.empty:
            ineqCurrnyConvtBondData2BaseCurrency.loc[:, ('ExposureName')] = ineqCurrnyConvtBondData2BaseCurrency['ExposureName'] + ' (Base)'
            ineqCurrnyConvtBondData2BaseCurrency.loc[:, ('Exposure')] = ineqCurrnyConvtBondData2BaseCurrency['Exposure'] * ineqCurrnyConvtBondData2BaseCurrency['FxRateUnderlyingToBaseCurrny']
            ineqCurrnyConvtBondData2BaseCurrency.loc[:, ('ExposureType')] = 'Qty*ConvRatio*Delta*UnderlyingPx*FX'

        ineqCurrnyConvtBondDataStandby = pd.concat([ineqCurrnyConvtBondData1, ineqCurrnyConvtBondData2], axis=0, sort=True)
        ineqCurrnyConvtBondData = ineqCurrnyConvtBondDataStandby.groupby(self.groupbyCols + ['Date','ExposureName','QuantityDirection']).agg({'Exposure': 'sum'})
        ineqCurrnyConvtBondData = ineqCurrnyConvtBondData.reset_index()

        ''' 8. Bond Future: ref 6'''
        ''' 9: Bond Swap: ref 2,3'''
        ''' 10: FX Option'''
        fxOptionData.loc[:, 'ExposureName'] = fxOptionData['Currency'] + ' Exposure'
        fxOptionData.loc[:, 'Exposure'] = fxOptionData['QuantityEnd'] * fxOptionData['Delta'] + fxOptionData['MktValLocal']
        if not fxOptionData.empty:
            fxOptionData.loc[:, ('ExposureType')] = 'Qty*Delta+MktValue'

        posDetailRecords = self.join_pos_deatails(fxOptionData, posDetailRecords)
        fxOptionDataBaseCurrency = fxOptionData.copy()
        if not fxOptionDataBaseCurrency.empty:
            fxOptionDataBaseCurrency.loc[:, ('ExposureName')] = fxOptionDataBaseCurrency['ExposureName'] + ' (Base)'
            #fxOptionDataBaseCurrency.loc[:, ('Exposure')] = fxOptionDataBaseCurrency['Exposure'] * fxOptionDataBaseCurrency['FxRateLocalBook']
            fxOptionDataBaseCurrency['DeltaFloat'] =fxOptionDataBaseCurrency['Delta'].astype(float)
            fxOptionDataBaseCurrency.loc[:, ('Exposure')] = np.where(fxOptionDataBaseCurrency['DeltaFloat'].isna(),0,-(fxOptionDataBaseCurrency['QuantityEnd'] * fxOptionDataBaseCurrency['Delta'] * fxOptionDataBaseCurrency['FxRateLocalBook']))
            fxOptionData.loc[:, ('ExposureType')] = 'Qty*(-Delta)*FX'


        ''' 11: IRS/FX Swap '''
        irsAndFxSwapData.loc[:, 'ExposureName'] = irsAndFxSwapData['Currency'] + ' Exposure'
        irsAndFxSwapData.loc[:, 'Exposure'] = irsAndFxSwapData['MktValLocal']
        if not irsAndFxSwapData.empty:
            irsAndFxSwapData.loc[:, ('ExposureType')] = 'Market Value(local)'

        posDetailRecords = self.join_pos_deatails(irsAndFxSwapData, posDetailRecords)

        irsAndFxSwapDataBaseCurrency = irsAndFxSwapData.copy()
        if not irsAndFxSwapDataBaseCurrency.empty:
            irsAndFxSwapDataBaseCurrency.loc[:, ('ExposureName')] = irsAndFxSwapDataBaseCurrency['ExposureName'] + ' (Base)'
            irsAndFxSwapDataBaseCurrency.loc[:, ('Exposure')] = irsAndFxSwapDataBaseCurrency['Exposure'] * irsAndFxSwapDataBaseCurrency['FxRateLocalBook']
            irsAndFxSwapDataBaseCurrency.loc[:, ('ExposureType')] = 'Market Value(local) * FX'

        ''' 12: CCY FX Asset'''
        fxData_TRD_CSH.loc[:, 'ExposureName'] = fxData_TRD_CSH['Currency'] + ' Exposure'
        fxData_TRD_CSH.loc[:, 'Exposure'] = fxData_TRD_CSH['QuantityEnd']
        if not fxData_TRD_CSH.empty:
            fxData_TRD_CSH.loc[:, ('ExposureType')] = 'Qty'

        posDetailRecords = self.join_pos_deatails(fxData_TRD_CSH, posDetailRecords)

        fxDataBaseCurrency = fxData_TRD_CSH.copy()
        if not fxDataBaseCurrency.empty:
            fxDataBaseCurrency.loc[:, ('ExposureName')] = fxDataBaseCurrency['ExposureName'] + ' (Base)'
            fxDataBaseCurrency.loc[:, ('Exposure')] = fxDataBaseCurrency['Exposure'] * fxDataBaseCurrency['FxRateLocalBook']
            fxData_TRD_CSH.loc[:, ('ExposureType')] = 'Qty * FX'

        fxExposureData = fxData_TRD_CSH.groupby(self.groupbyCols + ['Date', 'ExposureName', 'QuantityDirection']).agg({'Exposure': 'sum'})
        fxExposureData = fxExposureData.reset_index()

        '''  13, SWAP CFD'''
        CFDSwapDataWithSameCurrency = CFDSwapData[CFDSwapData['Currency']==CFDSwapData['UnderlyingPnlCcyCode']].copy()
        CFDSwapDataWithSameCurrency.loc[:, 'ExposureName'] = CFDSwapDataWithSameCurrency['Currency'] + ' Exposure'
        CFDSwapDataWithSameCurrency.loc[:, 'Exposure'] = CFDSwapDataWithSameCurrency['MktValLocal']
        if not CFDSwapDataWithSameCurrency.empty:
            CFDSwapDataWithSameCurrency.loc[:, ('ExposureType')] = 'Market Value(local)'

        posDetailRecords = self.join_pos_deatails(CFDSwapDataWithSameCurrency, posDetailRecords)

        CFDSwapDataWithSameCurrencyBaseCurrency = CFDSwapDataWithSameCurrency.copy()
        if not CFDSwapDataWithSameCurrencyBaseCurrency.empty:
            CFDSwapDataWithSameCurrencyBaseCurrency.loc[:, ('ExposureName')] = CFDSwapDataWithSameCurrencyBaseCurrency['ExposureName'] + ' (Base)'
            CFDSwapDataWithSameCurrencyBaseCurrency.loc[:, ('Exposure')] = CFDSwapDataWithSameCurrencyBaseCurrency['Exposure'] * CFDSwapDataWithSameCurrencyBaseCurrency['FxRateLocalBook']
            CFDSwapDataWithSameCurrencyBaseCurrency.loc[:, ('ExposureType')] = 'Market Value(local) * FX'

        CFDSwapDataWithDiffCurrency = CFDSwapData[CFDSwapData['Currency'] != CFDSwapData['UnderlyingPnlCcyCode']].copy()
        CFDSwapDataWithDiffCurrency.loc[:, 'ExposureName'] = CFDSwapDataWithDiffCurrency['UnderlyingPnlCcyCode'] + ' Exposure'
        CFDSwapDataWithDiffCurrency.loc[:, 'Exposure'] = CFDSwapDataWithDiffCurrency['QuantityEnd'] * CFDSwapDataWithDiffCurrency['UnderlyingLast'] *  CFDSwapDataWithDiffCurrency['UnderlyingPricingMultiplier']
        if not CFDSwapDataWithDiffCurrency.empty:
            CFDSwapDataWithDiffCurrency.loc[:, ('ExposureType')] = 'Qty * UnderlyingPX * Multiplier'

        posDetailRecords = self.join_pos_deatails(CFDSwapDataWithDiffCurrency, posDetailRecords)

        CFDSwapDataWithDiffCurrencyBaseCurrency = CFDSwapDataWithDiffCurrency.copy()
        if not CFDSwapDataWithDiffCurrencyBaseCurrency.empty:
            CFDSwapDataWithDiffCurrencyBaseCurrency.loc[:, ('ExposureName')] = CFDSwapDataWithDiffCurrencyBaseCurrency['ExposureName'] + ' (Base)'
            CFDSwapDataWithDiffCurrencyBaseCurrency.loc[:, ('Exposure')] = CFDSwapDataWithDiffCurrencyBaseCurrency['Exposure'] * CFDSwapDataWithDiffCurrencyBaseCurrency['FxRateUnderlyingToBaseCurrny']
            CFDSwapDataWithDiffCurrencyBaseCurrency.loc[:, ('ExposureType')] = 'Qty * UnderlyingPX * Multiplier * FX'


        CFDSwapDataWithDiffCurrency2 = CFDSwapData[CFDSwapData['Currency'] != CFDSwapData['UnderlyingPnlCcyCode']].copy()
        CFDSwapDataWithDiffCurrency2.loc[:, 'ExposureName'] = CFDSwapDataWithDiffCurrency2['Currency'] + ' Exposure'
        CFDSwapDataWithDiffCurrency2.loc[:, 'Exposure'] = CFDSwapDataWithDiffCurrency2['MktValLocal']
        if not CFDSwapDataWithDiffCurrency2.empty:
            CFDSwapDataWithDiffCurrency2.loc[:, ('ExposureType')] = 'Market Value(local)'

        posDetailRecords = self.join_pos_deatails(CFDSwapDataWithDiffCurrency2, posDetailRecords)

        CFDSwapDataWithDiffCurrencyBaseCurrency2 = CFDSwapDataWithDiffCurrency2.copy()
        if not CFDSwapDataWithDiffCurrencyBaseCurrency2.empty:
            CFDSwapDataWithDiffCurrencyBaseCurrency2.loc[:, ('ExposureName')] = CFDSwapDataWithDiffCurrencyBaseCurrency2['ExposureName'] + ' (Base)'
            CFDSwapDataWithDiffCurrencyBaseCurrency2.loc[:, ('Exposure')] = CFDSwapDataWithDiffCurrencyBaseCurrency2['Exposure'] * CFDSwapDataWithDiffCurrencyBaseCurrency2['FxRateLocalBook']
            CFDSwapDataWithDiffCurrencyBaseCurrency2.loc[:, ('ExposureType')] = 'Market Value(local) * FX'

        otherData.loc[:, 'ExposureName'] = otherData['Currency'] + ' Exposure'
        otherData.loc[:, 'Exposure'] = otherData['MktValLocal']
        if not otherData.empty:
            otherData.loc[:, ('ExposureType')] = 'Market Value(local)'
        posDetailRecords = self.join_pos_deatails(otherData, posDetailRecords)
        otherDataBaseCurrency = otherData.copy()
        if not otherDataBaseCurrency.empty:
            otherDataBaseCurrency.loc[:, ('ExposureName')] = otherDataBaseCurrency['ExposureName'] + ' (Base)'
            otherDataBaseCurrency.loc[:, ('Exposure')] = otherDataBaseCurrency['Exposure'] * otherDataBaseCurrency['FxRateLocalBook']
            otherDataBaseCurrency.loc[:, ('ExposureType')] = 'Market Value(local) * FX'

        commonCols = self.groupbyCols + ['Date', 'ExposureName', 'Exposure', 'QuantityDirection']
        allResultData = pd.concat([equityCashData[commonCols], equityCashDataBaseCurrency[commonCols]], axis=0, sort=True)
        # if not eqCurrnyBwSettleUndlAssetDataBaseCurrency.empty:
        #     allResultData = pd.concat([allResultData, eqCurrnyBwSettleUndlAssetDataBaseCurrency[commonCols]], axis=0, sort=True)
        # if not equityCashDataBaseCurrency.empty:
        #     allResultData = pd.concat([allResultData, equityCashDataBaseCurrency[commonCols]], axis=0, sort=True)

        allResultData = pd.concat([allResultData, ineqCurrnyBwSettleUndlAssetData[commonCols]], axis=0, sort=True)
        if not ineqCurrnyBwSettleUndlAssetDataBaseCurrency.empty:
            allResultData = pd.concat([allResultData, ineqCurrnyBwSettleUndlAssetDataBaseCurrency[commonCols]], axis=0, sort=True)

        # allResultData = pd.concat([allResultData, ineqCurrnySettleData[commonCols]], axis=0, sort=True)
        # if not ineqCurrnySettleDataBaseCurrency.empty:
        #     allResultData = pd.concat([allResultData, ineqCurrnySettleDataBaseCurrency[commonCols]], axis=0, sort=True)

        allResultData = pd.concat([allResultData, vanillaUSDBondData[commonCols]], axis=0, sort=True)
        if not vanillaUSDBondDataBaseCurrency.empty:
            allResultData = pd.concat([allResultData, vanillaUSDBondDataBaseCurrency[commonCols]], axis=0, sort=True)

        allResultData = pd.concat([allResultData, vanillaOptionData[commonCols]], axis=0, sort=True)
        if not vanillaOptionDataBaseCurrency.empty:
            allResultData = pd.concat([allResultData, vanillaOptionDataBaseCurrency[commonCols]], axis=0, sort=True)

        allResultData = pd.concat([allResultData, vanillaBondAndFutureData[commonCols]], axis=0, sort=True)
        if not vanillaBondAndFutureDataBaseCurrency.empty:
            allResultData = pd.concat([allResultData, vanillaBondAndFutureDataBaseCurrency[commonCols]], axis=0, sort=True)

        allResultData = pd.concat([allResultData, fxOptionData[commonCols]], axis=0, sort=True)
        if not fxOptionDataBaseCurrency.empty:
            allResultData = pd.concat([allResultData, fxOptionDataBaseCurrency[commonCols]], axis=0, sort=True)
        allResultData = pd.concat([allResultData, irsAndFxSwapData[commonCols]], axis=0, sort=True)
        if not irsAndFxSwapDataBaseCurrency.empty:
            allResultData = pd.concat([allResultData, irsAndFxSwapDataBaseCurrency[commonCols]], axis=0, sort=True)

        if not ineqCurrnyConvtBondData.empty:
            allResultData = pd.concat([allResultData, ineqCurrnyConvtBondData[commonCols]], axis=0, sort=True)
        if not ineqCurrnyConvtBondData2BaseCurrency.empty:
            allResultData = pd.concat([allResultData, ineqCurrnyConvtBondData2BaseCurrency[commonCols]], axis=0, sort=True)
        if not ineqCurrnyConvtBondData1BaseCurrency.empty:
            allResultData = pd.concat([allResultData, ineqCurrnyConvtBondData1BaseCurrency[commonCols]], axis=0, sort=True)

        if not eqCurrnyConvtBondData.empty:
            allResultData = pd.concat([allResultData, eqCurrnyConvtBondData[commonCols]], axis=0, sort=True)
        if not eqCurrnyConvtBondDataBaseCurrency.empty:
            allResultData = pd.concat([allResultData, eqCurrnyConvtBondDataBaseCurrency[commonCols]], axis=0, sort=True)


        if not fxExposureData.empty:
            allResultData = pd.concat([allResultData, fxExposureData[commonCols]], axis=0, sort=True)
        if not fxDataBaseCurrency.empty:
            allResultData = pd.concat([allResultData, fxDataBaseCurrency[commonCols]], axis=0, sort=True)


        if not CFDSwapDataWithSameCurrency.empty:
            allResultData = pd.concat([allResultData, CFDSwapDataWithSameCurrency[commonCols]], axis=0, sort=True)
        if not CFDSwapDataWithSameCurrencyBaseCurrency.empty:
            allResultData = pd.concat([allResultData, CFDSwapDataWithSameCurrencyBaseCurrency[commonCols]], axis=0, sort=True)

        if not CFDSwapDataWithDiffCurrency.empty:
            allResultData = pd.concat([allResultData, CFDSwapDataWithDiffCurrency[commonCols]], axis=0, sort=True)
        if not CFDSwapDataWithDiffCurrency2.empty:
            allResultData = pd.concat([allResultData, CFDSwapDataWithDiffCurrency2[commonCols]], axis=0, sort=True)
        if not CFDSwapDataWithDiffCurrencyBaseCurrency.empty:
            allResultData = pd.concat([allResultData, CFDSwapDataWithDiffCurrencyBaseCurrency[commonCols]], axis=0, sort=True)
        if not CFDSwapDataWithDiffCurrencyBaseCurrency2.empty:
            allResultData = pd.concat([allResultData, CFDSwapDataWithDiffCurrencyBaseCurrency2[commonCols]], axis=0, sort=True)

        if not otherData.empty:
            allResultData = pd.concat([allResultData, otherData[commonCols]], axis=0, sort=True)
        if not otherDataBaseCurrency.empty:
            allResultData = pd.concat([allResultData, otherDataBaseCurrency[commonCols]], axis=0, sort=True)


        groupbyAllResultData = allResultData.groupby(self.groupbyCols + ['Date', 'ExposureName']).agg({'Exposure': 'sum'})
        groupbyAllResultData = groupbyAllResultData.reset_index()
        return groupbyAllResultData, posDetailRecords

    def join_pos_deatails(self, data_frame, posDetailRecords):
        if not data_frame.empty:
            posDetailData = data_frame[data_frame['Exposure'] != 0].copy()
            posDetailData[self.currency_pos_detail_float_cols] = posDetailData[self.currency_pos_detail_float_cols].astype(float).round(3)
            posDetailData[self.currency_pos_detail_float_cols] = posDetailData[self.currency_pos_detail_float_cols].fillna(0)
            posDetailData[['SecurityId','FundId', 'BookId','UnderlyingSecurityId']] = posDetailData[['SecurityId','FundId', 'BookId','UnderlyingSecurityId']].fillna(0)
            posDetailData = posDetailData[posDetailData['Exposure'] != 0].copy()
            posDetailRecords += pdUtil.dataFrameToSavableRecords(posDetailData, self.currency_pos_detail_cols)
        return posDetailRecords

    def getTeam(self):
        #sql = 'SELECT distinct BookCode FROM RiskDb.ref.Book'
        sql = 'SELECT distinct BookCode FROM RiskDb.ref.Book where BookCode not like \'V%\'  and BookCode not like \'TEAM%\' and BookCode not like \'A%\' and BookCode not like \'PT%\' and BookCode not like \'P%\' and BookCode not in (\'QD\',\'CICC\',\'01\',\'GTJA\',\'BOOK\',\'CASH\',\'CASH2\',\'DACHENG\',\'GUANGFA\',\'GUOTOURUIYIN\',\'INVESTMENT\',\'RLPL\',\'RLPL_ADJ\',\'S01\',\'S02\')'
        data = self.selectFromDataBase(sql)
        return list(data['BookCode'].unique())
    def getFund(self):
        sql = 'SELECT distinct FundCode FROM RiskDb.ref.Fund where IsActive=1 and IsReal=1 and FundCode not in(\'PTF\',\'CPTF\')'
        data = self.selectFromDataBase(sql)
        return list(data['FundCode'].unique())

    def cleanGivenDateData(self,dateStr):
        sql = 'delete from RiskDb.risk.RiskCommonReport where ReportType=2 and AsOfDate=\''+dateStr+'\''
        self.cursor.execute(sql)
        sql2 = 'delete FROM RiskDb.risk.RiskCurrencyExposurePos where AsOfDate=\''+dateStr+'\''
        self.cursor.execute(sql2)

    def runWithDateRange(self, startDateStr, stopDateStr ):
        self.initSqlServer(self.env)
        startDate = datetime.datetime.strptime(startDateStr, '%Y-%m-%d')
        stopDate = datetime.datetime.strptime(stopDateStr, '%Y-%m-%d')
        while (startDate <= stopDate):
            if (startDate.weekday() >= 0 and startDate.weekday() <= 4):
                dateStr = startDate.strftime('%Y-%m-%d')
                logging.info('re-run date='+dateStr)
                teamList = self.getTeam()
                fundList = self.getFund()
                # fundList =['PMSF']
                # teamList = ['T14']
                #fundList = ['PMSF']
                #teamList = ['T17']
                self.cleanGivenDateData(startDateStr)
                self.processData(startDateStr,[],teamList,[])
                self.processData(startDateStr, fundList, [], [])
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
    ####勿提交改动，
    env = 'prod'
    currcyExposure = CurrencyExposureCalculator(env)

    currcyExposure.runWithDateRange('2019-09-02', '2019-09-02')


