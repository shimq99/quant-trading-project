# encoding:UTF-8
from benchmark.base.Base import Base
import numpy as np
import pandas as pd
from benchmark.currency_exposure_2019.base.ExposureEnums import InstClass
class DefaultCalculator(Base):

    def __init__(self):
        self.defaultCalculatorList = [InstClass.DR.value, InstClass.ORD.value, InstClass.RIGHTS.value, InstClass.PFRD.value,
                                    InstClass.GOVT.value, InstClass.CONVPRF.value, InstClass.CONVBND.value,
                                    InstClass.BOND.value, InstClass.WRNT.value, InstClass.FRN.value, InstClass.OP_BONDFT.value,
                                    InstClass.CIS.value, InstClass.OP_CMDTY_FT.value]
        self.futureAndSwapCalculatorList = [InstClass.EQINDX_OP2.value, InstClass.BOND_FT.value, InstClass.EQINDX_FT.value,
                                       InstClass.CMDTY_FWRD.value, InstClass.FX_FT.value, InstClass.CFD.value,
                                       InstClass.CFD_OLD.value, InstClass.CMDINDX_FT.value, InstClass.EQINDX_SW.value,
                                       InstClass.BNDINDX_FT.value, InstClass.IR_FT.value, InstClass.EQ_SW.value,
                                       InstClass.FFA.value, InstClass.EQTY_FT.value]
        self.optionCalculatorList = [InstClass.CMDTY_OP.value, InstClass.EQTY_OP.value, InstClass.FX_OP.value,
                                InstClass.CMDINDX_OP.value]

    def lookForFXRate(self, data, fx_data):
        data['HelpCurrencyCode'] = 'USD'

        '''
           当数据库中找不到直接转换的汇率时(如： HKD/CNY), 设置中转汇率USD.
           
           1. 找到HKD/USD汇率
        '''
        fromCurrencyCodeData = data.copy()
        fromCurrencyCodeData['ToCurrencyCode'] = data['HelpCurrencyCode']
        fromCurrencyCodeData = pd.merge(fromCurrencyCodeData[['FromCurrencyCode', 'ToCurrencyCode', 'HelpCurrencyCode']], fx_data, how='left', on=['FromCurrencyCode', 'ToCurrencyCode'])
        fromCurrencyCodeData['FromCurrencyCodeReverse'] = fromCurrencyCodeData['ToCurrencyCode']
        fromCurrencyCodeData['ToCurrencyCodeReverse'] = fromCurrencyCodeData['FromCurrencyCode']
        fx_data_copy = fx_data.copy()
        fx_data_copy['FromCurrencyCodeReverse'] = fx_data_copy['FromCurrencyCode']
        fx_data_copy['ToCurrencyCodeReverse'] = fx_data_copy['ToCurrencyCode']
        fx_data_copy['LastReverse'] = 1 / fx_data_copy['Last']
        fx_data_copy.drop_duplicates(subset=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'], inplace=True)
        fromCurrencyCodeData = pd.merge(fromCurrencyCodeData, fx_data_copy[ ['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse', 'LastReverse']], how='left', on=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'])
        fromCurrencyCodeData['Last'] = np.where(fromCurrencyCodeData['Last'].isna(), fromCurrencyCodeData['LastReverse'], fromCurrencyCodeData['Last'])

        '''
           2. 找到USD/CNY汇率
        '''
        toCurrencyCodeData = data.copy()
        toCurrencyCodeData['FromCurrencyCode'] = data['HelpCurrencyCode']
        toCurrencyCodeData = pd.merge(toCurrencyCodeData[['FromCurrencyCode', 'ToCurrencyCode','HelpCurrencyCode']], fx_data, how='left', on=['FromCurrencyCode', 'ToCurrencyCode'])
        toCurrencyCodeData['FromCurrencyCodeReverse'] = toCurrencyCodeData['ToCurrencyCode']
        toCurrencyCodeData['ToCurrencyCodeReverse'] = toCurrencyCodeData['FromCurrencyCode']
        fx_data_copy = fx_data.copy()
        fx_data_copy['FromCurrencyCodeReverse'] = fx_data_copy['FromCurrencyCode']
        fx_data_copy['ToCurrencyCodeReverse'] = fx_data_copy['ToCurrencyCode']
        fx_data_copy['LastReverse'] = 1 / fx_data_copy['Last']
        fx_data_copy.drop_duplicates(subset=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'], inplace=True)
        toCurrencyCodeData = pd.merge(toCurrencyCodeData, fx_data_copy[['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse', 'LastReverse']], how='left',
                                        on=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'])
        toCurrencyCodeData['Last'] = np.where(toCurrencyCodeData['Last'].isna(), toCurrencyCodeData['LastReverse'], toCurrencyCodeData['Last'])


        dataForMerge = data.copy()
        fromCurrencyCodeDataForMerge = fromCurrencyCodeData.copy()
        fromCurrencyCodeDataForMerge['LastWithToHelpCurrencyCode'] = fromCurrencyCodeDataForMerge['Last']
        fromCurrencyCodeDataForMerge.drop_duplicates(subset=['FromCurrencyCode','HelpCurrencyCode'], inplace=True)
        dataForMerge = pd.merge(dataForMerge, fromCurrencyCodeDataForMerge[['FromCurrencyCode','HelpCurrencyCode', 'LastWithToHelpCurrencyCode']],
                                how='left',
                                on=['FromCurrencyCode', 'HelpCurrencyCode'])

        toCurrencyCodeDataForMerge = toCurrencyCodeData.copy()
        toCurrencyCodeDataForMerge['LastWithFromHelpCurrencyCode'] = toCurrencyCodeDataForMerge['Last']
        toCurrencyCodeDataForMerge.drop_duplicates(subset=['ToCurrencyCode', 'HelpCurrencyCode'], inplace=True)
        dataForMerge = pd.merge(dataForMerge, toCurrencyCodeDataForMerge[['ToCurrencyCode', 'HelpCurrencyCode', 'LastWithFromHelpCurrencyCode']], how='left', on=['ToCurrencyCode', 'HelpCurrencyCode'])

        dataForMerge['Last'] = np.where(dataForMerge['Last'].isna(),
                                        dataForMerge['LastWithToHelpCurrencyCode'] * dataForMerge['LastWithFromHelpCurrencyCode'],
                                        dataForMerge['Last'])
        return dataForMerge


    def selectFromDataBase(self,sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def lookForMostRecentFXRate(self,data, dateStr):
        self.initSqlServer('prod')
        sql = 'SELECT FxRateId,Date,FromCurrencyId,FromCurrencyCode,ToCurrencyId,ToCurrencyCode,Last FROM MarketData.mark.FxRateView where Date=(select max([Date]) FROM MarketData.mark.FxRateView where [Date] <=\''+dateStr+'\''
        total_result = pd.DataFrame(columns=['FromCurrencyCode', 'ToCurrencyCode', 'Last'])
        for index, data_row in data.iterrows():
            from_currency = data_row['FromCurrencyCode']
            to_currency = data_row['ToCurrencyCode']
            from_currency_reverse = data_row['FromCurrencyCodeReverse']
            to_currency_reverse = data_row['ToCurrencyCodeReverse']
            sql2 = sql+ ' AND FromCurrencyCode=\''+from_currency+'\' and ToCurrencyCode=\''+to_currency+'\') AND FromCurrencyCode=\''+from_currency+'\' and ToCurrencyCode=\''+to_currency+'\''
            result = self.selectFromDataBase(sql2)
            if result.empty:
                sql3 = sql + ' AND FromCurrencyCode=\''+from_currency_reverse+'\' and ToCurrencyCode=\''+to_currency_reverse+'\') AND FromCurrencyCode=\''+from_currency_reverse+'\' and ToCurrencyCode=\''+to_currency_reverse+'\''
                result = self.selectFromDataBase(sql3)
                result['FromCurrencyCode'] =to_currency_reverse
                result['ToCurrencyCode'] = from_currency_reverse
                result['Last'] = 1 / result['Last']
            total_result = pd.concat([total_result, result[['FromCurrencyCode', 'ToCurrencyCode', 'Last']]], axis=0)
        self.closeSqlServerConnection()
        return total_result

    def calcFXRate(self, dataframe_data, fx_data,dateStr):
        #test_data = dataframe_data[(dataframe_data['PriceLocal'] == 0.0) | (dataframe_data['PriceLocal'].isna())]
        #test_data2 = dataframe_data[~(dataframe_data['PriceLocal'] == 0.0) & ~(dataframe_data['PriceLocal'].isna())]

        dataframe_data['FxRateLocalBook'] = np.where((dataframe_data['PriceLocal'].astype(float) == 0.0) | (dataframe_data['PriceLocal'].isna()) | (dataframe_data['FxRateLocalBook'].astype(float) != 0),dataframe_data['FxRateLocalBook'],dataframe_data['PriceBook'].astype(float)/dataframe_data['PriceLocal'].astype(float))
        dataframe_data['FxRateLocalBookStart'] = np.where((dataframe_data['PriceLocalStart'].astype(float) == 0.0) | (dataframe_data['PriceLocalStart'].isna()), dataframe_data['FxRateLocalBookStart'], dataframe_data['PriceBookStart'].astype(float) / dataframe_data['PriceLocalStart'].astype(float))


        ineqCurrencyData = dataframe_data[(~dataframe_data['UnderlyingPnlCcyCode'].isna()) & (~dataframe_data['UnderlyingPnlCcyCode'].isin(['NOT SPECIFIED'])) & (dataframe_data['PnlCcyCode'] != dataframe_data['UnderlyingPnlCcyCode'])].copy()
        ineqCurrencyData['FromCurrencyCode'] = ineqCurrencyData['UnderlyingPnlCcyCode']
        ineqCurrencyData['ToCurrencyCode'] = ineqCurrencyData['BaseCurrency']
        ineqCurrencyDataWithFXRate = pd.merge(ineqCurrencyData[['FromCurrencyCode', 'ToCurrencyCode']], fx_data,how='left', on=['FromCurrencyCode', 'ToCurrencyCode'])

        ineqCurrencyDataWithFXRate['FromCurrencyCodeReverse'] = ineqCurrencyDataWithFXRate['ToCurrencyCode']
        ineqCurrencyDataWithFXRate['ToCurrencyCodeReverse'] = ineqCurrencyDataWithFXRate['FromCurrencyCode']
        fx_data_copy = fx_data.copy()
        fx_data_copy['FromCurrencyCodeReverse'] = fx_data_copy['FromCurrencyCode']
        fx_data_copy['ToCurrencyCodeReverse'] = fx_data_copy['ToCurrencyCode']
        fx_data_copy['LastReverse'] = 1/fx_data_copy['Last']

        ineqCurrencyDataWithFXRate = pd.merge(ineqCurrencyDataWithFXRate, fx_data_copy[['FromCurrencyCodeReverse','ToCurrencyCodeReverse','LastReverse']],how='left', on=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'])
        ineqCurrencyDataWithFXRate['Last'] = np.where(ineqCurrencyDataWithFXRate['Last'].isna(),ineqCurrencyDataWithFXRate['LastReverse'],ineqCurrencyDataWithFXRate['Last'])

        checkNALast = ineqCurrencyDataWithFXRate[ineqCurrencyDataWithFXRate['Last'].isna()]
        if not checkNALast.empty:
            ineqCurrencyDataWithFXRate = self.lookForFXRate(ineqCurrencyDataWithFXRate, fx_data)
            checkNALast = ineqCurrencyDataWithFXRate[ineqCurrencyDataWithFXRate['Last'].isna()]
            if not checkNALast.empty:
                '''
                could be possible that holiday/event on some countries.  e.g.: 2nd Sep is Vietnam holiday, so should use most recent price on it. 
                '''
                most_recent_fx_data = self.lookForMostRecentFXRate(checkNALast,dateStr)
                if most_recent_fx_data.empty:
                    raise Exception('get fx rate failed')

                ineqCurrencyDataWithFXRate = ineqCurrencyDataWithFXRate[~ineqCurrencyDataWithFXRate['Last'].isna()]
                ineqCurrencyDataWithFXRate = pd.concat([ineqCurrencyDataWithFXRate[['FromCurrencyCode', 'ToCurrencyCode', 'Last']], most_recent_fx_data], axis=0)

        ineqCurrencyDataWithFXRate['UnderlyingPnlCcyCode'] = ineqCurrencyDataWithFXRate['FromCurrencyCode']
        ineqCurrencyDataWithFXRate['BaseCurrency'] = ineqCurrencyDataWithFXRate['ToCurrencyCode']
        ineqCurrencyDataWithFXRate['FxRateUnderlyingToBaseCurrny'] = ineqCurrencyDataWithFXRate['Last']

        ineqCurrencyDataWithFXRate.drop_duplicates(subset=['UnderlyingPnlCcyCode', 'BaseCurrency'], inplace=True)
        dataframe_data = pd.merge(dataframe_data, ineqCurrencyDataWithFXRate[['UnderlyingPnlCcyCode', 'BaseCurrency', 'FxRateUnderlyingToBaseCurrny']],
                                  how='left',
                                  on=['UnderlyingPnlCcyCode', 'BaseCurrency'])
        return dataframe_data



    def calcNotnlMarketValue(self, dataframe_data):
        #### Since we store the net notional mkt value, thus need to take accruals into account.
        dataframe_data['NotnlMktValLocal'] = np.where(dataframe_data['InstrumentClass'].isin(self.defaultCalculatorList + self.optionCalculatorList + self.futureAndSwapCalculatorList),
                                             dataframe_data['QuantityEnd'] * dataframe_data['PriceLocal'] * dataframe_data['PricingMultiplier'] \
                                             + dataframe_data['TotalAccrualsLocal'],
                                             dataframe_data['NotnlMktValLocal'])
        dataframe_data['NotnlMktValBook'] = np.where(dataframe_data['InstrumentClass'].isin(self.defaultCalculatorList + self.optionCalculatorList + self.futureAndSwapCalculatorList),
                                             dataframe_data['QuantityEnd'] * dataframe_data['PriceLocal'] * dataframe_data['PricingMultiplier']  * dataframe_data['FxRateLocalBook']\
                                             + dataframe_data['TotalAccrualsLocal'],
                                             dataframe_data['NotnlMktValBook'])

    def calcBetaNotnlMarketValue(self, dataframe_data):
        dataframe_data['Beta'] = np.where(dataframe_data['Beta'].isna(),
                                          np.where(dataframe_data['UnderlyingBeta'].isna(), 1, dataframe_data['UnderlyingBeta']),
                                          dataframe_data['Beta'])
        dataframe_data['DeltaNotnlMktValLocal'] = np.where(dataframe_data['InstrumentClass'].isin(self.optionCalculatorList),
                                                          dataframe_data['QuantityEnd'] * dataframe_data['UnderlyingEndPriceLocal'] * dataframe_data['Delta'] * dataframe_data['PricingMultiplier'],
                                                          dataframe_data['NotnlMktValLocal'] * dataframe_data['Beta'])
        # dataframe_data['BetaNotnlMktValBook'] = np.where(dataframe_data['InstrumentClass'].isin(self.optionCalculatorList),
        #                                                  dataframe_data['BetaNotnlMktValLocal'] * dataframe_data['FxRateLocalBook'],
        #                                                  dataframe_data['NotnlMktValBook'] * dataframe_data['Beta'])

    def calcMarketValue(self, dataframe_data):
        ##Since we store the net mkt value, thus need to take accruals into account.

        dataframe_data['BeginMarketValueLocal'] = np.where(dataframe_data['InstrumentClass'].isin(self.futureAndSwapCalculatorList),
                                                      dataframe_data['QuantityStart'] * (dataframe_data['PriceLocalStart'] - dataframe_data['AvgCostLocal']) \
                                                      * dataframe_data['PricingMultiplier'] + dataframe_data['TotalAccrualsLocal'],
                                                      dataframe_data['QuantityStart'] * dataframe_data['PriceLocalStart'] \
                                                      * dataframe_data['PricingMultiplier'] * dataframe_data['TotalAccrualsLocal'])

        dataframe_data['MarketValueLocal'] = np.where(dataframe_data['InstrumentClass'].isin(self.futureAndSwapCalculatorList),
                                                      dataframe_data['QuantityEnd'] * (dataframe_data['PriceLocal'] - dataframe_data['AvgCostLocal']) \
                                                      * dataframe_data['PricingMultiplier'] + dataframe_data['TotalAccrualsLocal'],
                                                      dataframe_data['QuantityEnd'] * dataframe_data['PriceLocal'] \
                                                      * dataframe_data['PricingMultiplier'] * dataframe_data['TotalAccrualsLocal'])

        dataframe_data['MarketValueBook'] = np.where(dataframe_data['InstrumentClass'].isin(self.futureAndSwapCalculatorList),
                                                     dataframe_data['QuantityEnd'] * (dataframe_data['PriceLocal'] - dataframe_data['AvgCostLocal']) \
                                                     * dataframe_data['PricingMultiplier'] * dataframe_data['FxRateLocalBook'] + dataframe_data['TotalAccrualsLocal'],
                                                     dataframe_data['QuantityEnd'] * dataframe_data['PriceLocal'] * dataframe_data['PricingMultiplier'] * dataframe_data['TotalAccrualsLocal'] * dataframe_data['FxRateLocalBook']
                                                     )

        dataframe_data['BeginMarketValueBook'] = np.where(dataframe_data['InstrumentClass'].isin(self.futureAndSwapCalculatorList),
                                                     dataframe_data['QuantityStart'] * (dataframe_data['PriceLocalStart'] - dataframe_data['AvgCostLocal']) \
                                                     * dataframe_data['PricingMultiplier'] * dataframe_data['FxRateLocalBook'] + dataframe_data['TotalAccrualsLocal'],
                                                     dataframe_data['QuantityStart'] * dataframe_data['PriceLocalStart']  * dataframe_data['PricingMultiplier'] * dataframe_data['TotalAccrualsLocal'] * dataframe_data['FxRateLocalBook']
                                                     )


