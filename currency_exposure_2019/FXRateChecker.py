# encoding:UTF-8
import pandas as pd
import numpy as np


def calFXRateWithColNames(dataframe_data, fx_data, from_currency_col, to_currency_col):
    dataframe_data_standby = dataframe_data.copy()
    fx_data_standby = fx_data.copy()
    dataframe_data_standby['FromCurrencyCode'] = dataframe_data_standby[from_currency_col]
    dataframe_data_standby['ToCurrencyCode'] = dataframe_data_standby[to_currency_col]
    fx_data_standby.drop_duplicates(subset=['FromCurrencyCode', 'ToCurrencyCode'], inplace=True)
    firstRoundDataWithFXRate = pd.merge(dataframe_data_standby[['FromCurrencyCode', 'ToCurrencyCode']], fx_data,
                                        how='left', on=['FromCurrencyCode', 'ToCurrencyCode'])

    firstRoundDataWithFXRate['FromCurrencyCodeReverse'] = firstRoundDataWithFXRate['ToCurrencyCode']
    firstRoundDataWithFXRate['ToCurrencyCodeReverse'] = firstRoundDataWithFXRate['FromCurrencyCode']
    fx_data_copy = fx_data.copy()
    fx_data_copy['FromCurrencyCodeReverse'] = fx_data_copy['FromCurrencyCode']
    fx_data_copy['ToCurrencyCodeReverse'] = fx_data_copy['ToCurrencyCode']
    fx_data_copy['LastReverse'] = 1 / fx_data_copy['Last']

    firstRoundDataWithFXRate = pd.merge(firstRoundDataWithFXRate, fx_data_copy[
        ['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse', 'LastReverse']], how='left',
                                        on=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'])
    firstRoundDataWithFXRate['Last'] = np.where(firstRoundDataWithFXRate['Last'].isna(),
                                                firstRoundDataWithFXRate['LastReverse'],
                                                firstRoundDataWithFXRate['Last'])

    checkNALast = firstRoundDataWithFXRate[firstRoundDataWithFXRate['Last'].isna()]
    if not checkNALast.empty:
        firstRoundDataWithFXRate = lookForFXRate(firstRoundDataWithFXRate, fx_data)
        checkNALast = firstRoundDataWithFXRate[firstRoundDataWithFXRate['Last'].isna()]
        if not checkNALast.empty:
            raise Exception('get fx rate failed')

    firstRoundDataWithFXRate[from_currency_col+'To'+to_currency_col+'Last'] = firstRoundDataWithFXRate['Last']
    firstRoundDataWithFXRate.drop_duplicates(subset=[from_currency_col, to_currency_col], inplace=True)
    dataframe_data = pd.merge(dataframe_data, firstRoundDataWithFXRate[
        [from_currency_col, to_currency_col, from_currency_col+'To'+to_currency_col+'Last']],
                              how='left',
                              on=[from_currency_col, to_currency_col])
    return dataframe_data


def lookForFXRate(data, fx_data):
    data['HelpCurrencyCode'] = 'USD'

    '''
       当数据库中找不到直接转换的汇率时(如： HKD/CNY), 设置中转汇率USD.

       1. 找到HKD/USD汇率
    '''
    fromCurrencyCodeData = data.copy()
    fromCurrencyCodeData['ToCurrencyCode'] = data['HelpCurrencyCode']
    fromCurrencyCodeData = pd.merge(fromCurrencyCodeData[['FromCurrencyCode', 'ToCurrencyCode', 'HelpCurrencyCode']],
                                    fx_data, how='left', on=['FromCurrencyCode', 'ToCurrencyCode'])
    fromCurrencyCodeData['FromCurrencyCodeReverse'] = fromCurrencyCodeData['ToCurrencyCode']
    fromCurrencyCodeData['ToCurrencyCodeReverse'] = fromCurrencyCodeData['FromCurrencyCode']
    fx_data_copy = fx_data.copy()
    fx_data_copy['FromCurrencyCodeReverse'] = fx_data_copy['FromCurrencyCode']
    fx_data_copy['ToCurrencyCodeReverse'] = fx_data_copy['ToCurrencyCode']
    fx_data_copy['LastReverse'] = 1 / fx_data_copy['Last']
    fx_data_copy.drop_duplicates(subset=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'], inplace=True)
    fromCurrencyCodeData = pd.merge(fromCurrencyCodeData,
                                    fx_data_copy[['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse', 'LastReverse']],
                                    how='left', on=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'])
    fromCurrencyCodeData['Last'] = np.where(fromCurrencyCodeData['Last'].isna(), fromCurrencyCodeData['LastReverse'],
                                            fromCurrencyCodeData['Last'])

    '''
       2. 找到USD/CNY汇率
    '''
    toCurrencyCodeData = data.copy()
    toCurrencyCodeData['FromCurrencyCode'] = data['HelpCurrencyCode']
    toCurrencyCodeData = pd.merge(toCurrencyCodeData[['FromCurrencyCode', 'ToCurrencyCode', 'HelpCurrencyCode']],
                                  fx_data, how='left', on=['FromCurrencyCode', 'ToCurrencyCode'])
    toCurrencyCodeData['FromCurrencyCodeReverse'] = toCurrencyCodeData['ToCurrencyCode']
    toCurrencyCodeData['ToCurrencyCodeReverse'] = toCurrencyCodeData['FromCurrencyCode']
    fx_data_copy = fx_data.copy()
    fx_data_copy['FromCurrencyCodeReverse'] = fx_data_copy['FromCurrencyCode']
    fx_data_copy['ToCurrencyCodeReverse'] = fx_data_copy['ToCurrencyCode']
    fx_data_copy['LastReverse'] = 1 / fx_data_copy['Last']
    fx_data_copy.drop_duplicates(subset=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'], inplace=True)
    toCurrencyCodeData = pd.merge(toCurrencyCodeData,
                                  fx_data_copy[['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse', 'LastReverse']],
                                  how='left',
                                  on=['FromCurrencyCodeReverse', 'ToCurrencyCodeReverse'])
    toCurrencyCodeData['Last'] = np.where(toCurrencyCodeData['Last'].isna(), toCurrencyCodeData['LastReverse'],
                                          toCurrencyCodeData['Last'])

    dataForMerge = data.copy()
    fromCurrencyCodeDataForMerge = fromCurrencyCodeData.copy()
    fromCurrencyCodeDataForMerge['LastWithToHelpCurrencyCode'] = fromCurrencyCodeDataForMerge['Last']
    fromCurrencyCodeDataForMerge.drop_duplicates(subset=['FromCurrencyCode', 'HelpCurrencyCode'], inplace=True)
    dataForMerge = pd.merge(dataForMerge, fromCurrencyCodeDataForMerge[
        ['FromCurrencyCode', 'HelpCurrencyCode', 'LastWithToHelpCurrencyCode']],
                            how='left',
                            on=['FromCurrencyCode', 'HelpCurrencyCode'])

    toCurrencyCodeDataForMerge = toCurrencyCodeData.copy()
    toCurrencyCodeDataForMerge['LastWithFromHelpCurrencyCode'] = toCurrencyCodeDataForMerge['Last']
    toCurrencyCodeDataForMerge.drop_duplicates(subset=['ToCurrencyCode', 'HelpCurrencyCode'], inplace=True)
    dataForMerge = pd.merge(dataForMerge, toCurrencyCodeDataForMerge[
        ['ToCurrencyCode', 'HelpCurrencyCode', 'LastWithFromHelpCurrencyCode']], how='left',
                            on=['ToCurrencyCode', 'HelpCurrencyCode'])

    dataForMerge['Last'] = np.where(dataForMerge['Last'].isna(),
                                    dataForMerge['LastWithToHelpCurrencyCode'] * dataForMerge[
                                        'LastWithFromHelpCurrencyCode'],
                                    dataForMerge['Last'])
    return dataForMerge
