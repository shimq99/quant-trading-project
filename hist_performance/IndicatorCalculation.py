#encoding:UTF-8

import math
import numpy as np
import pandas as pd
import datetime

class IndicatorCalculation(object):

    @staticmethod
    def calculateMaxDD(ytdYieldList):
        #highestYield = -float("inf")
        highestYield = ytdYieldList[0]
        maxDD = 0.0
        currentDD = 0.0
        winDays = 0.0
        lossDays = 0.0
        avgWinPnl = 0.0
        avgLossPnl = 0.0
        sumWinPnl = 0.0
        sumLossPnl = 0.0        
        yesterdayPnl = 0.0
        winRatio = 0.0
        lossRatio = 0.0
        for ytd in ytdYieldList:
            if(ytd > highestYield):
                highestYield = ytd
            currentDD = (1 + ytd)/(1 + highestYield) - 1
            if(currentDD < maxDD):
                maxDD = currentDD

            todayPnl = ytd - yesterdayPnl
            if(todayPnl > 0):
                sumWinPnl += todayPnl
                winDays += 1
            elif(todayPnl < 0):
                sumLossPnl += todayPnl
                lossDays += 1

            yesterdayPnl = ytd

        if(winDays > 0):
            avgWinPnl = sumWinPnl/winDays
        if(lossDays > 0):
            avgLossPnl = sumLossPnl/lossDays

        if(len(ytdYieldList) >= 2):
            dtdYield = ytdYieldList[-1] - ytdYieldList[-2]
        elif(len(ytdYieldList) == 1):
            dtdYield = ytdYieldList[0]
        else:
            dtdYield = 0.0

        if(len(ytdYieldList) > 0):
            winRatio = winDays/len(ytdYieldList)
            lossRatio = lossDays/len(ytdYieldList)

        return (dtdYield, highestYield, maxDD, currentDD, avgWinPnl, winRatio, avgLossPnl, lossRatio)

    @staticmethod
    def calculateRecovery(bookYtdGrossReturnDataframe,fundId,bookId):
        if bookId == 'None':
            bookId=0

        fundAndBookData = bookYtdGrossReturnDataframe[(bookYtdGrossReturnDataframe['FundId'] == int(fundId)) & (bookYtdGrossReturnDataframe['BookId'] == int(bookId))].copy()
        recovered = 'Not Yet'
        fundAndBookData.index = pd.to_datetime(fundAndBookData['Date'])
        fundAndBookData['Date'] = pd.to_datetime(fundAndBookData['Date'])
        fundAndBookData.sort_index(ascending=True, inplace=True)

        firstDate = fundAndBookData['Date'].iloc[0]
        previousDateData = pd.DataFrame([0], columns={'YtdGrossReturn'}, index=[firstDate - datetime.timedelta(days=1)])
        #离散return 公式 ： (p_t / p_t - 1) - 1
        #离散累计return:    (1 + 离散return).cumprod()
        fundAndBookData = pd.concat([fundAndBookData, previousDateData], axis=0)
        fundAndBookData.sort_index(ascending=True, inplace=True)
        fundAndBookData.dropna(subset=['YtdGrossReturn'], how='all', inplace=True)
        #fundAndBookData['YtdGrossReturn'] = fundAndBookData['YtdGrossReturn'].fillna(0)
        fundAndBookData['PCT_CHG'] = (fundAndBookData['YtdGrossReturn'] - fundAndBookData['YtdGrossReturn'].shift(1)) / (1 + fundAndBookData['YtdGrossReturn'].shift(1))
        fundAndBookData['CUM_RET'] = (1+fundAndBookData['PCT_CHG']).astype(float).cumprod()
        fundAndBookData.dropna(subset=['CUM_RET'], how='all', inplace=True)

        #连续的cumulative return,但由于PCT change不是连续的，故不适用
        #fundAndBookData['CUM_RET'] = fundAndBookData['PCT_CHG'].astype(float).cumsum().apply(np.exp)
        if not fundAndBookData.empty:
            fundAndBookData['CUM_MAX'] = fundAndBookData['CUM_RET'].cummax()
            fundAndBookData['CurrentDD'] = (fundAndBookData['CUM_RET'] /fundAndBookData['CUM_MAX']) -1
            maxDD = fundAndBookData['CurrentDD'].min()

            maxDDDate = fundAndBookData[fundAndBookData['CurrentDD'] == maxDD].index[0]
            CumReturnBeforeMaxDD = fundAndBookData[fundAndBookData['Date'] <= maxDDDate]['CUM_RET'].max()
            CumReturnAfterMaxDD = fundAndBookData[fundAndBookData['Date'] > maxDDDate]['CUM_RET'].max()
            if CumReturnAfterMaxDD > CumReturnBeforeMaxDD:
                recovered = 'Recovered'
            maxDDPeriodData = fundAndBookData[fundAndBookData['Date'] <= maxDDDate]
            duplicated_test = maxDDPeriodData.duplicated(subset=['Date'],keep=False)
            duplicated_data = maxDDPeriodData[duplicated_test]
            if not duplicated_data.empty:
                ##如有重复，只保留Marking source
                validData = duplicated_data[duplicated_data['Source'] == 'Marking']
                if validData.shape[0] ==1:
                    maxDDPeriodData.drop_duplicates(subset='Date', inplace=True, keep=False)
                    maxDDPeriodData = pd.concat([maxDDPeriodData, validData], axis=0)
                    maxDDPeriodData.sort_index(ascending=True, inplace=True)
                else:
                    raise Exception('duplicate data for fundid:'+str(fundId)+', and bookId:'+str(bookId)+', pls check Nav table')
            maxDDStartDate = maxDDPeriodData.ix[maxDDPeriodData['CUM_RET'].idxmax(),'Date']
            maxDDStartDateStr = maxDDStartDate.strftime('%Y-%m-%d')
            maxDDDateStr = maxDDDate.strftime('%Y-%m-%d')
            another_maxDD = maxDD
            return (another_maxDD, maxDDStartDateStr, maxDDDateStr, recovered)
        return (0, None, None, None)

    @staticmethod
    def calculateRecoveryWithPct(bookYtdGrossReturnDataframe, fundId, bookId):
        if bookId == 'None':
            bookId = 0

        fundAndBookData = bookYtdGrossReturnDataframe[(bookYtdGrossReturnDataframe['FundId'] == int(fundId)) & (
                    bookYtdGrossReturnDataframe['BookId'] == int(bookId))].copy()
        recovered = 'Not Yet'
        fundAndBookData.index = pd.to_datetime(fundAndBookData['Date'])
        fundAndBookData['Date'] = pd.to_datetime(fundAndBookData['Date'])
        fundAndBookData.sort_index(ascending=True, inplace=True)

        firstDate = fundAndBookData['Date'].iloc[0]
        # 离散return 公式 ： (p_t / p_t - 1) - 1
        # 离散累计return:    (1 + 离散return).cumprod()
        fundAndBookData['CUM_RET'] = (1 + fundAndBookData['PCT_CHG']).astype(float).cumprod()
        fundAndBookData.dropna(subset=['CUM_RET'], how='all', inplace=True)
        # 连续的cumulative return,但由于PCT change不是连续的，故不适用
        # fundAndBookData['CUM_RET'] = fundAndBookData['PCT_CHG'].astype(float).cumsum().apply(np.exp)
        if not fundAndBookData.empty:
            fundAndBookData['YTD'] = (1 + fundAndBookData['PCT_CHG']).astype(float).cumprod() - 1
            ytdYieldList = fundAndBookData['YTD'].tolist()
            annualRtn = fundAndBookData['YTD'].iloc[-1] / len(ytdYieldList) * 250
            (annualVol, annualRtn, annualSharpe) = IndicatorCalculation.calculateAnnualVolatilitySharpe(ytdYieldList,tradeDays=250)
            fundAndBookData['CUM_MAX'] = fundAndBookData['CUM_RET'].cummax()
            fundAndBookData['CurrentDD'] = (fundAndBookData['CUM_RET'] / fundAndBookData['CUM_MAX']) - 1
            currentDD = fundAndBookData['CurrentDD'].iloc[-1]
            maxDD = fundAndBookData['CurrentDD'].min()

            maxDDDate = fundAndBookData[fundAndBookData['CurrentDD'] == maxDD].index[0]
            CumReturnBeforeMaxDD = fundAndBookData[fundAndBookData['Date'] <= maxDDDate]['CUM_RET'].max()
            CumReturnAfterMaxDD = fundAndBookData[fundAndBookData['Date'] > maxDDDate]['CUM_RET'].max()
            if CumReturnAfterMaxDD > CumReturnBeforeMaxDD:
                recovered = 'Recovered'
            maxDDPeriodData = fundAndBookData[fundAndBookData['Date'] <= maxDDDate]
            duplicated_test = maxDDPeriodData.duplicated(subset=['Date'], keep=False)
            duplicated_data = maxDDPeriodData[duplicated_test]
            if not duplicated_data.empty:
                ##如有重复，只保留Marking source
                validData = duplicated_data[duplicated_data['Source'] == 'Marking']
                if validData.shape[0] == 1:
                    maxDDPeriodData.drop_duplicates(subset='Date', inplace=True, keep=False)
                    maxDDPeriodData = pd.concat([maxDDPeriodData, validData], axis=0)
                    maxDDPeriodData.sort_index(ascending=True, inplace=True)
                else:
                    raise Exception('duplicate data for fundid:' + str(fundId) + ', and bookId:' + str(
                        bookId) + ', pls check Nav table')
            maxDDStartDate = maxDDPeriodData.ix[maxDDPeriodData['CUM_RET'].idxmax(), 'Date']
            maxDDStartDateStr = maxDDStartDate.strftime('%Y-%m-%d')
            maxDDDateStr = maxDDDate.strftime('%Y-%m-%d')
            another_maxDD = maxDD
            return (another_maxDD, maxDDStartDateStr, maxDDDateStr, recovered, annualRtn, currentDD, annualVol, annualSharpe)
        return (0, None, None, None,0,0,0,0)

    @staticmethod
    def calculateAnnualVolatilitySharpe(ytdYieldList, tradeDays = 252):
        try:    
            dtdYieldList = []        
            i = 0
            for ytd in ytdYieldList:
                if(i == 0):
                    dtdYieldList.append(ytd)
                else:
                    dtdYieldList.append(ytdYieldList[i] - ytdYieldList[i-1])

                i += 1

            sumYtd = 0.0
            avgYtd = 0.0
            for ytd in dtdYieldList:
                sumYtd += ytd

            avgYtd = sumYtd/len(dtdYieldList)
            squareSum = 0.0
            for ytd in dtdYieldList:
                squareSum += (ytd - avgYtd) * (ytd - avgYtd)

            annualRtn = ytdYieldList[-1]/len(ytdYieldList) * tradeDays
            annualVol = math.sqrt(squareSum/(len(dtdYieldList) - 1)) * math.sqrt(tradeDays - 2) #why minus 2
            annualSharpe = annualRtn/annualVol
            return (annualVol, annualRtn, annualSharpe)

        except Exception,e:
            return (0.0, 0.0, 0.0)


if __name__ == '__main__':
    ytdYieldList = [0.98, 0.99, 0.97, 1, 1.02, 1.01, 1.05, 0.9, 0.98, 1.02]
    print IndicatorCalculation.calculateMaxDD(ytdYieldList)
    print IndicatorCalculation.calculateAnnualVolatilitySharpe(ytdYieldList)

        


     




