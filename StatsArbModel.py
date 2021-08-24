import pandas as pd
import numpy as np
import os
import fnmatch
import fileinput
from scipy.interpolate import interp1d
from sklearn.metrics import mean_squared_error
import time
import re
import datetime as dt
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
from matplotlib.pyplot import figure
import matplotlib.ticker as ticker
import matplotlib.mlab as mlab
import scipy.optimize as optimize
from scipy.optimize import Bounds
from scipy.optimize import basinhopping
from scipy.optimize import minimize, LinearConstraint
from statsmodels.tsa.stattools import adfuller

from statsmodels.tsa.vector_ar.vecm import coint_johansen
from statsmodels.tsa.vector_ar.var_model import VAR
import statsmodels.api as sm
from sklearn import linear_model

from statsmodels.tsa.tsatools import lagmat, add_trend
from statsmodels.tsa.adfvalues import mackinnonp
from statsmodels.regression.linear_model import OLS
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from math import sqrt
import statsmodels.tsa.vector_ar.vecm as vecmpk
from numpy import polyfit
from matplotlib import pyplot
from statsmodels.tsa.statespace.kalman_filter import KalmanFilter
# from pykalman import KalmanFilter
import sklearn.preprocessing as preprocessing
import pywt
pd.set_option('display.max_columns', 10)
pd.set_option('precision', 15)
import decimal
import datetime
import logging
from base.Base import Base
# # Generate dataset
import pandas as pd
import numpy as np
import os
import time
import re
import datetime as dt
import statsmodels.tsa.stattools as ts
from sys import exit


decimal.getcontext().prec = 10
from tools import PandasDBUtils as pdUtil
from log.LogManager import LogManager

class StatsArbModel(Base):
    def __init__(self):
        LogManager('StatsArbModel')
        logging.info('StatsArbModel')
        self.filename =''
        self.opentimes =[]

    def sma(self,data, window):
        sma = data.rolling(window=window).mean()
        return sma

    def bb(self,data, sma, window, param):
        ### set upper / lower bollinger band
        std = data.rolling(window=window).std()
        upper_bb = sma + std * param
        lower_bb = sma - std * param
        return upper_bb, lower_bb

    def contractrollover(self,df):
        ### check contract rollover start datetime
        df['id'] = 1
        groups = df.groupby('id')
        for col in ['ContractName_x', 'ContractName_y']:
            df[f'check-{col}'] = groups[col].shift().fillna(df[col]).ne(df[col]).astype(int)
        return df

    def setrollover(self,result):
        result = self.contractrollover(result)
        result['checkrollover'] = np.where((result['check-ContractName_x'] != 0) | (result['check-ContractName_y'] != 0), 1, 0)
        rolloverdf = result.groupby(['TraDate'])['checkrollover'].max().reset_index()
        rolloverdf = rolloverdf.rename(columns={'checkrollover': 'rolloverTraDate'})
        ### set snooze for T and T+1 date
        rolloverindex = rolloverdf.index
        T_index = rolloverindex[rolloverdf['rolloverTraDate'] == 1].tolist()
        maxlen = len(rolloverdf) - 1
        T1_index = [x + 1 for x in T_index if x < maxlen]
        # set T+1 day snooze
        rolloverdf.at[T1_index, 'rolloverTraDate'] = 1
        result = pd.merge(rolloverdf, result, on='TraDate', how='outer')
        return result

    def markweeklylasttradepoint(self,result):
        ### mark weekly last trading timestamp, make sure clear position before weekends
        result['TraDate'] = pd.to_datetime(result['TraDate'], format='%Y%m%d')
        result['DateYear'] = result['TraDate'].apply(lambda x: x.isocalendar()[0])
        result['DateWeekNum'] = result['TraDate'].apply(lambda x: x.isocalendar()[1])
        result['DateWeekDayNum'] = result['TraDate'].apply(lambda x: x.isocalendar()[2])
        weeklyreturndf = result.groupby(['DateYear', 'DateWeekNum']).agg({'DateTime': 'last'}).reset_index()
        weeklyreturndf['lasttradepoint'] = 1
        result = pd.merge(weeklyreturndf[['DateTime', 'lasttradepoint']], result, on='DateTime', how='right')
        result['lasttradepoint'] = result['lasttradepoint'].fillna(0)
        return result

    def smoothT_T1(self,df):
        result = self.contractrollover(df)
        result = self.separaterollover(result, 'check-ContractName_x')
        result = self.separaterollover(result, 'check-ContractName_y')
        result['smoothClose1'] = np.where(result['check-ContractName_x_traday'] == 1, np.NaN, df['smoothClose1'])
        result['smoothClose2'] = np.where(result['check-ContractName_y_traday'] == 1, np.NaN, df['smoothClose2'])
        ### ignore contract price at Date T & T+1, simply connect the price before and after to smooth the price jump
        result = result.interpolate()
        return result

    def smoothrolloverjump(self,df):
        df = df.sort_values(by='DateTime')
        result = pd.DataFrame()
        index = df.index
        contractnames = list(set(df['ContractName'].tolist()))
        contractnames.sort()
        df['smoothClose'] = 0
        for name in contractnames:
            equalcondition = df['ContractName'] == name
            beforecondition = df['ContractName'] < name
            if len(index[beforecondition]) > 0:
                beforeindex = index[beforecondition][-1]
                df['smoothClose'] = round(df['Close'].iloc[beforeindex:61 + beforeindex].ewm(span=12, adjust=False).mean(), 2)
        df['smoothClose'] = np.where((df['smoothClose'] == 0) | (df['smoothClose'].isna()), df['Close'],df['smoothClose'])
        return df

    def opt_1Minsample(self,df):
        startpoint = df['DateTime'].iloc[0]
        endpoint = df['DateTime'].iloc[-1]
        next1Wpoint = startpoint
        resultdf = pd.DataFrame()
        opt_bandlist = []
        while (next1Wpoint <= endpoint):
            next1Mpoint = startpoint + dt.timedelta(days=7)
            next1Wpoint = next1Mpoint + dt.timedelta(days=7)
            ### simply set band width to be 4 (4 std from mean)
            opt_band = [4]
            ### optimize the strategy return of last 7-day data and get opmized band width use for later 7-day data
            # opt_band = optFunction(startpoint,next1Mpoint)
            opt_bandlist.append(opt_band)
            resultoutsample = self.outsamplestrategyrun(opt_band, df, next1Mpoint, next1Wpoint)
            resultoutsample['optmz_band'] = opt_band[0]
            resultdf = pd.concat([resultdf, resultoutsample], ignore_index=True)
            startpoint = startpoint + dt.timedelta(days=7)
        print(opt_bandlist)
        return resultdf

    def beforeRO(self,result):
        ### mark just before rollover timestamp (make sure clear all positions before rollover at the last timepoint)
        result = result.reset_index()
        t = result[(result['check-ContractName_x'] != 0) | (result['check-ContractName_y'] != 0)].groupby(['TraDate'])['DateTime', 'index'].first().reset_index()
        rolloverdayindex = t['index'].tolist()
        beforeRO_index = [x - 1 for x in rolloverdayindex]
        result['beforeRO'] = 0
        result.at[beforeRO_index, 'beforeRO'] = 1
        return result

    def outsamplestrategyrun(self,param, df, next1Mpoint, next1Wpoint):
        ### Bollinger band strategy, simple moving average (span = 20), upper / lower bollinger band by last 7-day optmized band width
        result = df[(df['DateTime'] >= next1Mpoint) & (df['DateTime'] < next1Wpoint)].copy()
        result.loc[:, 'sma_20'] = self.sma(result['spread'], 20)
        result.loc[:, 'upper_bb'], result.loc[:, 'lower_bb'] = self.bb(result['spread'], result['sma_20'], 20, param)
        result.loc[:, 'middle_bb'] = result['sma_20']
        result = result.fillna(0)
        ### mark clear position / snooze signals: 1) contract rollover day data; 2) mark last trading point before rollover day; 3) mark weekly last trading point before weekends
        result = self.setrollover(result)
        result = self.beforeRO(result)
        result = self.markweeklylasttradepoint(result)
        ### implement bollinger band strategy
        flag, Buy, Sell = self.implement_bb_strategy(result)
        result['Buy'] = Buy
        result['Sell'] = Sell
        result['flag'] = flag
        return result

    def implement_bb_strategy(self,result):
        data = result['spread']
        lower_bb = result['lower_bb']
        upper_bb = result['upper_bb']
        rolloverdate = result['rolloverTraDate']
        beforeROdate = result['beforeRO']
        weeklylasttradepoint = result['lasttradepoint']
        Buy = [0]
        Sell = [0]
        flag = [0]
        for i in range(1, len(data)):
            ### 3 clear positions / snooze trading signals
            if (beforeROdate[i] != 0) | (rolloverdate[i] != 0) | (weeklylasttradepoint[i] != 0):
                if flag[-1] == 1:
                    flag.append(0)
                    Buy.append(0)
                    Sell.append(1)
                elif flag[-1] == -1:
                    flag.append(0)
                    Buy.append(1)
                    Sell.append(0)
                else:
                    flag.append(0)
                    Buy.append(0)
                    Sell.append(0)
                continue
            ### spread downward cross lower bb, long the pair
            if data[i - 1] > lower_bb[i - 1] and data[i] < lower_bb[i]:
                if flag[-1] != 1:
                    flag.append(1)
                    Buy.append(1)
                    Sell.append(0)
                else:
                    flag.append(flag[-1])
                    Buy.append(0)
                    Sell.append(0)
            ### spread upward cross upper bb, short the pair
            elif data[i - 1] < upper_bb[i - 1] and data[i] > upper_bb[i]:
                if flag[-1] != -1:
                    flag.append(-1)
                    Buy.append(0)
                    Sell.append(1)
                else:
                    flag.append(flag[-1])
                    Buy.append(0)
                    Sell.append(0)
            else:
                flag.append(flag[-1])
                Buy.append(0)
                Sell.append(0)
        return flag, Buy, Sell

    def returncalcu(self,result):
        ### calculate return based on trading flag
        ### future2 = coef * future1 + intercept
        ### long the pair: long 1 futures2, short coef futures1
        flag = result['flag'].tolist()
        result['ExecuteClose1'] = result['Close1']
        result['ExecuteClose2'] = result['Close2']
        result = self.spreadcost(result)
        result['Returns1'] = result['ExecuteClose1'].pct_change().fillna(0)
        result['Returns2'] = result['ExecuteClose2'].pct_change().fillna(0)
        Pos = [0] + flag[:-1]
        result['Pos'] = Pos
        bb_strategy_ret = []
        #### long 2 short 1
        for i in range(len(result['Returns1'])):
            try:
                returns = -result['Returns1'][i] * result['Pos'][i] * result['coef'][i] + result['Returns2'][i] * result['Pos'][i]
                bb_strategy_ret.append(returns)
            except:
                pass
        result['bb_strategy_ret'] = bb_strategy_ret
        result['CumStrategyReturn'] = (result['bb_strategy_ret'] + 1).cumprod()
        return result

    def spreadcost(self,df):
        ### consider spread cost, 0.01% bid-ask spread cost, 0.01% commission fee cost
        df['BuyClose1'] = df['Close1'] * 1.0001 * 1.0001
        df['BuyClose2'] = df['Close2'] * 1.0001 * 1.0001
        df['SellClose1'] = df['Close1'] * (1 - 0.0001) * (1 - 0.0001)
        df['SellClose2'] = df['Close2'] * (1 - 0.0001) * (1 - 0.0001)
        df['ExecuteClose1'] = np.where(df['Sell'] == 1, df['BuyClose1'], df['ExecuteClose1'])
        df['ExecuteClose1'] = np.where(df['Buy'] == 1, df['SellClose1'], df['ExecuteClose1'])
        df['ExecuteClose2'] = np.where(df['Buy'] == 1, df['BuyClose2'], df['ExecuteClose2'])
        df['ExecuteClose2'] = np.where(df['Sell'] == 1, df['SellClose2'], df['ExecuteClose2'])
        return df

    def insampleregression(self,X, Y, data1M):
        x = sm.add_constant(data1M[X])
        model = sm.OLS(data1M[Y], x)
        results = model.fit()
        intercept = results.params[0]
        coef = results.params[1]
        yfit = coef * data1M[X] + intercept
        a = pd.DataFrame(yfit)
        y_residual = (data1M[Y] - a[X]).tolist()
        data1M.insert(3, "spread", y_residual, True)
        data1M['coef'] = coef
        info = self.ADFtest(data1M)
        # info = self.cointegrationtest(data1M, X, Y)
        return coef, intercept, info

    def cointegrationtest(self, data, X,Y ):
        data['x'] = data[X].diff(1).fillna(0)
        data['y'] = data[Y].diff(1).fillna(0)
        test_result = ts.coint(data['x'], data['y'])
        print('p-value = ' + str(test_result[1]))
        if test_result[1] >= 0.05:
            info = 'c'
        else:
            info = 'p'
        return info

    def insample_adftest(self,x, y, data1W, coef, intercept):
        yfit = coef * data1W[[x]] + intercept
        yfit = yfit.rename(columns={x: 'Close'})
        a = pd.DataFrame(yfit)
        y_residual = (data1W[y] - a['Close']).tolist()
        data1W.insert(3, "spread", y_residual, True)
        data1W['coef'] = coef[0]
        self.ADFtest(data1W)
        return data1W

    def outsample_oneweek(self,x, y, data1W, coef, intercept):
        ### use last 30-day coefficient calculate spread for later strategy use
        yfit = coef * data1W[[x]] + intercept
        yfit = yfit.rename(columns={x: 'Close'})
        a = pd.DataFrame(yfit)
        y_residual = (data1W[y] - a['Close']).tolist()
        data1W.insert(3, "spread", y_residual, True)
        data1W['coef'] = coef
        # info = self.ADFtest(data1W)
        return data1W

    def ADFtest(self,data):
        ### check whether the pair is cointegrated (spread calculated by regression is stationary like p-value <0.05)
        ### output info, stationary -> 'c' continue; non-stationary -> 'p' pass this part data
        series = data['spread'].values
        result = adfuller(series, autolag='AIC')
        ADF_Statistic = result[0]
        p_value = result[1]
        print('p_value:', p_value)
        Critial_Values5 = result[4].get('5%')
        Critial_Values1 = result[4].get('1%')
        Critial_Values10 = result[4].get('10%')
        # print('ADF_Statistic:', ADF_Statistic)
        # print('[Critial_Values5, Critial_Values1,Critial_Values10]:', Critial_Values5, Critial_Values1,
        #       Critial_Values10)
        ### check non-stationarity
        if (p_value < 0.05):
            print('It is stationarity.')
            logging.info('pass')
            info = 'p'
            pass
        elif (p_value > 0.05):
            print('It is non-stationarity.')
            logging.error('pair spread not stationary')
            info = 'c'
            # exit()
        return info

    def separaterollover(self,df, name):
        ### set snooze for T and T+1 date signal
        df[name] = np.where((df[name] != 0), 1, 0)
        rolloverdf = df.groupby(['TraDate'])[name].max().reset_index()
        rolloverdf = rolloverdf.rename(columns={name: name + '_traday'})
        rolloverindex = rolloverdf.index
        T_index = rolloverindex[rolloverdf[name + '_traday'] == 1].tolist()
        T1_index = [x + 1 for x in T_index]
        # set T+1 day snooze signal
        rolloverdf.at[T1_index, name + '_traday'] = 1
        result = pd.merge(rolloverdf, df, on='TraDate', how='outer')
        return result

    def Kmodelrun(self,HC, RB):
        ### concat all valid conintegrated data and calculate spread for later strategy use
        HC['smoothClose1'] = HC['Close']
        RB['smoothClose2'] = RB['Close']
        HC = HC.rename(columns={'Close': 'Close1'})
        RB = RB.rename(columns={'Close': 'Close2'})
        df = pd.merge(HC[['DateTime', 'Close1', 'smoothClose1', 'TraDate', 'ContractName']],RB[['DateTime', 'Close2', 'smoothClose2', 'TraDate', 'ContractName']], on=['DateTime', 'TraDate'],how='inner')
        df = df.sort_values(by='DateTime')
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        df = self.smoothT_T1(df)
        df['Close1_lg'] = np.log(df['Close1'])
        df['Close2_lg'] = np.log(df['Close2'])
        df['smoothClose1_lg'] = np.log(df['smoothClose1'])
        df['smoothClose2_lg'] = np.log(df['smoothClose2'])

        df = self.contractrollover(df)
        df = self.separaterollover(df, 'check-ContractName_x')
        df = self.separaterollover(df, 'check-ContractName_y')
        startpoint = df['DateTime'].iloc[0]
        endpoint = df['DateTime'].iloc[-1]
        next1Wpoint = startpoint
        result = pd.DataFrame()
        data = pd.DataFrame()
        # coef, intercept, info = self.insampleregression('smoothClose1_lg', 'smoothClose2_lg', df)
        while (next1Wpoint <= endpoint):
            next1Mpoint = startpoint + dt.timedelta(days=7)
            next1Wpoint = next1Mpoint + dt.timedelta(days=7)
            data1M = df[(df['DateTime'] >= startpoint) & (df['DateTime'] < next1Mpoint)]
            data1W = df[(df['DateTime'] >= next1Mpoint) & (df['DateTime'] < next1Wpoint)]
            if len(data1M)>0:
                coef, intercept, info = self.insampleregression('smoothClose1_lg', 'smoothClose2_lg', data1M)
                if info=='c':
                    ### if the last 30-day data is not conintegrated, pass this round data and continue to next 30-day regression
                    startpoint = startpoint + dt.timedelta(days=7)
                    continue
                ### if the last 30-day data is conintegrated, then the coefficient is valid, then use for later 7-day strategy
                data = self.outsample_oneweek('Close1_lg', 'Close2_lg', data1W, coef, intercept)
                result = pd.concat([result, data], ignore_index=True)
            startpoint = startpoint + dt.timedelta(days=7)
        return result


    def runStatsArb(self):
        df1 = pd.read_csv('F:\\maincontract\\Y.csv')
        df2 = pd.read_csv('F:\\maincontract\\M.csv')
        result = self.Kmodelrun(df1, df2)
        result['DateTime'] = pd.to_datetime(result['DateTime'])
        outdata = self.opt_1Minsample(result)
        result = self.returncalcu(outdata)
        print(result['CumStrategyReturn'].iloc[-1])




if __name__ == '__main__':
    StatsArbModel = StatsArbModel()
    StatsArbModel.runStatsArb()
