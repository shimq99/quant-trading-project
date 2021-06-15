# # Generate dataset
import pandas as pd
import numpy as np
import os

pd.set_option('display.max_columns', 10)
pd.set_option('precision', 15)
import decimal

decimal.getcontext().prec = 10
from tools import PandasDBUtils as pdUtil
from log.LogManager import LogManager
import fnmatch
import fileinput
from scipy.interpolate import interp1d
from sklearn.metrics import mean_squared_error
import time
import re
import datetime as dt
import logging
from base.Base import Base
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
from matplotlib.pyplot import figure
import matplotlib.ticker as ticker
import matplotlib.mlab as mlab
from bokeh.layouts import gridplot
from bokeh.plotting import figure, output_file, show,save
import bokeh.themes
from bokeh.io import curdoc
from fractions import Fraction
import scipy.optimize as optimize
from scipy.optimize import Bounds
from scipy.optimize import basinhopping


class MACDModel(Base):
    def __init__(self):
        LogManager('MACDModel')
        logging.info('MACDModel')
        self.filename =''
        self.opentimes =[]

    def readallcatcontractsdata(self,cat):
        path = 'C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\Combine2019_2020\\Combined'
        result = pd.DataFrame()
        catfilepaths = self.find_filespath(cat, path)
        for catfile in catfilepaths:
            contractname = re.split('[ _ |.]', str(os.path.basename(catfile)))[0]
            df = pd.read_csv(catfile)
            df['ContractName'] = contractname
            #         df = calcontractMACD(df)
            result = pd.concat([result, df], ignore_index=True)
        return result

    def calcontractMACD(self,df):
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        df['exp1'] = round(df.Close.ewm(span=12, adjust=False).mean(), 2)
        df['exp2'] = round(df.Close.ewm(span=26, adjust=False).mean(), 2)
        df['dif'] = df['exp1'] - df['exp2']
        df['dea'] = round(df['dif'].ewm(span=9, adjust=False).mean(), 2)
        df['MACD'] = round((df['dif'] - df['dea']) * 2, 2)
        return df

    def find_filespath(self,cat, search_path):
        result = []
        for dir, dir_name, file_list in os.walk(search_path):
            for files in file_list:
                if (re.findall('([a-zA-Z ]*)\d*.*', files)[0] == cat.lower()) & (files.endswith('_1D.csv')):
                    result.append(os.path.join(search_path, dir, files))
        return result

    def getmaincontract(self,date, cat_all_data, lastmaincontract):
        lastTraDate = cat_all_data[cat_all_data['TraDate'] < date]['TraDate'].iloc[-1]
        cat_all_data['DateTime'] = pd.to_datetime(cat_all_data['DateTime'])

        #     ### for 3min
        #     a = cat_all_data.sort_values(by=['TraDate','DateTime','OI_total'], ascending=[True,True,True])
        ### for 1d
        a = cat_all_data.sort_values(by=['TraDate', 'DateTime', 'OI'], ascending=[True, True, True])
        maincontract = a[(a['TraDate'] == lastTraDate) & (a['ContractName'] >= lastmaincontract)]['ContractName'].iloc[-1]

        return lastTraDate, maincontract

    def get1Dmaincontract(self,date, cat_all_data, lastmaincontract):
        lastTraDate = cat_all_data[cat_all_data['TraDate'] < date]['TraDate'].iloc[-1]
        cat_all_data['DateTime'] = pd.to_datetime(cat_all_data['DateTime'])

        #     ### for 3min
        #     a = cat_all_data.sort_values(by=['TraDate','DateTime','OI_total'], ascending=[True,True,True])
        ### for 1d
        a = cat_all_data.sort_values(by=['TraDate', 'Vol'], ascending=[True, True])
        maincontract = a[(a['TraDate'] == lastTraDate) & (a['ContractName'] >= lastmaincontract)]['ContractName'].iloc[-1]

        return lastTraDate, maincontract

    def runmaindata(self,cat_all_data):
        datelist = list(set(cat_all_data['TraDate'].tolist()))
        datelist.sort()
        del datelist[0]
        result = pd.DataFrame()
        lastmaincontract = 'a2001'
        print(datelist)

        for date in datelist:
            lastTraDate, maincontract = self.get1Dmaincontract(date, cat_all_data, lastmaincontract)
            print(lastTraDate, maincontract)
            lastmaincontract = maincontract
            main_data = cat_all_data[(cat_all_data['TraDate'] == date) & (cat_all_data['ContractName'] == maincontract)].copy()
            result = pd.concat([result, main_data], ignore_index=True)
        result = result.sort_values(by='DateTime')
        result = self.calcontractMACD(result)
        return result

    ### go through total data by line not by tradate, mark last tradtime as 1, as send signal to clear position at second to last
    def marktradateclosetime(self,data, lasttratime, Klineperiod):
        Klineperiod = '60min'
        lasttratime = '15:00:00'
        lasttratime = dt.datetime.strptime(lasttratime, '%H:%M:%S')

        r = re.compile("([0-9]+)([a-zA-Z]+)")
        m = r.match(Klineperiod)
        timenum = int(m.group(1))
        timeperiod = m.group(2)
        lastsignaltime = str((lasttratime - dt.timedelta(minutes=timenum)).time())

        data['tratimemark'] = 0
        data['tratimemark'] = np.where(data['Time'] == lastsignaltime, 1, 0)
        data['tratimemark'] = data['tratimemark'].astype(int)
        return data

    def correctDate(self,data_interval, preTraDay):
        data_interval.loc[(data_interval['Time'] < '08:59:00') | (data_interval['Time'] > '15:16:00'), 'Date'] = np.NaN
        data_interval['Date'] = data_interval['Date'].fillna(preTraDay)
        data_interval['DateTime'] = pd.to_datetime(data_interval['Date'] + ' ' + data_interval['Time'])

        data_interval['DateTimeNextCalenDay'] = data_interval['DateTime'] + dt.timedelta(days=1)
        data_interval['DateTime'] = np.where(data_interval['Time'] < '08:59:00', data_interval['DateTimeNextCalenDay'],data_interval['DateTime'])
        return data_interval

    def f(self,params):
        A, B, C, D = params
        #     cat_all_data = readallcatcontractsdata('A')
        #     cat_all_data = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\2020A\\A_cat_all_data.csv')
        result = self.runsignal(A, B, C, D)
        return result

    def maxcrestcheck(self,data,MACD_maxcheck,i):
        ### 得到当前波段波峰
        if (data['MACD'][i] * MACD_maxcheck > 0) & (abs(data['MACD'][i]) > abs(MACD_maxcheck)):
            MACD_maxcheck = data['MACD'].iloc[i]
        elif (data['MACD'][i] * MACD_maxcheck <= 0):
            MACD_maxcheck = data['MACD'].iloc[i]
        # print(data['TraDate'][i], data['MACD'][i], MACD_maxcheck)
        return MACD_maxcheck

    def goldensignalcheck(self, lengthtrack,data, i,flag,Buy,Sell):
         ##金叉
        if (lengthtrack > 5):
            flag.append(1)
            Buy.append(1)
            Sell.append(0)
        else:
            flag.append(1)
            Buy.append(1)
            Sell.append(0)

        return data, flag,Buy,Sell
    ##当个traday的数据传进来

    def deathsignalcheck(self, data, i,flag,Buy,Sell):
        flag.append(-1)
        Buy.append(0)
        Sell.append(1)
        return data, flag,Buy,Sell

    def longclearcheck(self,data,returncheck,MACD_maxcheck,i,Sell,Buy,flag):
        if (returncheck >= A) | (returncheck <= B) | (abs(data['MACD'][i]) < 0.2 * abs(MACD_maxcheck)) | ((data['MACD'][i - 1] >= 0) & (data['MACD'][i] <= 0)):
            Sell.append(1)
            Buy.append(0)
            flag.append(0)
        else:
            flag.append(flag[-1])
            Buy.append(0)
            Sell.append(0)

        return data,returncheck,Sell,Buy,flag

    def shortclearcheck(self,data,returncheck,MACD_maxcheck,i,Sell,Buy,flag):
        if (returncheck >= C) | (returncheck <= D) | (abs(data['MACD'][i]) < 0.2 * abs(MACD_maxcheck)) | (
                (data['MACD'][i - 1] >= 0) & (data['MACD'][i] <= 0)):
            Sell.append(0)
            Buy.append(1)
            flag.append(0)
        else:
            flag.append(flag[-1])
            Buy.append(0)
            Sell.append(0)
        return data, returncheck, Sell, Buy, flag

    def MACDNEW_ML(self,data, A, B, C, D):
        data['IntraReturn'] = data['Close'].pct_change().fillna(0)
        Buy = [0]
        Sell = [0]
        flag = [0]
        Pos = [0, 0]
        TraFlag = [0, 0, 0]
        StrategyReturn = [0]
        tempcheck = []
        MACD_maxcheck = 0.1
        lengthtrack = 0

        data = self.marktradateclosetime(data, '15:00:00', '60min')
        data.reset_index(inplace=True)

        for i in range(1, len(data)):
            MACD_maxcheck = self.maxcrestcheck(self, data, MACD_maxcheck, i)
            StrategyReturn.append(TraFlag[-2] * data['IntraReturn'].iloc[i])

            if (flag[-1] == 0):
                if ((data['MACD'][i - 1] <= 0) & (data['MACD'][i] >= 0)):
                    data, flag,Buy,Sell = self.goldensignalcheck( data, i, flag, Buy, Sell)
                if ((data['MACD'][i - 1] >= 0) & (data['MACD'][i] <= 0) & (lengthtrack > 5)):  ##死叉
                    data, flag, Buy, Sell = self.deathsignalcheck(data, i, flag, Buy, Sell)
                else:
                    flag.append(flag[-1])
                    Buy.append(0)
                    Sell.append(0)
            elif (flag[-1] == 1):
                tempcheck.append(StrategyReturn[-1])
                b = [x + 1 for x in tempcheck]
                returncheck = np.prod(b) - 1
                ### 平仓signal, 波峰20%即平仓
                data,returncheck,Sell,Buy,flag = self.longclearcheck(self, data, returncheck, MACD_maxcheck, i, Sell, Buy, flag)
            elif (flag[-1] == -1):
                tempcheck.append(StrategyReturn[-1])
                b = [x + 1 for x in tempcheck]
                returncheck = np.prod(b) - 1
                data, returncheck, Sell, Buy, flag = self.shortclearcheck(self, data, returncheck, MACD_maxcheck, i,Sell, Buy, flag)
            else:
                tempcheck = []
                flag.append(flag[-1])
                Buy.append(0)
                Sell.append(0)

            Pos.append(flag[-1])
            TraFlag.append(flag[-1])
            if (data['MACD'][i - 1] >= 0) & (data['MACD'][i] <= 0):
                lengthtrack = 1
            else:
                lengthtrack = lengthtrack + 1
        Pos = Pos[:-1]
        TraFlag = TraFlag[:-2]
        return data, (Buy, Sell, flag, Pos, TraFlag)

    def runsignal(self,A, B, C, D):
        #     result = runmaindata(cat_all_data)
        result = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\Combine2019_2020\\Combined\\a_maindata_correctbyVol.csv')
        result = result[result['TraDate'] >= 201901010]
        data, a = self.MACDNEW_ML(result, A, B, C, D)
        data['Buy'] = a[0]
        data['Sell'] = a[1]
        data['flag'] = a[2]
        data['Pos'] = a[3]
        data['TraFlag'] = a[4]
        data['Stopup'] = a[5]
        data['Stopdown'] = a[6]

        data['Pricipal'] = 100
        data['leverage'] = 10
        data['StrategyReturn'] = data['TraFlag'] * data['IntraReturn']
        data['CumStrategyReturn'] = (data['StrategyReturn'] + 1).cumprod()
        data['CurrentBook'] = data['CumStrategyReturn'] * data['Pricipal'] * data['leverage']

        data['Earning'] = data['CurrentBook'] - data['Pricipal'] * data['leverage']
        earning = -data['Earning'].iloc[-1]

        return earning

    def runModel(self, A,B,C,D):
        result = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\Combine2019_2020\\Combined\\a_maindata_correct.csv')
        # result = result[result['TraDate'] >= 20200102]
        result = result[result['TraDate'] >= 20191011]
        data, a = self.MACDNEW_ML(result, A, B, C, D)
        data['Buy'] = a[0]
        data['Sell'] = a[1]
        data['flag'] = a[2]
        data['Pos'] = a[3]
        data['TraFlag'] = a[4]
        data['Stopup'] = a[5]
        data['Stopdown'] = a[6]

        data['Pricipal'] = 100
        data['leverage'] = 10
        data['StrategyReturn'] = data['TraFlag'] * data['IntraReturn']
        data['CumStrategyReturn'] = (data['StrategyReturn'] + 1).cumprod()
        data['CurrentBook'] = data['CumStrategyReturn'] * data['Pricipal'] * data['leverage']

        data['Earning'] = data['CurrentBook'] - data['Pricipal'] * data['leverage']

if __name__ == '__main__':
    MACDModel = MACDModel()
    A, B, C, D = [0.0112, -0.0149, 0.0062, -0.0146]
    MACDModel.runModel(A,B,C,D)