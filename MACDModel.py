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
        return MACD_maxcheck

    def goldensignalcheck(self, lengthtrack,tempcheck,SignalType,checki,checktype,i,data, flag,Buy,Sell):
         ##金叉条件下，同时需要满足金叉点的相邻前一个MACD的波段周期要超过5天，如果出现，金叉或死叉的开仓信号+前一个波段的MACD周期不超过5天的情形，那么你就等待another two days再开仓。
         if (lengthtrack > 5):
             tempcheck = []
             flag.append(1)
             Buy.append(1)
             SignalType.append(1)
             Sell.append(0)
         else:
             checki = i + 1
             checktype = 'gold'
             flag.append(flag[-1])
             SignalType.append(0)
             Buy.append(0)
             Sell.append(0)
         return lengthtrack,tempcheck,SignalType, flag,Buy,Sell,checki,checktype

    def deathsignalcheck(self, lengthtrack,tempcheck,SignalType,checki,checktype,i,data, flag,Buy,Sell):
        if (lengthtrack > 5):
            tempcheck = []
            flag.append(-1)
            SignalType.append(2)
            Buy.append(0)
            Sell.append(1)
        else:
            checki = i + 1
            checktype = 'death'
            #                     print(data['TraDate'][i],i,checki, checktype)
            flag.append(flag[-1])
            SignalType.append(0)
            Buy.append(0)
            Sell.append(0)
        return lengthtrack,tempcheck,SignalType, flag,Buy,Sell,checki,checktype

    def signaltypecheck(self,checktype,flag,Buy,SignalType,Sell):
        ## signal type tracker, to show in the final transaction detail output
        if checktype == 'gold':
            tempcheck = []
            flag.append(1)
            Buy.append(1)
            SignalType.append(3)
            Sell.append(0)
        elif checktype == 'death':
            tempcheck = []
            flag.append(-1)
            SignalType.append(4)
            Buy.append(0)
            Sell.append(1)
        else:
            flag.append(flag[-1])
            SignalType.append(0)
            Buy.append(0)
            Sell.append(0)
        return flag,Buy,SignalType,Sell,tempcheck

    def longclearcheck(self,tempcheck,data,SignalType,returncheck,lengthtrack,MACD_maxcheck,i,Sell,Buy,flag, A, B):
        if (returncheck >= A): ## stop profit
            Sell.append(1)
            Buy.append(0)
            SignalType.append(5)
            flag.append(0)
            tempcheck = []
        elif (returncheck <= B): ## stop loss
            Sell.append(1)
            Buy.append(0)
            SignalType.append(6)
            flag.append(0)
            tempcheck = []
        elif ((abs(data['MACD'][i]) < 0.2 * abs(MACD_maxcheck)) & (lengthtrack >= 4)): ## MACD绝对值<=20%*波段内的最大值的绝对值 且这个MACD的波段周期走过了5天
            Sell.append(1)
            Buy.append(0)
            SignalType.append(7)
            flag.append(0)
            tempcheck = []
        elif ((data['MACD'][i - 1] >= 0) & (data['MACD'][i] <= 0)): ## 遇到下一个死叉 平仓
            Sell.append(1)
            Buy.append(0)
            SignalType.append(2)
            flag.append(0)
            tempcheck = []
        else:
            flag.append(flag[-1])
            SignalType.append(0)
            Buy.append(0)
            Sell.append(0)
        return returncheck,Sell,Buy,flag,SignalType,tempcheck

    def shortclearcheck(self,tempcheck,data,SignalType,returncheck,lengthtrack,MACD_maxcheck,i,Sell,Buy,flag, C,D):
        if (returncheck >= C): ## stop profit
            Sell.append(0)
            Buy.append(1)
            flag.append(0)
            tempcheck = []
            SignalType.append(5)
        elif (returncheck <= D): ## stop loss
            Sell.append(0)
            Buy.append(1)
            flag.append(0)
            tempcheck = []
            SignalType.append(6)
        elif ((abs(data['MACD'][i]) < 0.2 * abs(MACD_maxcheck)) & (lengthtrack >= 4)): ## MACD绝对值<=20%*波段内的最大值的绝对值 且这个MACD的波段周期走过了5天
            Sell.append(0)
            Buy.append(1)
            flag.append(0)
            tempcheck = []
            SignalType.append(7)
        elif ((data['MACD'][i - 1] >= 0) & (data['MACD'][i] <= 0)): ## 遇到下一个金叉 平仓
            Sell.append(0)
            Buy.append(1)
            flag.append(0)
            tempcheck = []
            SignalType.append(1)
        else:
            flag.append(flag[-1])
            Buy.append(0)
            Sell.append(0)
            SignalType.append(0)
        return returncheck,Sell,Buy,flag,SignalType,tempcheck

    def MACDNEW_ML(self,data, A, B, C, D):
        data['IntraReturn'] = data['Close'].pct_change().fillna(0)
        Buy = [0]
        Sell = [0]
        SignalType = [0]
        flag = [0]
        Pos = [0, 0] ## since get into the position at the next timestamp after seeing the trading signal
        TraFlag = [0, 0, 0] ## to calculate cumulative return, there's a delay after the position is 1 / -1
        StrategyReturn = [0]
        tempcheck = []
        MACD_maxcheck = 0.1
        lengthtrack = 0 ##前一波段MACD周期的tracker
        checki = 10000000 ## used for wait for another two days (金叉点或死叉点的相邻前一个MACD的波段周期要超过5天。其中第二个条件是为了避免金叉或死叉反复出现，形成虚假信号，误以为趋势形成。如果出现，金叉或死叉的开仓信号+前一个波段的MACD周期不超过5天的情形，那么你就等待another two days再开仓)
        checktype = 'non'
        # data = self.marktradateclosetime(data, '15:00:00', '60min')
        data.reset_index(inplace=True)
        for i in range(1, len(data)):
            MACD_maxcheck = self.maxcrestcheck(data, MACD_maxcheck, i)
            StrategyReturn.append(TraFlag[-2] * data['IntraReturn'].iloc[i])
            if (flag[-1] == 0): ## open position criterion
                if ((data['MACD'][i - 1] <= 0) & (data['MACD'][i] >= 0)): #金叉
                    lengthtrack,tempcheck,SignalType,flag,Buy,Sell,checki,checktype = self.goldensignalcheck(lengthtrack,tempcheck,SignalType,checki,checktype,i,data, flag,Buy,Sell)
                elif ((data['MACD'][i - 1] >= 0) & (data['MACD'][i] <= 0) & (lengthtrack > 5)):  ##死叉
                    lengthtrack, tempcheck, SignalType, flag, Buy, Sell,checki,checktype = self.deathsignalcheck(lengthtrack,tempcheck,SignalType,checki,checktype,i,data, flag,Buy,Sell)
                elif (i == checki):
                    flag,Buy,SignalType,Sell,tempcheck = self.signaltypecheck(checktype, flag, Buy, SignalType, Sell)
                else:
                    flag.append(flag[-1])
                    SignalType.append(0)
                    Buy.append(0)
                    Sell.append(0)
            elif (flag[-1] == 1): ## close long position
                tempcheck.append(StrategyReturn[-1])
                b = [x + 1 for x in tempcheck]
                returncheck = np.prod(b) - 1 ## check current cummulative return, used for stop loss / stop profit
                ### 平仓signal, 波峰20%即平仓
                returncheck,Sell,Buy,flag,SignalType,tempcheck = self.longclearcheck(tempcheck,data,SignalType,returncheck,lengthtrack,MACD_maxcheck,i,Sell,Buy,flag, A, B)
            elif (flag[-1] == -1):
                tempcheck.append(StrategyReturn[-1])
                b = [x + 1 for x in tempcheck]
                returncheck = np.prod(b) - 1
                returncheck, Sell, Buy, flag, SignalType, tempcheck = self.shortclearcheck(tempcheck,data, SignalType,returncheck,lengthtrack,MACD_maxcheck, i, Sell,Buy, flag, C,D)
            else:
                tempcheck = []
                SignalType.append(0)
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
        return data, (Buy,Sell,flag,Pos,TraFlag,SignalType)

    def runsignal(self,A, B, C, D):
        #     result = runmaindata(cat_all_data)
        result = pd.read_csv('F:\\maincontract\\P.csv')
        result = self.calcontractMACD(result) ### calculate indicators dif, dea, MACD
        result = result[(result['TraDate'] >= 20200102) & (result['TraDate'] <= 20200601)] ## set in sample data like 2020.1-2020.6
        data, a = self.MACDNEW_ML(result, A, B, C, D)
        data['Buy'] = a[0]
        data['Sell'] = a[1]
        data['flag'] = a[2]
        data['Pos'] = a[3]
        data['TraFlag'] = a[4]
        data['SignalType'] = a[5]
        data['Pricipal'] = 100
        data['leverage'] = 10
        data['StrategyReturn'] = data['TraFlag'] * data['IntraReturn']
        data['CumStrategyReturn'] = (data['StrategyReturn'] + 1).cumprod()
        data['CurrentBook'] = data['CumStrategyReturn'] * data['Pricipal'] * data['leverage']
        data['Earning'] = data['CurrentBook'] - data['Pricipal'] * data['leverage']
        earning = - data['Earning'].iloc[-1] ### minimization optimization function thus minimize the oppoiste earning (maximize earning)
        return earning

    def runModel(self, A,B,C,D):
        result = pd.read_csv('F:\\maincontract\\P.csv')
        result = self.calcontractMACD(result)
        # result = result[result['TraDate'] >= 20200102]
        result = result[result['TraDate'] >= 20191011]
        data, a = self.MACDNEW_ML(result, A, B, C, D)
        data['Buy'] = a[0]
        data['Sell'] = a[1]
        data['flag'] = a[2]
        data['Pos'] = a[3]
        data['TraFlag'] = a[4]
        data['SignalType'] = a[5]
        data['Pricipal'] = 100
        data['leverage'] = 10
        data['StrategyReturn'] = data['TraFlag'] * data['IntraReturn']
        data['CumStrategyReturn'] = (data['StrategyReturn'] + 1).cumprod()
        data['CurrentBook'] = data['CumStrategyReturn'] * data['Pricipal'] * data['leverage']
        data['preEarning'] = data['CurrentBook'] - data['Pricipal'] * data['leverage']
        data['ExecutePrice'] = data['Close'].shift(-1)
        data['Earning'] = data['preEarning'].shift(-1)
        data['ExecuteDate'] = data['TraDate'].shift(-1)
        # data['ExecuteDate'] = data['ExecuteDate'].astype(int)
        data = data.ffill()
        data['ExecuteDate'] = data['ExecuteDate'].astype(int)
        # data['ExecuteDate']  = pd.to_datetime(data['ExecuteDate'] )

    class RandomDisplacementBounds(object):
        def __init__(self, xmin, xmax, stepsize=0.1): ### set optimization stepsize
            self.xmin = xmin
            self.xmax = xmax
            self.stepsize = stepsize

        def __call__(self, x):
            """take a random step but ensure the new position is within the bounds """
            min_step = np.maximum(self.xmin - x, -self.stepsize)
            max_step = np.minimum(self.xmax - x, self.stepsize)
            random_step = np.random.uniform(low=min_step, high=max_step, size=x.shape)
            xnew = x + random_step
            return xnew

    def optimizeABCD(self):
        bounds = [(0.005, 0.015), (-0.040, -0.005), (0.005, 0.015), (-0.040, -0.005)] # set boundary for each parameter (A, B, C, D upper and lower bound setting)
        bounded_step = self.RandomDisplacementBounds(np.array([b[0] for b in bounds]), np.array([b[1] for b in bounds]))
        """ Custom optimizer """
        minimizer_kwargs = {"method": "L-BFGS-B", "bounds": bounds}
        """ Solve with bounds """
        x0 = [0.015, -0.015, 0.015, -0.015] ## set initial x0 for optimization
        # x0 = [0.04,-0.02,0.04,-0.02]
        ret = basinhopping(self.f, x0, minimizer_kwargs=minimizer_kwargs, niter=1, take_step=bounded_step)
        ### optimized A,B,C,D results
        print(ret.x)

if __name__ == '__main__':
    MACDModel = MACDModel()
    ### if A, B, C, D have already been optimized and known
    # A, B, C, D = [0.0112, -0.0149, 0.0062, -0.0146]
    # MACDModel.runModel(A,B,C,D)

    ### if want to optimize A,B,C,D
    MACDModel.optimizeABCD()