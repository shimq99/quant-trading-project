# # Generate dataset
import pandas as pd
import numpy as np
import os
import fnmatch
import fileinput
from scipy.interpolate import interp1d
from sklearn.metrics import mean_squared_error
import time
import re
import datetime

# coding=utf-8
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# encoding:UTF-8
import sys
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


pd.set_option('display.max_columns', 10)
pd.set_option('precision', 15)
import decimal

decimal.getcontext().prec = 10
from tools import PandasDBUtils as pdUtil
from log.LogManager import LogManager

class CheckwithCY(Base):
    def __init__(self):
        LogManager('CheckwithCY')
        logging.info('CheckwithCY')

    def runTest(self):
        startdate = pd.Timestamp(2020, 1, 1, 0)
        enddate = pd.Timestamp(2021, 1, 1, 0)
        tick_data = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\changyuAL2012.csv')
        tick_data['Time'] = pd.to_datetime(tick_data['Time'], infer_datetime_format=True)
        changyu_AL2012 = tick_data[(tick_data['Time'] >= startdate) & (tick_data['Time'] < enddate)].copy()
        changyu_AL2012.describe()
        kline5min_data = pd.read_csv('C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\al2012_5min1.csv')
        cy = changyu_AL2012['Time'].tolist()
        my = kline5min_data['DateTime'].tolist()
        a = list(set(my) - set(cy))
        b = [x.split() for x in a]
        a

if __name__ == '__main__':
    CheckwithCY = CheckwithCY()
    CheckwithCY.runTest()
