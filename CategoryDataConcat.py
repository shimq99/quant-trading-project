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
import datetime as dt
import logging
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
from scipy.optimize import minimize, LinearConstraint

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

decimal.getcontext().prec = 10
from tools import PandasDBUtils as pdUtil
from log.LogManager import LogManager

class CATConcator(Base):
    def __init__(self):
        LogManager('CATConcator')
        logging.info('CATConcator')
        self.filename =''
        self.opentimes =[]

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
                if (re.findall('([a-zA-Z ]*)\d*.*', files)[0] == cat.lower()) & (files.endswith('_3min.csv')):
                    result.append(os.path.join(search_path, dir, files))
        return result

    def getmaincontract(self,date, cat_all_data, lastmaincontract):
        ### indentify maincontract based on last trading day highest OI /  identify by Vol is ok as well
        lastTraDate = cat_all_data[cat_all_data['TraDate'] < date]['TraDate'].iloc[-1]
        cat_all_data['DateTime'] = pd.to_datetime(cat_all_data['DateTime'])
        a = cat_all_data.sort_values(by=['TraDate', 'DateTime', 'OI_total'], ascending=[True, True, True])
        maincontract = a[(a['TraDate'] == lastTraDate) & (a['ContractName'] >= lastmaincontract)]['ContractName'].iloc[-1]
        return lastTraDate, maincontract

    def readallcatcontractsdata(self,cat):
        ### find all paths of the Category and concat them together
        path = 'F:\\KlineData'
        result = pd.DataFrame()
        catfilepaths = self.find_filespath(cat, path)
        print(catfilepaths)
        for catfile in catfilepaths:
            contractname = re.split('[ _ |.]', str(os.path.basename(catfile)))[0]
            df = pd.read_csv(catfile)
            df['ContractName'] = contractname
            result = pd.concat([result, df], ignore_index=True)
        return result

    def runmaindata(self):
        for cat in ['PP','V','J', 'JM', 'Y', 'M', 'P', 'AG', 'AU']:
            cat_all_data = self.readallcatcontractsdata(cat)
            datelist = list(set(cat_all_data['TraDate'].tolist()))
            datelist.sort()
            del datelist[0]
            result = pd.DataFrame()
            ### set initial main contract, since data starts 2019, it's safe to set initial main contract as 1901
            lastmaincontract = cat.lower()+'1901'
            for date in datelist:
                lastTraDate, maincontract = self.getmaincontract(date, cat_all_data, lastmaincontract)
                print(lastTraDate, maincontract)
                lastmaincontract = maincontract
                main_data = cat_all_data[(cat_all_data['TraDate'] == date) & (cat_all_data['ContractName'] == maincontract)].copy()
                result = pd.concat([result, main_data], ignore_index=True)
            result = result.sort_values(by='DateTime')
            result.to_csv('F:\\maincontract\\'+cat+'.csv')

        return result
    


if __name__ == '__main__':
    CATConcator = CATConcator()
    CATConcator.runmaindata()
