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
from os import listdir
from os.path import join
import time
import re
import datetime as dt


pd.set_option('display.max_columns', 10)
pd.set_option('precision', 15)
import decimal

decimal.getcontext().prec = 10
from tools import PandasDBUtils as pdUtil
from log.LogManager import LogManager

class RawDataPreparation(Base):
    def __init__(self):
        LogManager('QTDataCleaning')
        logging.info('QTDataCleaning')

    def find_filenames(self,search_path):
        ### Combine Raw Data for Y2020
        file_list0 = []
        for dir, dir_name, file_list in os.walk(search_path):
            for file in file_list:
                if file.endswith(".csv"):
                    file_list0.append(file)
        return list(set(file_list0))

    def find_filespath(self,filename, search_path):
        ### Combine Raw Data for Y2020
        result = []

        # Walking top-down from the root
        for dir, dir_name, file_list in os.walk(search_path):
            if filename in file_list:
                result.append(os.path.join(search_path, dir, filename))
        return result

    def combinedcontract(self,name, path, storedir):
        ### Combine Raw Data for Y2020
        files = self.find_filespath(name, path)
        combined_df = pd.concat([pd.read_csv(file) for file in files])
        combined_df.to_csv(storedir + '\\' + name)
        print(name + '_saved')
        return combined_df

    def runrawdata(self):
        ### Combine Raw Data for Y2020
        storedir = 'D:\\1920total'

        ### first time running raw data
        path = r'D:\\Combined'

        # filenames = ['a2001.csv','a2003.csv', 'a2005.csv', 'a2007.csv', 'a2009.csv', 'a2011.csv']
        filenames = self.find_filenames(path)
        for name in filenames:
            logging.info('running'+' '+name)
            self.combinedcontract(name, path, storedir)
        ### 查漏补缺 Raw Data

        # f = open("C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\202101-202103-a2111-Lv2\\202101-a2111\\20210104-a2111", "r")
        # f
        # tick_data
        # path0 = r'C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data\\2020tickdata'
        # a = self.find_filenames(path0)
        # path = r'C:\\Users\\shimq\\Desktop\\CMSI\\Tick Data'
        # b = self.find_filenames(path)
        # filenames = list(set(b) - set(a))
        # for name in filenames:
        #     logging.info('running'+' '+name)
        #     self.combinedcontract(name, path, storedir)




if __name__ == '__main__':
    RawDataPreparation = RawDataPreparation()
    RawDataPreparation.runrawdata()
