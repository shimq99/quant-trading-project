# encoding:UTF-8
from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import pandas as pd
import logging
from benchmark.tools import PandasDBUtils as pdUtil
from benchmark.tools import Utils
import numpy as np
import datetime
import json
from decimal import *
getcontext().prec = 6
from benchmark.position.PmsfCvfPosition import *
from benchmark.risk_control_2019.RiskMaxAbsValue import *
import os
import os.path
from openpyxl import load_workbook
from shutil import copyfile

'''
ExposureTeamListUpdater临时用于update matlab risk 任务读取的Config.xlsx
之后如果matlab任务被python替换,可以不再使用 
'''
class ExposureTeamListUpdater(Base):
    def __init__(self, env, config_path):
        self.env = env
        self.config_path = config_path
        LogManager('ExposureTeamListUpdater')

    def updateToDataBase(self, sql, updateRecords):
        if updateRecords:
            self.cursor.executemany(sql, updateRecords)

    def selectFromDataBase(self, sql):
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return resultDataFrame

    def insertToDatabase(self, sql, data):
        if data:
            self.cursor.executemany(sql, data)
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def getFundBookBlackListInfo(self):
        sql = 'SELECT Id,FundId,FundCode,BookCode,IsShow,IsCalc FROM RiskDb.ref.RiskReportBlackList where IsCalc=0'
        data = self.selectFromDataBase(sql)
        data['FundBookCode'] = data['FundCode'] +'-'+data['BookCode']
        return data

    def getActiveFundBok(self):
        sql = 'SELECT DISTINCT EnglishName, FundCode, BookCode FROM ReferenceData.ref.UserAccountView WHERE EnglishName = BookManager AND Role = \'PortMngr\' AND IsActive = 1 AND IsRealFund = 1'
        data = self.selectFromDataBase(sql)
        data['Fund'] = data['FundCode']
        data['Book'] = data['BookCode']
        return data

    def getFundBookInfo(self, dateStr):
        exclude_fund=['CPTF']
        exclude_book = ['GTJA','YANBINGLIANG','ZHENGTIANTAO190601','ZOUZHOU190601']
        sql = 'SELECT Distinct Fund,Book FROM [Portfolio].[perf].[NavView] where Date=\''+dateStr+'\' and Fund not in (\'' + ('\',\'').join(exclude_fund)+'\') and Book not in (\'' + ('\',\'').join(exclude_book)+'\')'
        data = self.selectFromDataBase(sql)
        return data

    def updateConfig(self, dateStr):
        fundbook_info = self.getFundBookInfo(dateStr)
        fundbook_blacklist_info = self.getFundBookBlackListInfo()
        config_path = self.config_path + 'Config.xlsx'

        existing_book_detail = pd.read_excel(config_path, sheet_name='Book_Detail')
        existing_book_detail_list = list(existing_book_detail['NAME'].unique())


        blackList = list(fundbook_blacklist_info['FundBookCode'].unique())
        fundbook_info['SubFund'] = fundbook_info['Book']
        fundbook_info['NAME'] = fundbook_info['Fund'] +'-'+fundbook_info['SubFund']
        fundbook_info = fundbook_info[~fundbook_info['NAME'].isin(blackList)]
        fundbook_info['USER'] = ''
        fundbook_info['In_Force'] = '1'
        fundbook_info['Drawdown'] = '0.05'
        fundbook_info['ID'] = np.arange(1, len(fundbook_info) + 1)


        fundbook_info['ID'] = fundbook_info['ID'].astype(str)
        fundbook_info= fundbook_info[['ID', 'Fund', 'SubFund', 'NAME', 'USER', 'In_Force', 'Drawdown']]
        pending_append_data = pd.DataFrame()
        for index, data_row in fundbook_info.iterrows():
            name = data_row['NAME']
            if not name in existing_book_detail_list:
                pending_append_data = pending_append_data.append(data_row)
        existing_book_detail = existing_book_detail.append(pending_append_data)
        existing_book_detail.index = np.arange(1, len(existing_book_detail) + 1)
        existing_book_detail['ID'] = existing_book_detail.index
        existing_book_detail['ID'] = existing_book_detail['ID'].astype(str)
        existing_book_detail['In_Force'] = '1'
        existing_book_detail['Drawdown'] = '0.05'
        existing_book_detail = existing_book_detail[~existing_book_detail['NAME'].isin(blackList)]
        book = load_workbook(config_path)
        del book['Book_Detail']
        writer = pd.ExcelWriter(config_path, engine='openpyxl')
        writer.book = book
        existing_book_detail[['ID', 'Fund', 'SubFund', 'NAME', 'USER', 'In_Force', 'Drawdown']].to_excel(writer, sheet_name='Book_Detail', index=False)

        writer.save()
        writer.close()

    def backupConfigFile(self, backup_filename):
        backup_filepath =self.config_path+'\\config-bak\\' + backup_filename
        if os.path.exists(backup_filepath):
            os.remove(backup_filepath)
        copyfile(self.config_path + 'Config.xlsx', self.config_path+'\\config-bak\\' + backup_filename)

    def update(self):
        currentDate = datetime.datetime.now()
        weekDay = currentDate.date().weekday()
        diff = 1
        if (weekDay == 5 or weekDay == 6):
            return
        elif (weekDay == 0):
            diff += 2
        runYesterDay = currentDate - datetime.timedelta(days=diff)
        runYesterDayStr = runYesterDay.strftime('%Y-%m-%d')
        backup_filename ='Config-bak-'+runYesterDayStr+'.xlsx'
        self.initSqlServer(self.env)
        self.backupConfigFile(backup_filename)
        self.updateConfig(runYesterDayStr)
        self.closeSqlServerConnection()

if __name__ == '__main__':
    env = 'prod'
    exposureTeamListUpdater = ExposureTeamListUpdater(env,'C:\\Script\\navloader\\')
    exposureTeamListUpdater.update()


