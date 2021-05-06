# encoding:UTF-8
from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import pandas as pd
import logging
from benchmark.marketdata.MarketDataDownloader import *
import numpy as np
import datetime
import json
from decimal import *

getcontext().prec = 6
import os
import paramiko


class BBGUploader(Base):

    def __init__(self, filePath):
        LogManager('BBGUploader')
        self.sourceFilePath = filePath
        self.hostname = 'bfmrr-sftp.bloomberg.com'
        self.username = 'u90605725'
        self.password = '+A06G30=UvuvR8c{'
        self.port = 30206
        self.destination = '/'

        # self.hostname = '192.168.200.69'
        # self.username = 'pinpoint'
        # self.password = 'abc.123'
        # self.port = 22
        # self.destination = '/tmp/'

    def uploadFile(self, fileNameList):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=self.hostname, port=self.port, username=self.username, password=self.password)
        ftp_client = client.open_sftp()
        for fileName in fileNameList:
            if ('.csv' in fileName):
                ftp_client.put(self.sourceFilePath+fileName, self.destination+fileName)
        ftp_client.close()

    def verifyFileName(self):
        fileNameList = os.listdir(self.sourceFilePath)
        for fileName in fileNameList:
            if ('.csv' in fileName):
                index = fileName.find('.csv')
                fileDateStr = fileName[index - 8:index]
                fileDate = datetime.datetime.strptime(fileDateStr, '%Y%m%d')
                data = pd.read_csv(self.sourceFilePath + fileName)
                data.rename(columns=lambda x: x.strip(), inplace=True)
                data['Date'] = pd.to_datetime(data['Date'])
                fileContentDate = data['Date'].iloc[0]
                if fileContentDate != fileDate:
                    fileContentDateStr = fileContentDate.strftime('%Y%m%d')
                    newFileName = fileName[0:index - 8] + fileContentDateStr + '.csv'
                    logging.warning('wrong date at file name:' + fileName + ', correcting to ' + newFileName)
                    os.rename(self.sourceFilePath + fileName, self.sourceFilePath + newFileName)

    def moveFilesToProcessed(self, fileNameList):
        processedDir = self.sourceFilePath + 'processed\\'
        for fileName in fileNameList:
            if ('.csv' in fileName):
                os.rename(self.sourceFilePath + fileName, processedDir + fileName)

    def startUpload(self):
        self.verifyFileName()
        fileNameList = os.listdir(self.sourceFilePath)
        fileNameList = [x for x in fileNameList if '.csv' in x]
        if len(fileNameList) == 3:
            self.uploadFile(fileNameList)
            self.moveFilesToProcessed(fileNameList)
            self.sendMessageToMail(['vivienne.chan@pinpointfund.com','patrick.lo@pinpointfund.com'], '[SUCCESS] VaR File Uploaded', (',').join(fileNameList))
        else:
            logging.warning('file not ready')

    def tryListFile(self):
        fileNameList = os.listdir(self.sourceFilePath)
        logging.info(fileNameList)
        fileNameList = [x for x in fileNameList if '.csv' in x]
        logging.info(fileNameList)

if __name__ == '__main__':
    env = 'prod'
    # path = '\\\\192.168.200.3\\ftp\\Linedata_Risk\\VaR\\'
    path = 'C:\\devel\\file\\VaR\\'
    bbgUploader = BBGUploader(path)
    bbgUploader.tryListFile()
    #bbgUploader.startUpload()
