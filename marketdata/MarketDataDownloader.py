from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import requests
import json
import re
import datetime
import logging
import pandas as pd
from benchmark.tools import PandasDBUtils as pdUtil
import pyodbc

class MarketDataDownloader(Base):

    def __init__(self, env, bbgUrl='http://192.168.203.40:9090'):
        #self.bbgUrl = 'http://192.168.203.79:9098'
        #self.bbgUrl= 'http://192.168.203.40:9090' ##hkpc-bbg risk
        self.bbgUrl=bbgUrl
        #self.bbgUrl = 'http://192.168.203.90:9090'
        LogManager('MarketDataDownloader')
        self.env=env
        self.inceptionDate = datetime.datetime.strptime('2018-12-31', '%Y-%m-%d')

    '''
    Column 1 - Adjustment Date
    Column 2 - Adjustment Factor
    Column 3 - Operator Type (1=div, 2=mult, 3=add, 4=sub. Opposite for Volume)
    Column 4 - Flag (1=prices only, 2=volumes only, 3=prices and volumes)  
    '''
    def getEquityDvdSplitInfoFromBbg(self,tickers):
        mdsUrl = self.bbgUrl + '/mktsvc/refdata'
        headers = {'content-type': 'application/json'}
        # DVD_HIST_ALL, DVD_HIST get only cash divident
        payload = {"tickers": tickers, "fields": ["EQY_DVD_ADJUST_FACT"]}
        r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
        obj = json.loads(r.text)
        print obj['data']
        # should use re.findall(r'Adjustment Date=(.+?), Adjustment Factor=(.+?), Adjustment Factor Operator Type=(.+?), Adjustment Factor Flag=(.+?)}', s)
        for ticker, v in obj['data'].items():
            info = v["EQY_DVD_ADJUST_FACT"]
            events = re.findall(r'{(.+?)}', info)
            for s in events:
                items = s.split(', ')
                fieldDict = dict()
                for item in items:
                    field = re.findall(r'^(.+?)=', item)[0]
                    value = re.findall(r'=(.+?)$', item)[0]
                    fieldDict[field] = value

                try:

                    adjustDatePy = datetime.datetime.strptime(fieldDict['Adjustment Date'], '%Y-%m-%d')
                    if (adjustDatePy > self.inceptionDate):
                        print ticker, fieldDict['Adjustment Date'], fieldDict['Adjustment Factor'], \
                            fieldDict['Adjustment Factor Operator Type'], fieldDict['Adjustment Factor Flag']
                        sql = 'insert into RiskDb.bench.BbgTickerDvdSplitInfo(BbgTicker, AdjustmentDate, AdjustmentFactor, OperatorType, Flag) \
                              values(?, ?, ?, ?, ?)'
                        self.cursor.execute(sql, (
                        ticker, fieldDict['Adjustment Date'], float(fieldDict['Adjustment Factor']), \
                        int(float(fieldDict['Adjustment Factor Operator Type'])),
                        int(float(fieldDict['Adjustment Factor Flag']))))

                except Exception, e:
                    logging.error('Exception on download Equity Dvd Split Info Data,error= '+e.args[1]+', ticker='+ticker)
                    continue

    def getStockCashDividentEventsFromBbg(self, tickers):
        mdsUrl = self.bbgUrl + '/mktsvc/refdata'
        headers = {'content-type': 'application/json'}
        #DVD_HIST_ALL, DVD_HIST get only cash divident
        payload = {"tickers": tickers, "fields": ["DVD_HIST"]}
        r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
        obj = json.loads(r.text)
        records = []
        for ticker, v in obj['data'].items():
            info = v["DVD_HIST"]
            events = re.findall(r'{Declared Date=(.+?), Ex-Date=(.+?), Record Date=(.+?), Payable Date=(.+?), Dividend Amount=(.+?), Dividend Frequency=(.+?), Dividend Type=(.+?)}', info)
            for s in events:
                if s[1] is None or s[1] == 'null':
                    logging.warn('ExDate is null, ignore')
                    continue
                exDatePy = datetime.datetime.strptime(s[1], '%Y-%m-%d')
                if(exDatePy > self.inceptionDate):
                    tmp = []
                    tmp.append(ticker)
                    for i in range(len(s)):
                        if(s[i] == 'null'):
                            tmp.append(None)
                        else:
                            tmp.append(s[i])
                    divAmount = float(tmp[5])
                    tmp[5] = divAmount
                    records.append(tmp)
        print records
        if records:
            try:
                sql = 'insert into RiskDb.bench.BbgCashDividentInfo(BbgTicker, DeclaredDate, ExDate, RecordDate, PayableDate, DividentAmount, \
                DividentFrequency, DividentType) values(?,?,?,?,?,?,?,?)'
                self.cursor.executemany(sql, records)
            except Exception,e:
                logging.error('error while saving cash div, pls check data value')
                raise

    def addStockDividentDayClosePrice(self, startDayStr, stopDayStr):
        sql = 'select BbgTicker, ExDate from RiskDb.bench.BbgCashDividentInfo where ExDate between ? and ?'
        self.cursor.execute(sql, (startDayStr, stopDayStr))
        fields = ['PX_LAST']
        #fields = ['PX_LAST']
        mdsUrl = self.bbgUrl + '/mktsvc/histdata'
        headers = {'content-type': 'application/json'}
        stockDayList = self.cursor.fetchall()
        for stockDay in stockDayList:
            stock = stockDay[0]
            dateStr = stockDay[1].strftime('%Y-%m-%d')
            payload = {"tickers": [stock], "fields": fields, 'startDate': dateStr, 'endDate': dateStr, 'period': 'DAILY'}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            data = []
            for stock, v in obj['data'].items():
                for dateStr, fieldDict in v.items():
                    #stock, dateStr, fieldDict['PX_LAST']
                    try:
                        sql = 'update RiskDb.bench.BbgCashDividentInfo set PayDayClosePrice=? where BbgTicker=? and ExDate=?'
                        self.cursor.execute(sql, (fieldDict['PX_LAST'], stock, dateStr))
                    except Exception,e:
                        logging.error('update ' + stock + ' ' + dateStr + ' fails')

    def getOptionVolume(self,tickers, startDayStr, stopDayStr):
        fields = ['PX_LAST','PX_VOLUME']
        # fields = ['PX_LAST']
        mdsUrl = self.bbgUrl + '/mktsvc/histdata'
        headers = {'content-type': 'application/json'}
        payload = {"tickers": tickers, "fields": fields, 'startDate': startDayStr, 'endDate': stopDayStr, 'period': 'DAILY', 'days': 'NON_TRADING_WEEKDAYS'}
        r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
        obj = json.loads(r.text)
        data = []
        for stock, v in obj['data'].items():
            for dateStr, fieldDict in v.items():
                # stock, dateStr, fieldDict['PX_LAST']
                try:
                    px_vol = 0
                    px_last = 0
                    if 'PX_VOLUME' in fieldDict:
                        px_vol = fieldDict['PX_VOLUME']
                    if 'PX_LAST' in fieldDict:
                        px_last = fieldDict['PX_LAST']
                    sql = 'insert into [RiskDb].[bench].[BbgTickerEodPrice]([BbgTicker],[TradeDate],[ClosePrice],[FloatVolume]) Values(?,?,?,?)'
                    self.cursor.execute(sql, (stock, dateStr, px_last, px_vol))
                except Exception, e:
                    logging.error('update ' + stock + ' ' + dateStr + ' fails')


    def getAdrUnderlyingTicker(self,tickers):
        mdsUrl = self.bbgUrl + '/mktsvc/refdata'
        headers = {'content-type': 'application/json'}
        payload = {"tickers": tickers, "fields": ["ADR_UNDL_TICKER"]}
        ##r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
        ##obj = json.loads(r.text)

        # 2018-01-01 2018-03-23
    def loadAdrUnderlyingTicker(self,tickers):
        tickerAdrUnderlyingDict = dict()
        sql = 'SELECT Ticker,UnderlyingTicker,UnderlyingType FROM RiskDb.ref.ADRUnderlyingInfo'
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        adrEquityData = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        adrEquityData = adrEquityData[adrEquityData['Ticker'].isin(tickers)]
        allADRTickers = list(adrEquityData['Ticker'].unique())
        for ticker in allADRTickers:
            tickerAdrUnderlyingDict[ticker] = adrEquityData[adrEquityData['Ticker']==ticker]['UnderlyingTicker'].iloc[0]
        return (tickerAdrUnderlyingDict,adrEquityData)

    def getVolFromDict(self, adrVolDict, underlyingTicker, dateStr):
        vol = 0
        currentDay = datetime.datetime.strptime(dateStr, '%Y-%m-%d')
        maxTryTime = 10
        i = 0
        flag = False
        tryDate = currentDay
        while (i < maxTryTime):
            tryDateStr = tryDate.strftime('%Y-%m-%d')
            key = underlyingTicker + '_' + tryDateStr
            if (adrVolDict.has_key(key)):
                flag = True
                vol = float(adrVolDict[key])
                break

            i += 1
            tryDate = tryDate + datetime.timedelta(days=-1)

        if (not flag):
            # print underlyingTicker, ' ', dateStr, ' EQY_FLOAT field did not exist'
            logging.warn(underlyingTicker + ' ' + dateStr + ' EQY_FLOAT field did not exist, will set as 0')

        return vol

        # 2018-01-01 2018-03-23

    def getBbgTickerPXLAST(self, startDayStr, stopDayStr, tickers, adjSplit='false'):
        if tickers:
            mdsUrl = self.bbgUrl + '/mktsvc/histdata'
            headers = {'content-type': 'application/json'}
            payload = {"tickers": tickers, "fields": ["PX_LAST"], "adjSplit": adjSplit,
                   "startDate": startDayStr, "endDate": stopDayStr, "fill": "PREVIOUS_VALUE",
                   "days": "NON_TRADING_WEEKDAYS"}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            records = []
            for ticker, v in obj['data'].items():
                for dateStr, fieldDict in v.items():
                    records.append((ticker, dateStr, fieldDict['PX_LAST']))
            result = pd.DataFrame(records, columns=['Ticker', 'TradeDate', 'PX_LAST'])
            result['PX_LAST'] = result['PX_LAST'].astype(float)
            return pd.DataFrame(records, columns=['Ticker', 'TradeDate', 'PX_LAST'])

    #2018-01-01 2018-03-23
    def getBbgTickerEodMd(self, startDayStr, stopDayStr, tickers):
        (tickerAdrUnderlyingDict, adrEquityData) = self.loadAdrUnderlyingTicker(tickers)
        if not adrEquityData.empty:
            tickers += list(adrEquityData['UnderlyingTicker'].unique())

        mdsUrl = self.bbgUrl + '/mktsvc/histdata'
        headers = {'content-type': 'application/json'}
        payload = {"tickers": tickers, "fields": ["PX_LAST", "EQY_FLOAT"], "adjSplit": "false", "startDate": startDayStr, "endDate": stopDayStr, "fill": "PREVIOUS_VALUE", "days": "NON_TRADING_WEEKDAYS"}
        r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
        obj = json.loads(r.text)
        adrPriceDict = dict()
        adrVolDict = dict()
        records = []
        print obj['data']
        for ticker, v in obj['data'].items():
            for dateStr, fieldDict in v.items():
                if(fieldDict.has_key('PX_LAST') and fieldDict.has_key('EQY_FLOAT')):
                    #print ticker, dateStr, fieldDict['PX_LAST'], fieldDict['EQY_FLOAT']
                    #self.upsertTickerEodPriceToDb(ticker, dateStr, fieldDict['PX_LAST'], fieldDict['EQY_FLOAT'])
                    records.append((ticker, dateStr, fieldDict['PX_LAST'], fieldDict['EQY_FLOAT']))
                elif(fieldDict.has_key('PX_LAST')):
                    key = ticker + '_' + dateStr
                    adrPriceDict[key] = fieldDict['PX_LAST']
                elif(fieldDict.has_key('EQY_FLOAT')):
                    key = ticker + '_' + dateStr
                    adrVolDict[key] = fieldDict['EQY_FLOAT']

        for k, price in adrPriceDict.items():
            ticker = k.split('_')[0]
            dateStr = k.split('_')[1]
            if(tickerAdrUnderlyingDict.has_key(ticker)):
                underlyingTicker = tickerAdrUnderlyingDict[ticker]
                vol = self.getVolFromDict(adrVolDict, underlyingTicker, dateStr)
                #print ticker, dateStr, price, vol
                #self.upsertTickerEodPriceToDb(ticker, dateStr, price, vol)
                records.append((ticker, dateStr, price, vol))
            else:
                #print ticker, ' does not have EQY_FLOAT field'
                logging.error(ticker + '(could be ADR underlying), does not have EQY_FLOAT field')

        if((len(records) + len(adrPriceDict) + 1) < len(tickers)):
            #exit()
            logging.warn('return ticker records less than required tickers, pls check if needed')
            pass
        try:
            for r in records:
                try:
                    sql = 'insert into RiskDb.bench.BbgTickerEodPrice(BbgTicker, TradeDate, ClosePrice, FloatVolume) \
                                      values(?, ?, ?, ?)'
                    self.cursor.execute(sql, r)
                except Exception,e:
                    pass
        except Exception,e:
            return

    def getOverwriteBeta(self,startDayStr, stopDayStr, tickers):
        if tickers:
            mdsUrl = self.bbgUrl + '/mktsvc/refdata'
            headers = {'content-type': 'application/json'}
            payload = {"tickers": tickers, "fields": ["BETA_RAW_OVERRIDABLE"],
                       "overrides": [{"field": "BETA_OVERRIDE_END_DT", "value": stopDayStr},{"field": "BETA_OVERRIDE_START_DT","value": startDayStr},{"field": "BETA_OVERRIDE_PERIOD", "value": "D"}]}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            records = []
            for ticker, v in obj['data'].items():
                if v.has_key('BETA_RAW_OVERRIDABLE'):
                    BETA_RAW_OVERRIDABLE = v['BETA_RAW_OVERRIDABLE']
                    records.append((ticker, stopDayStr, BETA_RAW_OVERRIDABLE))
            return pd.DataFrame(records, columns=['Ticker', 'Date', 'BETA_RAW_OVERRIDABLE'])

    def getOverwriteBetaWithBenchmarkIndex(self,startDayStr, stopDayStr, tickers, benchmark_index):
        if tickers:
            mdsUrl = self.bbgUrl + '/mktsvc/refdata'
            headers = {'content-type': 'application/json'}
            payload = {"tickers": tickers, "fields": ["BETA_RAW_OVERRIDABLE"],
                       "overrides": [{"field": "BETA_OVERRIDE_END_DT", "value": stopDayStr},{"field": "BETA_OVERRIDE_START_DT","value": startDayStr},{"field": "BETA_OVERRIDE_PERIOD", "value": "D"},{"field":"BETA_OVERRIDE_REL_INDEX","value": benchmark_index}]}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            records = []
            for ticker, v in obj['data'].items():
                if v.has_key('BETA_RAW_OVERRIDABLE'):
                    BETA_RAW_OVERRIDABLE = v['BETA_RAW_OVERRIDABLE']
                    records.append((ticker, stopDayStr, BETA_RAW_OVERRIDABLE))
            return pd.DataFrame(records, columns=['Ticker', 'Date', 'BETA_RAW_OVERRIDABLE'])

    def getCnvtBondDelta(self, currentDateStr, tickers, CV_MODEL_CALC_TYP=3):
        if tickers:
            mdsUrl = self.bbgUrl + '/mktsvc/refdata'
            headers = {'content-type': 'application/json'}
            payload = {"tickers": tickers, "fields": ["CV_MODEL_DELTA_S"],
                       "overrides": [{"field": "FLAT_CREDIT_SPREAD_CV_MODEL", "value": "400"},{"field": "CV_MODEL_TYP", "value": "J"},{"field": "CV_MODEL_CALC_TYP","value": CV_MODEL_CALC_TYP},{"field": "CV_MODEL_BORROW_COST","value": "0.5"}]}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            records = []
            for ticker, v in obj['data'].items():
                if v.has_key('CV_MODEL_DELTA_S'):
                    CV_MODEL_DELTA_S = v['CV_MODEL_DELTA_S']
                    records.append((ticker, currentDateStr, CV_MODEL_DELTA_S))
            return pd.DataFrame(records, columns=['Ticker', 'Date', 'CV_MODEL_DELTA_S'])

    def getAndReturnCurrencEodPrice(self,startDayStr, stopDayStr, currencies):
        if currencies:
            mdsUrl = self.bbgUrl + '/mktsvc/histdata'
            headers = {'content-type': 'application/json'}
            payload = {"tickers": currencies, "fields": ["PX_LAST"], "adjSplit": "false",
                       "startDate": startDayStr,
                       "endDate": stopDayStr, "fill": "PREVIOUS_VALUE", "days": "NON_TRADING_WEEKDAYS"}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            records = []
            for ticker, v in obj['data'].items():
                for dateStr, fieldDict in v.items():
                    records.append((ticker, dateStr, fieldDict['PX_LAST']))
            return pd.DataFrame(records, columns=['CurrencyCode', 'TradeDate', 'FX_LAST'])


    def getCurrencEodPrice(self, startDayStr, stopDayStr, currencies):
        if currencies:
            mdsUrl = self.bbgUrl + '/mktsvc/histdata'
            headers = {'content-type': 'application/json'}
            payload = {"tickers": currencies, "fields": ["PX_LAST"], "adjSplit": "false",
                       "startDate": startDayStr,
                       "endDate": stopDayStr, "fill": "PREVIOUS_VALUE", "days": "NON_TRADING_WEEKDAYS"}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            records = []
            for ticker, v in obj['data'].items():
                for dateStr, fieldDict in v.items():
                    records.append((ticker, dateStr, fieldDict['PX_LAST']))
            try:
                for r in records:
                    try:
                        sql = 'insert into RiskDb.bench.CurrencyEodPrice(CurrencyCode,TradeDate,ClosePrice) \
                                                  values(?, ?, ?)'
                        logging.info(r)
                        self.cursor.execute(sql, r)
                    except pyodbc.IntegrityError, e:
                        '''Integrity Error most likely is duplicate record which could be ignored'''
                        logging.warning('getCurrencEodPrice: integrity error while saving record, will ignore: ' + e.message + e.args[1])
                    except Exception, e:
                        logging.error('getCurrencEodPrice: error while saving record: '+':'.join(r)+', ' + e.message)
                        raise Exception('getCurrencEodPrice: error while saving record: '+':'.join(r)+', ' + e.message)
            except Exception, e:
                logging.error('getCurrencEodPrice: error while saving data, ' + e.message)
                raise Exception('getCurrencEodPrice: error while saving data, ' + e.message)
            return records

    def cleanupMarketData(self, startDateStr, stopDateStr,tickers,teamList,calcYear,indexTickers):
        logging.info('cleaning market data - start')
        sql = 'delete from RiskDb.bench.BbgTickerDvdSplitInfo where BbgTicker in (\'' + ('\',\'').join(tickers)+'\') and AdjustmentDate > ?'
        self.cursor.execute(sql, self.inceptionDate)

        sql = 'delete from RiskDb.bench.BbgCashDividentInfo where BbgTicker in (\'' + ('\',\'').join(tickers)+'\') and ExDate > ?'
        self.cursor.execute(sql, self.inceptionDate)

        sql = 'delete from RiskDb.bench.BbgTickerEodPrice where BbgTicker in (\'' + ('\',\'').join(tickers)+'\') and TradeDate between ? and ?'
        self.cursor.execute(sql, (startDateStr, stopDateStr))

        sql = 'delete from RiskDb.bench.CurrencyEodPrice where TradeDate between ? and ?'
        self.cursor.execute(sql, (startDateStr, stopDateStr))

        portfolioCodeList = []
        for team in teamList:
            portfolioCodeList.append(calcYear+team)

        sql = 'delete from RiskDb.bench.BenchmarkPortfolioConstEodPrice where PortfolioCode in (\'' + ('\',\'').join(portfolioCodeList)+'\') and TradeDate between ? and ?'
        self.cursor.execute(sql, (startDateStr, stopDateStr))

        sql = 'delete from RiskDb.bench.BenchmarkPortfolioEodPrice where PortfolioCode in (\'' + ('\',\'').join(portfolioCodeList)+'\') and TradeDate between ? and ?'
        self.cursor.execute(sql, (startDateStr, stopDateStr))

        sql = 'delete from RiskDb.bench.BenchmarkIndexEodPrice where IndexCode in (\'' + ('\',\'').join(portfolioCodeList) + '\') and TradeDate between ? and ?'
        self.cursor.execute(sql, (startDateStr, stopDateStr))

        sql = 'delete from RiskDb.bench.BenchmarkIndexEodPrice where IndexCode in (\'' + ('\',\'').join(indexTickers) + '\') and TradeDate between ? and ?'
        self.cursor.execute(sql, (startDateStr, stopDateStr))

        logging.info('cleaning market data - completed')

    def getIndexMdFromBbg(self, beginDateStr, endDateStr, indexTickers):
        fields = ['PX_LAST', 'CHG_PCT_1D']
        mdsUrl = self.bbgUrl + '/mktsvc/histdata'
        headers = {'content-type': 'application/json'}
        payload = {"tickers": indexTickers, "fields": fields, 'startDate': beginDateStr, 'endDate': endDateStr, 'period': 'DAILY',   'fill': 'PREVIOUS_VALUE', 'days': 'NON_TRADING_WEEKDAYS', 'periodicityAdjustment': 'ACTUAL'}
        r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
        obj = json.loads(r.text)
        data = []
        for k,v in obj['data'].items():
            for dateStr, fieldDict in v.items():
                print k, dateStr, fieldDict['PX_LAST'], fieldDict['CHG_PCT_1D']
                try:
                    data.append([k, dateStr, self.getDictFild(fieldDict, 'PX_LAST'),  self.getDictFild(fieldDict, 'CHG_PCT_1D')])
                except Exception,e:
                    logging.error('getIndexMdFromBbg: '+k + dateStr + ',  data missing....')

        self.upsertDataToBenchMarkEodpriceDb(data)

    def getIndexMdFromBbgNILVALUE(self, beginDateStr, endDateStr, indexTickers):
        fields = ['PX_LAST', 'CHG_PCT_1D']
        mdsUrl = self.bbgUrl + '/mktsvc/histdata'
        headers = {'content-type': 'application/json'}
        payload = {"tickers": indexTickers, "fields": fields, 'startDate': beginDateStr, 'endDate': endDateStr, 'period': 'DAILY',   'fill': 'NIL_VALUE', 'days': 'NON_TRADING_WEEKDAYS', 'periodicityAdjustment': 'ACTUAL'}
        r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
        obj = json.loads(r.text)
        data = []
        for k,v in obj['data'].items():
            for dateStr, fieldDict in v.items():
                print k, dateStr, fieldDict['PX_LAST'], fieldDict['CHG_PCT_1D']
                data.append([k, dateStr, self.getDictFild(fieldDict, 'PX_LAST'),  self.getDictFild(fieldDict, 'CHG_PCT_1D')])


        self.upsertDataToBenchMarkEodpriceDb(data)


    def getDictFild(self, fieldDict, fieldName):
        if(fieldDict.has_key(fieldName)):
            return fieldDict[fieldName]
        else:
            return None

    def upsertDataToBenchMarkEodpriceDb(self, dataList):
        currentTime = datetime.datetime.now()
        for d in dataList:
            try:
                ticker = d[0]
                dateStr = d[1]
                sql = 'if exists (select * from bench.BenchmarkIndexEodPrice where IndexCode=? and TradeDate=?) select 1 else select 0';
                self.cursor.execute(sql, (ticker, dateStr))
                existFlag = False
                for row in self.cursor.fetchall():
                    if (row[0] == 1):
                        existFlag = True
                        break
                if (existFlag):
                    logging.error('upsertDataToBenchMarkEodpriceDb: duplicate records for ticker: ' + ticker+' and date:'+dateStr)
                    #raise Exception('upsertDataToBenchMarkEodpriceDb: duplicate records for ticker: ' + ticker+' and date:'+dateStr)
                else:
                    sql = 'insert into RiskDb.bench.BenchmarkIndexEodPrice (IndexCode, TradeDate, ClosePrice, PctChange, LastUpdatedOn) '
                    sql += 'values(?,?,?,?,?)'
                    self.cursor.execute(sql, (d[0], d[1], self.parFloat(d[2]), self.parFloat(d[3]), currentTime))
            except Exception, e:
                logging.error('upsertDataToBenchMarkEodpriceDb: error when saving '+e.message)
                raise Exception('upsertDataToBenchMarkEodpriceDb: error when saving '+e.message)

    def parFloat(self, para):
        try:
            return float(para)
        except Exception,e:
            return None

    def loadIndexEODPriceFromExcel(self, filePath, indexCode):
        data = pd.read_excel(filePath)
        data['IndexCode'] = indexCode
        data['TradeDate'] = pd.to_datetime(data['TradeDate'], format='%d/%m/%Y')
        records = pdUtil.dataFrameToSavableRecords(data, ['IndexCode','TradeDate', 'Price', 'Pct_change'])
        sql = 'insert into RiskDb.bench.BenchmarkIndexEodPrice(IndexCode, TradeDate, ClosePrice, PctChange) values(?,?,?,?)'
        self.insertToDatabase(sql, records)

    def getAllTickers(self):
        sql = 'SELECT distinct BbgTicker FROM RiskDb.bench.BenchmarkPortfolioConstEodPrice'
        self.cursor.execute(sql)
        columns = [column[0] for column in self.cursor.description]
        allResult = self.cursor.fetchall()
        resultDataFrame = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
        return list(resultDataFrame['BbgTicker'].unique())

    def insertToDatabase(self,sql,data):
        if data:
            self.cursor.executemany(sql, data)
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')


    def getCurrencyInfo(self, tickers):
        if tickers:
            sql = 'select CurrencyId,CurrencyCode from RiskDb.ref.Currency'
            self.cursor.execute(sql)
            columns = [column[0] for column in self.cursor.description]
            allResult = self.cursor.fetchall()
            currencyInfoData = pd.DataFrame((tuple(row) for row in allResult), columns=columns)

            mdsUrl = self.bbgUrl + '/mktsvc/refdata'
            headers = {'content-type': 'application/json'}
            # DVD_HIST_ALL, DVD_HIST get only cash divident
            payload = {"tickers": tickers, "fields": ["CRNCY","DVD_CRNCY"]}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            records = []
            for ticker, v in obj['data'].items():
                stockCurrency = v["CRNCY"]
                stockCurrencyId = currencyInfoData[currencyInfoData['CurrencyCode']==stockCurrency]['CurrencyId'].iloc[0]
                if v.has_key('DVD_CRNCY'):
                    dvdCurrency = v['DVD_CRNCY']
                    dvdCurrencyId = currencyInfoData[currencyInfoData['CurrencyCode']==dvdCurrency]['CurrencyId'].iloc[0]
                    records.append((ticker, int(stockCurrencyId), int(dvdCurrencyId)))
            if records:
                try:
                    sql = 'insert into RiskDb.bench.BbgTickerCurrencyInfo(BbgTicker,TickerCurrencyID,DvdCurrencyID) values(?, ?, ?)'
                    self.cursor.executemany(sql, records)
                except Exception, e:
                    logging.error('getCurrencyInfo: error while saving BbgTickerCurrencyInfo,' + e.message + e.args[1])
                    raise Exception('getCurrencyInfo: error while saving BbgTickerCurrencyInfo,' + e.message + e.args[1])


    def getFXQuoteFactorFromBBG(self, tickers):
        if tickers:
            mdsUrl = self.bbgUrl + '/mktsvc/refdata'
            headers = {'content-type': 'application/json'}
            # DVD_HIST_ALL, DVD_HIST get only cash divident
            payload = {"tickers": tickers, "fields": ["QUOTE_FACTOR"]}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            records = []
            for ticker, v in obj['data'].items():
                quoteFactorValue = int(float(v["QUOTE_FACTOR"]))
                records.append((ticker, quoteFactorValue))
            if records:
                try:
                    sql = 'insert into RiskDb.ref.CurrencyInfo(CurrencyCode,PriceSize) values(?, ?)'
                    self.cursor.executemany(sql, records)
                except Exception, e:
                    logging.error('getFXQuoteFactorFromBBG: error while saving RiskDb.ref.CurrencyInfo,' + e.message + e.args[1])
                    raise Exception(
                        'getFXQuoteFactorFromBBG: error while saving RiskDb.ref.CurrencyInfo,' + e.message + e.args[1])
    def upsertForIPODate(self, dataList):
        for d in dataList:
            try:
                ticker = d[0]
                ipoDateStr = d[1]
                sql = 'if exists (select * from [RiskDb].[risk].[EquityGICSInfo] where BbgTicker=?) select 1 else select 0';
                self.cursor.execute(sql, (ticker))
                existFlag = False
                for row in self.cursor.fetchall():
                    if (row[0] == 1):
                        existFlag = True
                        break

                if (existFlag):
                    sql = 'update RiskDb.risk.EquityGICSInfo set IPODate=? where [BbgTicker]=?'
                    self.cursor.execute(sql, (ipoDateStr, ticker))
                else:
                    sql = 'insert into RiskDb.risk.EquityGICSInfo (IPODate, BbgTicker) '
                    sql += 'values(?,?)'
                    self.cursor.execute(sql, (ipoDateStr, ticker))

            except Exception, e:
                logging.error('upsertForIPODate: error when saving '+e.message)
                raise Exception('upsertForIPODate: error when saving '+e.message)


    def getIPODateFromBBG(self, tickers):
        if tickers:
            mdsUrl = self.bbgUrl + '/mktsvc/refdata'
            headers = {'content-type': 'application/json'}
            # DVD_HIST_ALL, DVD_HIST get only cash divident
            payload = {"tickers": tickers, "fields": ["EQY_INIT_PO_DT","EQY_RAW_BETA","BETA_RAW_OVERRIDABLE","EQY_RAW_BETA_6M"]}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            ##r.status_code==200
            obj = json.loads(r.text)
            records = []
            return_tickers =[]
            if r.status_code==200:
                for ticker, v in obj['data'].items():
                    return_tickers.append(ticker)
                    quoteFactorValue = ''
                    if 'EQY_INIT_PO_DT' in v:
                        quoteFactorValue = str(v["EQY_INIT_PO_DT"])
                    records.append((ticker, quoteFactorValue))
                empty_tickers = [ticker for ticker in tickers if ticker not in return_tickers]
                for emtpy_ticker in empty_tickers:
                    records.append((emtpy_ticker,''))
                if records:
                    self.upsertForIPODate(records)
                    return 'SUCCESS'
            else:
                logging.error('error: getIPODateFromBBG status code='+str(r.status_code))
                return 'FAILED'


    def getRefData(self, tickers, fields):
        if tickers:
            mdsUrl = self.bbgUrl + '/mktsvc/refdata'
            headers = {'content-type': 'application/json'}
            payload = {"tickers": tickers, "fields": fields}
            r = requests.post(mdsUrl, data=json.dumps(payload), headers=headers)
            obj = json.loads(r.text)
            print r.text
            #obj = json.loads('{"securityErrors":[],"fieldErrors":[],"data":{"HSI 11/28/19 P27200 Index":{"DELTA":"0.2986448","GAMMA":"0.1042393","OPT_STRIKE_PX":"27200.0","OPT_THETA":"-7.180377","VEGA":"22.29833"},"HSI 11/28/19 P26400 Index":{"DELTA":"0.2986448","GAMMA":"0.1042393","OPT_STRIKE_PX":"26400.0","OPT_THETA":"-7.180377","VEGA":"22.29833"},"HSCEI 11/28/19 P10300 Index":{"DELTA":"0.2986448","GAMMA":"0.1042393","OPT_STRIKE_PX":"10300.0","OPT_THETA":"-7.180377","VEGA":"22.29833"},"HSCEI 11/28/19 P10200 Index":{"DELTA":"0.2986448","GAMMA":"0.1042393","OPT_STRIKE_PX":"10200.0","OPT_THETA":"-7.180377","VEGA":"22.29833"},"HSI 12/30/19 C27000 Index":{"DELTA":"0.2986448","GAMMA":"0.1042393","OPT_STRIKE_PX":"27000.0","OPT_THETA":"-7.180377","VEGA":"22.29833"},"510050 CH 01/22/20 C3 Equity":{"DELTA":"0.3274173","GAMMA":"0.08505802","OPT_STRIKE_PX":"3.0","OPT_THETA":"-4.566237E-4","VEGA":"0.003515989"},"VIX US 12/18/19 C19 Index":{"DELTA":"0.2119745","GAMMA":"0.009706772","OPT_STRIKE_PX":"19.0","OPT_THETA":"-0.05283381","VEGA":"0.007076697"},"HSCEI 12/30/19 P10000 Index":{"DELTA":"-0.1470086","GAMMA":"0.05775833","OPT_STRIKE_PX":"10000.0","OPT_THETA":"-2.210167","VEGA":"5.822755"},"HSCEI 12/30/19 C10600 Index":{"DELTA":"0.3156711","GAMMA":"0.09751275","OPT_STRIKE_PX":"10600.0","OPT_THETA":"-3.154226","VEGA":"8.989227"},"HSI 12/30/19 P25200 Index":{"DELTA":"-0.1018687","GAMMA":"0.04381842","OPT_STRIKE_PX":"25200.0","OPT_THETA":"-4.407614","VEGA":"11.45293"},"NKY 12/13/19 P22000 Index":{"DELTA":"-0.01289742","GAMMA":"0.01164028","OPT_STRIKE_PX":"22000.0","OPT_THETA":"-2.016626","VEGA":"0.8275488"},"HSCEI 12/30/19 C10500 Index":{"DELTA":"0.4116287","GAMMA":"0.1081342","OPT_STRIKE_PX":"10500.0","OPT_THETA":"-3.412742","VEGA":"9.835249"},"NKY 12/13/19 P21750 Index":{"DELTA":"-0.00468539","GAMMA":"0.004736907","OPT_STRIKE_PX":"21750.0","OPT_THETA":"-0.7487606","VEGA":"0.3444764"},"VIX US 12/18/19 C21 Index":{"DELTA":"0.1472237","GAMMA":"0.007350388","OPT_STRIKE_PX":"21.0","OPT_THETA":"-0.04420981","VEGA":"0.006031368"},"VIX US 12/18/19 P12.5 Index":{"DELTA":"-0.1886362","GAMMA":"0.01473516","OPT_STRIKE_PX":"12.5","OPT_THETA":"-0.03044228","VEGA":"0.007079541"},"HSCEI 12/30/19 P9600 Index":{"DELTA":"-0.04301505","GAMMA":"0.01886603","OPT_STRIKE_PX":"9600.0","OPT_THETA":"-1.043653","VEGA":"2.313884"},"VIX US 12/18/19 C20 Index":{"DELTA":"0.1859895","GAMMA":"0.008447028","OPT_STRIKE_PX":"20.0","OPT_THETA":"-0.05146343","VEGA":"0.006226831"},"HSCEI 12/30/19 P10200 Index":{"DELTA":"-0.2819693","GAMMA":"0.09096617","OPT_STRIKE_PX":"10200.0","OPT_THETA":"-3.050911","VEGA":"8.540749"},"HSCEI 12/30/19 C10700 Index":{"DELTA":"0.231788","GAMMA":"0.08423298","OPT_STRIKE_PX":"10700.0","OPT_THETA":"-2.678163","VEGA":"7.711843"},"HSCEI 12/30/19 P9800 Index":{"DELTA":"-0.07782144","GAMMA":"0.03380859","OPT_STRIKE_PX":"9800.0","OPT_THETA":"-1.49878","VEGA":"3.678178"},"VIX US 12/18/19 C17 Index":{"DELTA":"0.3069347","GAMMA":"0.01338194","OPT_STRIKE_PX":"17.0","OPT_THETA":"-0.05642583","VEGA":"0.009004425"},"HSCEI 12/30/19 P10400 Index":{"DELTA":"-0.4782302","GAMMA":"0.1103278","OPT_STRIKE_PX":"10400.0","OPT_THETA":"-3.50913","VEGA":"10.06911"},"HSI 12/30/19 P25000 Index":{"DELTA":"-0.07683895","GAMMA":"0.03443195","OPT_STRIKE_PX":"25000.0","OPT_THETA":"-3.679751","VEGA":"9.269071"},"NKY 12/13/19 P22250 Index":{"DELTA":"-0.02203231","GAMMA":"0.02033244","OPT_STRIKE_PX":"22250.0","OPT_THETA":"-3.092712","VEGA":"1.3149"}}}')
            if r.status_code==200:
            #if True:
                data = pd.DataFrame.from_dict(obj['data'])
                ready_data = data.transpose()
                ready_data['Identifier(Agg)'] = ready_data.index
                return ready_data
            else:
                logging.error('error: getRefData status code='+str(r.status_code))
                return None

    def cleanupTickerMarketData(self, startDateStr, stopDateStr,tickers):
        logging.info('cleaning market data - start')
        sql = 'delete from RiskDb.bench.BbgTickerDvdSplitInfo where BbgTicker in (\'' + ('\',\'').join(tickers)+'\') and AdjustmentDate > ?'
        self.cursor.execute(sql, self.inceptionDate)

        sql = 'delete from RiskDb.bench.BbgCashDividentInfo where BbgTicker in (\'' + ('\',\'').join(tickers)+'\') and ExDate > ?'
        self.cursor.execute(sql, self.inceptionDate)

        sql = 'delete from RiskDb.bench.BbgTickerEodPrice where BbgTicker in (\'' + ('\',\'').join(tickers)+'\') and TradeDate between ? and ?'
        self.cursor.execute(sql, (startDateStr, stopDateStr))

        logging.info('cleaning market data - completed')

if __name__ == '__main__':
    env = 'prod'
    mdService = MarketDataDownloader(env)
    mdService.initSqlServer(mdService.env)
    try:
        logging.info('market data downloader')
        #tickers = ['T US Equity']
        #mdService.getCurrencyInfo()
        #mdService.updateConversionRatio()

        #mdService.getIndexMdFromBbg('2019-03-01','2019-09-05',['BBSW1M Index','HIHD01M Index','JY0001M Index','US0001M Index'])
        #filePath='C:\\Shared\\index\\'+indexCode+'.xlsx'
        #mdService.loadIndexEODPriceFromExcel(filePath,indexCode)
        #startdateStr = '2019-03-01'
        #dateStr = '2019-07-28'
        #tickers=['LTI IN Equity','LVS US Equity','MA US Equity','MAHB MK Equity','MAKRO TB Equity','MAXIS MK Equity','MGM US Equity','MM IN Equity','MMFS IN Equity','MRCO IN Equity','MRF IN Equity','MSFT US Equity','MSIL IN Equity','MSS IN Equity','MUTH IN Equity','NEST IN Equity','NESZ MK Equity','NIACL IN Equity','NTES US Equity','OFSS IN Equity','PAG IN Equity','PDD US Equity','PG IN Equity','PIDI IN Equity','PIEL IN Equity','PLNG IN Equity','PNB IN Equity','POWF IN Equity','PYPL US Equity','RBK IN Equity','RDY US Equity','RECL IN Equity','SBILIFE IN Equity','SBIN IN Equity','SHTF IN Equity','SKB IN Equity','SRCM IN Equity','ST SP Equity','SUNP IN Equity','SUNTV IN Equity','T US Equity','TAL US Equity','TCS IN Equity','TECHM IN Equity','THBEV SP Equity','TLKM IJ Equity','TME US Equity','TOPG MK Equity','TRUE TB Equity','TTAN IN Equity','TTMT IN Equity','TVSL IN Equity','TXN US Equity','UBBL IN Equity','UNSP IN Equity','UNVR IJ Equity','UPLL IN Equity','URC PM Equity','UTCEM IN Equity','V US Equity','VIPS US Equity','VNM VN Equity','VZ US Equity','WB US Equity','WIL SP Equity','WIT US Equity','WMT US Equity','WYNN US Equity','YES IN Equity','Z IN Equity','ZTO US Equity']
        #mdService.getEquityDvdSplitInfoFromBbg(tickers)

        tickers = ['KC US Equity','GDS US Equity','VNET US Equity']
        #mdService.cleanupTickerMarketData('2019-12-31', '2020-03-08', tickers)
        #mdService.getEquityDvdSplitInfoFromBbg(tickers)
        #mdService.getStockCashDividentEventsFromBbg(tickers)
        #mdService.getBbgTickerEodMd('2020-01-01', '2020-09-09', tickers)
        #mdService.addStockDividentDayClosePrice(startdateStr, dateStr)
        #mdService.getIndexMdFromBbg(dateStr,dateStr,indexTickers)
        #mdService.getCurrencEodPrice('2020-09-09', '2020-09-09', [ 'CHFUSD Curncy'])

        indexTickers = ['AS51 Index','MXAPJ Index','NIFTY Index','TWSE Index']
        mdService.getIndexMdFromBbg('2020-01-01', '2020-09-11', indexTickers)
        #mdService.cleanupTickerMarketData('2019-12-31','2020-01-17',tickers)
        #mdService.checkTickerDvdCcyInfo(tickers)
        #mdService.getBbgTickerEodMd('2019-12-01','2019-12-30',tickers)
        #mdService.getOptionVolume(['HSCEI 06/29/20 P11000 Index','NKY 03/13/20 C24500 Index','NKY 03/13/20 P18000 Index','NKY 03/13/20 P18500 Index','NKY 03/13/20 P22250 Index','NKY 03/13/20 P22750 Index','NKY 03/13/20 P23500 Index','NKY 04/10/20 C23000 Index','NKY 04/10/20 P21500 Index','NKY 06/12/20 P22000 Index','NKY 06/12/20 P22500 Index','SPX US 03/20/20 P2500 Index','SPX US 03/20/20 P3250 Index','HSCEI 03/30/20 C11100 Index','HSCEI 06/29/20 C11000 Index','SPX US 04/17/20 P3300 Index','VIX US 03/18/20 P17 Index','VIX US 03/18/20 P20 Index','VIX US 04/15/20 P16 Index','HSCEI 03/30/20 P9800 Index','KOSPI2 03/12/20 C320 Index','KOSPI2 06/11/20 C300 Index','KOSPI2 06/11/20 P300 Index','NKY 03/13/20 C23000 Index','NKY 03/13/20 P21500 Index','NKY 03/13/20 P22000 Index','NKY 03/13/20 P23375 Index','KOSPI2 03/12/20 P287.5 Index','KOSPI2 03/12/20 P300 Index','NKY 03/13/20 P21000 Index','NKY 03/13/20 P23750 Index','NKY 04/10/20 P22000 Index','SPX US 03/20/20 P3125 Index','SPX US 04/17/20 P2950 Index','SPX US 06/19/20 P2900 Index','VIX US 03/18/20 C18 Index','VIX US 04/15/20 P20 Index','VIX US 05/20/20 P15 Index','VIX US 05/20/20 P16 Index','NKY 04/10/20 C23500 Index','NKY 04/10/20 P21000 Index','NKY 04/10/20 P22250 Index','NKY 06/12/20 C24750 Index','NKY 06/12/20 P21000 Index','VIX US 04/15/20 P15 Index','VIX US 04/15/20 P18 Index','HSCEI 03/30/20 C11500 Index','HSCEI 03/30/20 P11000 Index','HSCEI 03/30/20 P9900 Index','KOSPI2 03/12/20 C300 Index','NKY 03/13/20 C24000 Index','NKY 03/13/20 C25000 Index','NKY 03/13/20 P20000 Index','NKY 03/13/20 P23000 Index','NKY 04/10/20 C24250 Index','NKY 04/10/20 P20000 Index','NKY 04/10/20 P20750 Index','NKY 06/12/20 P19000 Index','NKY 06/12/20 P20000 Index','SPX US 03/20/20 P2875 Index','SPX US 03/20/20 P2900 Index','SPX US 04/17/20 P3150 Index','VIX US 03/18/20 P14 Index','VIX US 03/18/20 P15 Index','VIX US 05/20/20 P18 Index','HSCEI 06/29/20 C11200 Index','HSCEI 03/30/20 P10700 Index'],'2020-01-20','2020-02-27')
        #mdService.getOptionVolume(['HSCEI 03/30/20 C11100 Index','HSCEI 03/30/20 P10000 Index','HSCEI 06/29/20 C11000 Index','HSCEI 06/29/20 P11000 Index','KOSPI2 03/12/20 P265 Index','KOSPI2 06/11/20 C300 Index','KOSPI2 06/11/20 P300 Index','NKY 03/13/20 C23000 Index','NKY 03/13/20 C24500 Index','NKY 03/13/20 P18000 Index','NKY 03/13/20 P18500 Index','NKY 03/13/20 P20500 Index','NKY 03/13/20 P21500 Index','NKY 03/13/20 P22000 Index','NKY 03/13/20 P22250 Index','NKY 03/13/20 P22750 Index','NKY 03/13/20 P23375 Index','NKY 03/13/20 P23500 Index','NKY 04/10/20 C23000 Index','NKY 04/10/20 C23500 Index','NKY 04/10/20 P21000 Index','NKY 04/10/20 P21125 Index','NKY 04/10/20 P21500 Index','NKY 04/10/20 P22250 Index','NKY 06/12/20 C24750 Index','NKY 06/12/20 P21000 Index','NKY 06/12/20 P22000 Index','NKY 06/12/20 P22500 Index','SPX US 03/20/20 P2500 Index','SPX US 03/20/20 P2900 Index','SPX US 03/20/20 P3250 Index','SPX US 04/17/20 P2950 Index','SPX US 04/17/20 P3300 Index','VIX US 03/18/20 P17 Index','VIX US 03/18/20 P20 Index','VIX US 04/15/20 P15 Index','VIX US 04/15/20 P16 Index','VIX US 04/15/20 P18 Index','HSCEI 03/30/20 C11500 Index','HSCEI 03/30/20 P11000 Index','HSCEI 03/30/20 P9900 Index','KOSPI2 03/12/20 C300 Index','KOSPI2 03/12/20 C320 Index','KOSPI2 03/12/20 P255 Index','KOSPI2 03/12/20 P287.5 Index','KOSPI2 03/12/20 P300 Index','NKY 03/13/20 C24000 Index','NKY 03/13/20 C25000 Index','NKY 03/13/20 P20000 Index','NKY 03/13/20 P21000 Index','NKY 03/13/20 P23000 Index','NKY 03/13/20 P23750 Index','NKY 04/10/20 C22000 Index','NKY 04/10/20 C24250 Index','NKY 04/10/20 P20000 Index','NKY 04/10/20 P20750 Index','NKY 04/10/20 P22000 Index','NKY 06/12/20 P19000 Index','NKY 06/12/20 P20000 Index','SPX US 03/20/20 P2875 Index','SPX US 03/20/20 P3125 Index','SPX US 04/17/20 P2700 Index','SPX US 04/17/20 P3150 Index','SPX US 06/19/20 P2900 Index','VIX US 03/18/20 C18 Index','VIX US 03/18/20 P14 Index','VIX US 03/18/20 P15 Index','VIX US 04/15/20 P20 Index','VIX US 05/20/20 P15 Index','VIX US 05/20/20 P16 Index','VIX US 05/20/20 P18 Index'],'2020-01-20','2020-02-27')
        # mdService.getEquityDvdSplitInfoFromBbg(tickers)
        # mdService.getStockCashDividentEventsFromBbg(tickers)
        #mdService.getEquityDvdSplitInfoFromBbg(tickers)
        #mdService.getStockCashDividentEventsFromBbg(tickers)
        #currencies = ['AUDUSD Curncy']
        #mdService.getCurrencEodPrice('2019-04-05','2019-04-07', currencies)



    except Exception, e:
        raise Exception('marketDataDownloader error, ' + e.message)
    finally:
        mdService.closeSqlServerConnection()
