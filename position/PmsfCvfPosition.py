#encoding:UTF-8
import pyodbc
import logging
import xlwt
import datetime

class PmsfCvfPosition(object):

    def __init__(self):
        self.positionList = []
        self.pctStyle = xlwt.XFStyle()
        self.pctStyle.num_format_str = '0.00%' 
        self.teamList = ('T09', 'T10', 'T11', 'T13', 'T14', 'T20', 'T23', 'T28', 'T30')
        self.teamAumDict = dict()
        self.countryList = ('China', 'Hong Kong', 'United States', 'Japan', 'Taiwan', 'Others')
        self.drSecurityMap = dict()
        self.securityIdBetaDict = dict()

    def initSqlServer(self):
        server = '192.168.200.22\prod' 
        database = 'Portfolio' 
        username = 'Dev' 
        password = 'Dev@123' 
        self.conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        self.cursor = self.conn.cursor()        

    def initLogger(self):
        logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='log.log',
                filemode='w')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)-8s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)

    def queryFundTeamAum(self, asOfDateStr):
        try:
            sql = "select Fund, Book, Aum from Portfolio.perf.NavView where  Fund in ('PMSF', 'CVF') and Date=?"
            self.cursor.execute(sql, asOfDateStr)
            records = self.cursor.fetchall()
            for r in records:
                try:
                    key = r[0] + '-' + r[1]
                    self.teamAumDict[key] = float(r[2])
                except Exception, e:
                    pass

        except Exception,e:
            print e

    def calculateCountryMv(self, teamPosition):
        chinaMv = 0.0
        hkMv = 0.0
        usMv = 0.0
        japanMv = 0.0
        taiwanMv = 0.0
        othersMv = 0.0
        for k, v in teamPosition.items():
            if(v[4] == self.countryList[0]):
                chinaMv += v[2]
            elif(v[4] == self.countryList[1]):
                hkMv += v[2]
            elif(v[4] == self.countryList[2]):
                usMv += v[2]
            elif(v[4] == self.countryList[3]):
                japanMv += v[2]
            elif(v[4] == self.countryList[4]):
                taiwanMv += v[2]
            else:
                othersMv += v[2]

        return (chinaMv, hkMv, usMv, japanMv, taiwanMv, othersMv)



    def runSingleTeam(self, asOfDateStr, team, positions):
        (pmsfTeamPosition, cvfTeamPosition) = self.getTeamPositions(asOfDateStr, team, positions)
        return self.calculateStockPct(pmsfTeamPosition, cvfTeamPosition, team, asOfDateStr)
        '''
        pmsfMv = self.calculateCountryMv(pmsfTeamPosition)
        cvfMv = self.calculateCountryMv(cvfTeamPosition)
        i = 0
        pmsfTeam = 'PMSF-' + team
        cvfTeam = 'CVF-' + team 
        for mv in pmsfMv:
            pmsfPct = pmsfMv[i]/self.teamAumDict[pmsfTeam]
            cvfPct = cvfMv[i]/self.teamAumDict[cvfTeam]
            diff = pmsfPct - cvfPct
            if(abs(pmsfPct-cvfPct) >= 0.01):
                value = (team, self.countryList[i], pmsfPct, cvfPct, diff)
                self.spamwriter.writerow(value)
            i += 1
        '''


    def cauculateTeamStockPct(self, teamPosition, aum, ticker, ulyTicker):
        pct = 0.0
        if(teamPosition.has_key(ticker)):
            pct = teamPosition[ticker][2]/aum
        else:
            for p in teamPosition:
                if(ulyTicker == p[3]):
                    pct = teamPosition[ticker][2]/aum
                    break
        return pct


    def calculateStockPct(self, pmsfTeamPosition, cvfTeamPosition, team, asOfDateStr):
        pmsfTeam = 'PMSF-' + team
        cvfTeam = 'CVF-' + team          

        stockDiffDict = dict()
        records = []
        for position in (pmsfTeamPosition, cvfTeamPosition):
            for k, v in position.items():
                pmsfPct = float(self.cauculateTeamStockPct(pmsfTeamPosition, self.teamAumDict[pmsfTeam], k, v[3]))
                cvfPct = float(self.cauculateTeamStockPct(cvfTeamPosition, self.teamAumDict[cvfTeam], k, v[3]))
                if(abs(pmsfPct-cvfPct) >= 0.01):                
                    if(not stockDiffDict.has_key(k)):
                        diff = pmsfPct - cvfPct
                        value = (asOfDateStr, team, k, pmsfPct, cvfPct, diff)
                        stockDiffDict[k] = value
                        records.append(value)
                        #self.spamwriter.writerow(value)

        return records

    def getTeamPositions(self, asOfDateStr, team, positions):
        try:
            pmsfTeam = 'PMSF-' + team
            cvfTeam = 'CVF-' + team
            pmsfTeamPosition = dict()
            cvfTeamPosition = dict()
            for r in positions:
                fund = r[0]
                book = r[0] + '-' + r[1]
                if(team in book):
                    externalAssetClass = r[6]
                    if(externalAssetClass == 'EQTY'):
                        #mv = float(r[5]) * float(self.securityIdBetaDict[int(r[12])])
                        mv = float(r[5])
                        ulyTicker = r[8]
                        externalInstrumentClass = r[7]
                        if(externalInstrumentClass == 'DR'):
                            ulyTicker = r[10]
                            self.drSecurityMap[ulyTicker] = r[8]
                        market_ctry = r[11]

                        if('Equity' not in ulyTicker):
                            continue

                        if(fund == 'PMSF'):
                            tickerDict = pmsfTeamPosition
                        elif(fund == 'CVF'):
                            tickerDict = cvfTeamPosition

                        if(tickerDict.has_key(ulyTicker)):
                            tickerDict[ulyTicker][2] += mv
                        else:
                            tickerDict[ulyTicker] = [fund, book, mv, ulyTicker, market_ctry]

            return (pmsfTeamPosition, cvfTeamPosition)
        except Exception,e:
            print e

    def closeSqlServerConnection(self):
        self.conn.commit()
        self.conn.close()

    def querySecurityBeta(self, asOfDateStr):
        sql = '''
              select ca.SecurityID, (COALESCE(
                            ca.BETA,
                            CASE WHEN (ca.BETA_OVERRIDABLE > 3.9) THEN 3.9
                            WHEN ca.BETA_OVERRIDABLE < -2.5 THEN -2.5
                            ELSE ca.BETA_OVERRIDABLE
                            END,
                            ca.BETA_6_MONTH)
                    +
                    COALESCE(
                            ca.BETA_6_MONTH,
                            ca.BETA,
                            CASE WHEN ca.BETA_OVERRIDABLE > 3.9 THEN 3.9
                            WHEN ca.BETA_OVERRIDABLE < -2.5 THEN -2.5
                            ELSE ca.BETA_OVERRIDABLE
                            END)
                ) * 0.5 AS Beta from [MarketData].[mark].[ufn_latest_analytics] (?, 5) ca
              '''
        self.cursor.execute(sql, asOfDateStr)
        rowList = self.cursor.fetchall()
        for row in rowList:
            self.securityIdBetaDict[row[0]] = row[1]

    def queryPositions(self, asOfDateStr):
        sql = "select A.Fund, A.Book, A.Security, A.QuantityDirection, A.QuantityEnd, A.NotnlMktValBook, B.ExternalAssetClass, B.ExternalInstClass, B.BB_TCM, B.CFD_BB_TCM, B.UnderlyingBB_TCM , B.ExchangeCountryDesc, B.SecurityId \
               from portfolio.pos.AggregatedPositionView A left join SecurityMaster.sm.SecurityView B on A.SecurityId=B.SecurityId \
               where [Date]=? and quantitydirection in ('LONG' ,'SHORT') and Fund in ('PMSF', 'CVF') and positiontypecode = 'TRD' and ExternalAssetClass='EQTY' order by Fund, Book"
        self.cursor.execute(sql, asOfDateStr)
        return self.cursor.fetchall()

    def saveBalancingInfo(self,allRecords):
        sql = 'insert into RiskDb.risk.RiskBalancingReport(AsOfDate,BookCode,StockOrCountry,PMSFPct,CVFPct,Diff) values(?,?,?,?,?,?)'
        self.cursor.executemany(sql, allRecords)

    def removeBalancingInfo(self, dateStr):
        sql ='delete from RiskDb.risk.RiskBalancingReport where AsOfDate=\''+dateStr+'\''
        self.cursor.execute(sql)

    def run(self):
        self.initSqlServer()
        self.initLogger()
        currentDate = datetime.datetime.now()
        weekDay = currentDate.date().weekday()
        diff = 1
        if (weekDay == 5 or weekDay == 6):
            return
        elif (weekDay == 0):
            diff += 2
        runYesterDay = currentDate - datetime.timedelta(days=diff)
        asOfDateStr = runYesterDay.strftime('%Y%m%d')
        print asOfDateStr     
        if(asOfDateStr == '') :
            return
        self.queryFundTeamAum(asOfDateStr)
        self.querySecurityBeta(asOfDateStr)
        positions = self.queryPositions(asOfDateStr)
        allRecords = []
        for team in self.teamList:
            try:
                teamRecords = self.runSingleTeam(asOfDateStr, team, positions)
                allRecords += teamRecords
            except Exception,e:
                pass

        if allRecords:
            self.removeBalancingInfo(asOfDateStr)
            self.saveBalancingInfo(allRecords)
        self.closeSqlServerConnection()

if __name__ == '__main__':
    p = PmsfCvfPosition()
    p.run()