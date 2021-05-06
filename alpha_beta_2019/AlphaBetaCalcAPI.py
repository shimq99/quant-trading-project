# encoding:UTF-8
import sys
import datetime
sys.path.append("C:\\Deployments\\prs-scripts")
from benchmark.alpha_beta_2019.AlphaBetaCalc import *

####勿改动，给第三方调用使用
env = 'prod'
alphaBetaCalc = AlphaBetaCalc(env)
if len(sys.argv) < 3:
    logging.error('parameter should contains date and fundbookCode')
    raise Exception('parameter should contains date and fundbookCode')

dateStr = sys.argv[1]
fundBookCodeArg = sys.argv[2]
if not dateStr:
    logging.error('date str can not be empty')
    raise Exception('date str can not be empty')
    exit(1)

currentDate = datetime.datetime.now()
weekDay = currentDate.date().weekday()
currentDateStr = currentDate.strftime('%Y-%m-%d')

if dateStr==currentDateStr:
    print 'ignore for re-running as given date is today'
    exit(0)

teamList = []
fundList = []
if fundBookCodeArg != 'NULL':
    fundBookList = fundBookCodeArg.split('_')
    teamSet = set()
    fundSet = set()
    for fundBook in fundBookList:
        fundCode = fundBook.split('-')[0]
        bookCode = fundBook.split('-')[1]
        teamSet.add(bookCode)
        fundSet.add(fundCode)

    calcNoBenchmarkTeams = True
    currentDate = datetime.datetime.now()
    currentHour = currentDate.hour
    currentMin = currentDate.minute

    weekDay = currentDate.date().weekday()
    diff = 1
    if (weekDay == 6):
        diff += 1
    elif (weekDay == 0):
        diff += 2
    runYesterDay = currentDate - datetime.timedelta(days=diff)
    runYesterDayStr = runYesterDay.strftime('%Y-%m-%d')
    runYesterDay = datetime.datetime.strptime(runYesterDayStr, '%Y-%m-%d')
    givenDate = datetime.datetime.strptime(dateStr, '%Y-%m-%d')

    if givenDate == runYesterDay:
        if (currentHour >= 10) & (currentMin >8 ):
            ###每天上午跑的任务在10点触发，因此更新任务在10点前不生效
            print 'success invoke alpha beta'
            alphaBetaCalc.initSqlServer(env)
            alphaBetaCalc.calcAlphaBeta(dateStr, list(teamSet), [], calcNoBenchmarkTeams=False, reRun=True, reRunFundList=list(fundSet))
            alphaBetaCalc.closeSqlServerConnection()
        else:
            print 'current time is before 10:08'
    else:
        ###T-2天以前的历史数据，不受限制可以重跑
        print 'success invoke alpha beta for historical data'
        alphaBetaCalc.initSqlServer(env)
        alphaBetaCalc.calcAlphaBeta(dateStr, list(teamSet), [], calcNoBenchmarkTeams=False, reRun=True, reRunFundList=list(fundSet))
        alphaBetaCalc.closeSqlServerConnection()
else:
    logging.warning('fundBookCode args is empty')