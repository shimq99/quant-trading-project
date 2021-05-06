# encoding:UTF-8
import sys
import datetime
sys.path.append("C:\\Script\\navloader")
print 'add sys path '
import matlab.engine

class RiskEngine2API():
    def runRiskEngine2(self):
        # startDateStr = '2019-01-01'
        # stopDateStr = '2019-04-10'
        # startDate = datetime.datetime.strptime(startDateStr, '%Y-%m-%d')
        # stopDate = datetime.datetime.strptime(stopDateStr, '%Y-%m-%d')
        # while (startDate <= stopDate):
        #     if (startDate.weekday() >= 0 and startDate.weekday() <= 4):
        #         dateStr = startDate.strftime('%Y-%m-%d')
        #         print  'run date=' + dateStr
        #         ####勿改动，给第三方调用使用
        #         eng = matlab.engine.start_matlab()
        #         eng.runRiskEngine2(dateStr, nargout=0)
        #     startDate = startDate + datetime.timedelta(days=1)

        print 'import all libs'
        currentDate = datetime.datetime.now()
        weekDay = currentDate.date().weekday()
        diff = 1
        if (weekDay == 5 or weekDay == 6):
            # if (weekDay == 6):  ### ad-hoc change , as ren fei needs the data at Sat 03/22
            exit(0)
        elif (weekDay == 0):
            diff += 2
        runYesterDay = currentDate - datetime.timedelta(days=diff)
        runYesterDayStr = runYesterDay.strftime('%Y%m%d')
        ####勿改动，给第三方调用使用
        eng = matlab.engine.start_matlab()
        eng.runRiskEngine2(runYesterDayStr, nargout=0)
        print runYesterDayStr
        print 'test'


if __name__ == '__main__':
    ####勿提交改动，
    riskEngine2API = RiskEngine2API()
    riskEngine2API.runRiskEngine2()
