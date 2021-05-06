# encoding:UTF-8
import sys
import datetime

sys.path.append("C:\\Deployments\\prs-scripts")
from benchmark.risk_control_2019.RiskControlReports import *

####勿改动，给第三方调用使用
env = 'prod'
riskControlReports = RiskControlReports(env)
currentDate = datetime.datetime.now()
currentHour = currentDate.hour
if currentHour >= 11:
    ###每天上午跑的任务在10点触发，因此更新任务在10点前不生效
    print 'success invoke riskControlReports'
    riskControlReports.run()
