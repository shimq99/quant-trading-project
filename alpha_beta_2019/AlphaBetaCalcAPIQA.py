# encoding:UTF-8
import sys
sys.path.append("C:\\devel\\pinpoint\\prs\\prs-scripts")
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

    refreshMarketData = False
    calcNoBenchmarkTeams = False
    print teamSet
    print fundSet
    print dateStr
else:
    logging.warning('fundBookCode args is empty')