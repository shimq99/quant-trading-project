# encoding:UTF-8
from benchmark.base.Base import Base
from benchmark.log.LogManager import LogManager
import pandas as pd
import logging
import numpy as np
from benchmark.risk_control_2019.RiksControlReportPMSFT22 import *
from decimal import *
getcontext().prec = 6

'''
Currency Exposure Calculator
1. Equity
   (1) stock: exposure = MV
   (2) swap:  - InstClass=Cash  -> check underlying  currency & settlement currency
              - InstClass=NONTL(Notional/Margin)    ignore
2. Bond
3. Option
4. Future
5. Convertible Bond
6. Bond Future
7. Bond Swap
8. FX Option
9: IRS
10: CDS
'''
class RiskCurrencyExposure(Base):
    def __init__(self, env):
        self.env = env
        LogManager('RiskCurrencyExposure')

if __name__ == '__main__':
    env = 'prod'
    riskCurrencyExposure = RiskCurrencyExposure(env)


