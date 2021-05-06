# encoding:UTF-8
from enum import Enum

class PctChangeType(Enum):
    Day=1
    Month=2
    Year=3

class AlphaBetaType(Enum):
    TEAM_WITH_STOCK_BENCHMARK = 0
    FUND_WITH_INDEX_BENCHMARK = 1
    TEAM_WITH_INDEX_BENCHMARK = 2
    TEAM_WITHOUT_BENCHMARK = 3


class TickerMarketStatus(Enum):
    ACTV = 0   #Active
    DLST = 1   #Delisted
    ACQU = 2   #Acquired
    UNLS = 3   #Unlisted
    SUSP = 4   #Suspended
    HALT = 5   #Halted
    PRIV = 6   #Private Company
    EXPD = 7   #Expired
    POST = 8   #Postponed
    PEND = 9   #Pending Listing
    TKCH = 10  #Ticker Change
    LIQU = 11  #Liquidated
    INAC = 12  #Inactive
    WHIS = 13  #When Issued
    PRNA = 14  #Price Not Available
    INFO = 15  #Addistional Info on DES
    RCNA = 16  #Reason not available

class PortfolioType(Enum):
    BOOK = 0   ##Benchmark是stock
    FUND = 1
    BOOK_BENCHMAR_WITH_INDEX = 2  ##Benchmark是index

class BbgTickerEodPriceValueType(Enum):
    USD_CURRENCY = 0
    LOCAL_CURRENCY = 1

class RiskControlHoldingLimitInfoType(Enum):
    SINGLE_BOOK = 0
    SINGLE_FUND = 1
    FUND_IN_FUND = 2  ###针对FUND:ACF全部再投进FUND：PMSF

class RiskLimitType(Enum):
    DEFAULT_LIMIT = 0
    TEMPORARY_LIMIT = 1   ##临时limit,有固定日期内生效

class RiskLimitStatus(Enum):
    APPROVED = 0   ##默认状态，如有修改，才会有PENDING_APPROVAL
    PENDING_APPROVAL = 1   ##待审批
    REJECTED = 2   ##approver拒绝此修改


class RiskControlStatus(Enum):
    PASS = 0   #达标
    NEED_RISK_CONTROL = 1   #需风控
    UNKNOWN = 2  #未定状态
    NO_APPLICABLE = 6  #不适用风控
    NEED_REPORT = 7   #use for Liquidity
    WAIVED = 8   #use for Liquidity

class RiskControlNoStatus(Enum):
    RISK_CONTROL_FIRST_TIME = 1  # 1次风控
    RISK_CONTROL_SEC_TIME = 2  # 2次风控
    RISK_CONTROL_THIRD_TIME = 3  # 3次风控

class RiskControlGrossMarginLimitType(Enum):
    HARD_LIMIT = 'HARD_LIMIT'   #hard limit with number
    DYNAMIC_LIMIT = 'DYNAMIC_LIMIT'   #需动态计算

class RiskCommonReportType(Enum):
    SPECIFIC_TEAM_REPORT = 1  ##特定team的report
    CURRENCY_EXPOSURE_REPORT = 2  ##Currency Exposure Report
    TOP_BOTTOM_TEN_EQUITY = 3  ##Currency Exposure Report
    #BOTTOM_TEN_EQUITY = 4  ##Currency Exposure Report
    QDII_UNIT_GROSS=5
    PMSF_CVF_LEVERAGE=6
    DAILY_SECURITY_PCT_CHANGE_IN_BOOK=7
    DAILY_SECURITY_HOLDING_IN_BOOK=8
    DAILY_SECURITY_PX_PCT_CHANGE=9
    MONTHLY_SECURITY_PX_PCT_CHANGE =10


class RiskFundStatReportPositionType(Enum):
    TICKER_SUM_MV = 0  ##TICKER 所有MV之和
    TICKER_MV_PER_FUNDBOOK = 1  ##TICKER 在不同fund book下的MV

class RiskFundStatReportType(Enum):
    TOP_MARKET_VALUE_STOCKS = 0  ##TopMarketValueStocks
    LONG_SHORT_STOCKS=1 ##LongShortStocks

class RiskStopLossControlRecordStatus(Enum):
    ACTIVE = 0
    INACTIVE=1

class RiskStopLossControlRecordType(Enum):
    DEFAULT = 0

class FactorRiskBetType(Enum):
    TOP10=0
    BOTTOM10=1

class RiskExceptionSummaryReport(Enum):
    NOT_IN_GICS_LIST = 1
    NOT_IN_COUNTRY_LIST = 2
    GLOBAL_RESTRICTED = 3
    INDEX_NOT_ALLOW_TO_TRADE=4
    MAXDD_ALERT=5
    YTD_ALERT = 6
    MTD_ALERT = 7
    TOPBOTTOM_TEN_ALERT=8
    QDII_UNITGROSS_ALERT=9
    IPO_BETA_OVERWRITE=10

class RiskExceptionSummaryReportStatus(Enum):
    FAILED = 0
    PASSED = 1
    RESOLVED = 2

class ShortList(Enum):
    NORMAL = 0
    AXE_LIST = 1

class RiskIndexTradableLimitTeamType(Enum):
    FUND=0
    TEAM=1
    ALL_TEAM_MATCH=2  #team匹配， 如所有W team的Limit, 则Team=W, 类型是ALL_TEAM_MATCH


class EquityMaxReturnReportType(Enum):
    FUND=0
    TEAM=1
    ALL_TEAM_MATCH=2  #team匹配， 如所有W team的Limit, 则Team=W, 类型是ALL_TEAM_MATCH
