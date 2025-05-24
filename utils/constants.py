"""
一些通用的正则表达式
"""
from enum import unique, StrEnum, Enum, auto

# 带千分号的 数字表达形式 -10,000.12
number_in_eng = r'-?(\d+?(,\d+)*?(\.\d+)?)'


@unique
class FundAttrKey(StrEnum):
    """
    基金属性枚举
    """
    FUND_CODE = '基金代码',
    FUND_SIMPLE_NAME = '基金简称',
    MORNINGSTAR_FUND_ID = '(晨星)基金代码',
    FUND_TYPE = '基金类型',
    FUND_SIZE = '资产规模(亿)',
    FUND_COMPANY = '基金管理人',
    FUND_VALUE = '基金净值',
    # 兼容带新场景，A+B -> B -> B+C，此时基金经理为时长最长的B，对应的任职时间为 这三段 B连续任职的任职时间
    FUND_MANAGER = '基金经理(最近连续最长任职)',
    DATE_OF_APPOINTMENT = '基金经理的上任时间',

    STANDARD_DEVIATION_THREE_YEARS = '近三年标准差',
    SHARPE_THREE_YEARS = '近三年夏普',

    MANAGEMENT_FEE_RATE = '管理费率(每年)',
    CUSTODY_FEE_RATE = '托管费率(每年)',
    SALES_SERVICE_FEE_RATE = '销售服务费率(每年)',

    FIVE_YEAR_ANNUALIZED_RETURN = "五年回报（年化）",
    TEN_YEAR_ANNUALIZED_RETURN = "十年回报（年化）",


@unique
class PageType(Enum):
    """
    页面的爬取和解析 枚举
    """
    # 基金概况 https://fundf10.eastmoney.com/jbgk_910009.html
    OVERVIEW = auto()
    # 基金经理 https://fundf10.eastmoney.com/jjjl_910009.html
    MANAGER = auto()
    # 特色数据 https://fundf10.eastmoney.com/tsdata_910009.html
    METRICS = auto()
    # 晨星基本信息 https://www.morningstar.cn/handler/fundsearch.ashx
    MORNINGSTAR = auto()
    # 基金回报 https://www.morningstar.cn/handler/quicktake.ashx?command=return&fcid=0P00019IIB
    RETURN = auto()


# 最终爬取结果文件的占位，用于区分是真的木有数据，还是爬取可能失败/遗漏
NO_DATA = 'NO_DATA'
DATA_ERROR = 'DATA_ERROR'

# 日志输出格式
log_format = '%(asctime)s [%(processName)s/%(levelname)s] %(message)s'
