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
    FUND_TYPE = '基金类型',
    FUND_SIZE = '资产规模(亿)',
    FUND_COMPANY = '基金管理人',
    FUND_VALUE = '基金净值',
    # 兼容带新场景，A+B -> B -> B+C，此时基金经理为时长最长的B，对应的任职时间为 这三段 B连续任职的任职时间
    FUND_MANAGER = '基金经理(最近连续最长任职)',
    DATE_OF_APPOINTMENT = '基金经理的上任时间',
    STANDARD_DEVIATION_THREE_YEARS = '近三年标准差',
    SHARPE_THREE_YEARS = '近三年夏普',
    THREE_YEARS_INCREASE = '近三年涨幅',
    FIVE_YEARS_INCREASE = '近五年涨幅'


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
    # 阶段涨幅 https://fundf10.eastmoney.com/jdzf_006624.html
    INCREASE = auto()
