"""
数据清洗类型
"""
from enum import Enum, auto


class PageType(Enum):
    """
    页面的爬取和解析 枚举
    """
    # 基金概况 https://fundf10.eastmoney.com/jbgk_910009.html
    OVERVIEW = auto()
    # 基金经理 https://fundf10.eastmoney.com/jjjl_910009.html
    MANAGER = auto
    # 特色数据 https://fundf10.eastmoney.com/tsdata_910009.html
    METRICS = auto()
