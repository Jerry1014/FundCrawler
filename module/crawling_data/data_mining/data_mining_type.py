"""
数据清洗类型
"""
from enum import Enum
from string import Template


class PageType(Enum):
    """
    枚举
    """
    OVERVIEW: Template('http://fundf10.eastmoney.com/jbgk_%fund_code.html')
