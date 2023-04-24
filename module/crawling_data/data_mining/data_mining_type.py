from enum import Enum
from string import Template


class PageType(Enum):
    OVERVIEW: Template('http://fundf10.eastmoney.com/jbgk_%fund_code.html')
