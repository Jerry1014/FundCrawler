"""
模块间交互所使用的BO
"""
from enum import unique, StrEnum


# todo 统一一个类
class NeedCrawledOnceFund:
    """
    需要爬取的 单个基金信息
    """

    def __init__(self, code: str, name: str):
        self.code = code
        self.name = name


class FundCrawlingResult:
    """
    基金的最终爬取结果定义
    """

    @unique
    class Header(StrEnum):
        """
        结果key
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

    def __init__(self, fund_code: str, fund_name: str):
        self.fund_info_dict = {FundCrawlingResult.Header.FUND_CODE: fund_code,
                               FundCrawlingResult.Header.FUND_SIMPLE_NAME: fund_name}
