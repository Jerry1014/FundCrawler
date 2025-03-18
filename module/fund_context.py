"""
模块间交互所使用的BO
"""
from module.downloader.download_by_requests import FundResponse
from utils.constants import FundAttrKey, PageType


class FundContext:
    """
    基金爬取数据的上下文
    """

    def __init__(self, fund_code: str, fund_name: str):
        self.fund_code: str = fund_code
        self.fund_name: str = fund_name
        self.fund_type = None
        self.fund_size = None
        self.fund_company = None
        self.fund_value = None
        self.fund_manager = None
        self.date_of_appointment = None
        self.standard_deviation_three_years = None
        self.sharpe_three_years = None
        self.three_years_increase = None
        self.five_years_increase = None

        # 爬取到的网页数据
        self.http_response_dict: dict[PageType, FundResponse] = dict()

    def to_result_row(self):
        return {
            FundAttrKey.FUND_CODE: self.fund_code,
            FundAttrKey.FUND_SIMPLE_NAME: self.fund_name,
            FundAttrKey.FUND_TYPE: self.fund_type,
            FundAttrKey.FUND_SIZE: self.fund_size,
            FundAttrKey.FUND_COMPANY: self.fund_company,
            FundAttrKey.FUND_VALUE: self.fund_value,
            FundAttrKey.FUND_MANAGER: self.fund_manager,
            FundAttrKey.DATE_OF_APPOINTMENT: self.date_of_appointment,
            FundAttrKey.STANDARD_DEVIATION_THREE_YEARS: self.standard_deviation_three_years,
            FundAttrKey.SHARPE_THREE_YEARS: self.sharpe_three_years,
            FundAttrKey.THREE_YEARS_INCREASE: self.three_years_increase,
            FundAttrKey.FIVE_YEARS_INCREASE: self.five_years_increase,
        }
