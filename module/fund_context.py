"""
模块间交互所使用的BO
"""
from typing import Optional

from module.downloader.download_by_requests import FundResponse
from utils.constants import FundAttrKey, PageType


class FundContext:
    """
    基金爬取数据的上下文
    """

    def __init__(self, fund_code: str, fund_name: str):
        self.fund_code: str = fund_code
        self.fund_name: str = fund_name
        # 晨星的基金标识
        self.morningstar_fund_id: Optional[str] = None
        self.fund_type: Optional[str] = None
        self.fund_size: Optional[str] = None
        self.fund_company: Optional[str] = None
        self.fund_value: Optional[str] = None
        self.fund_manager: Optional[str] = None
        self.date_of_appointment: Optional[str] = None
        self.management_fee_rate: Optional[str] = None
        self.custody_fee_rate: Optional[str] = None
        self.sales_service_fee_rate: Optional[str] = None
        self.annualized_return_five_year: Optional[str] = None
        self.annualized_return_ten_year: Optional[str] = None
        self.standard_deviation_five_years: Optional[str] = None
        self.standard_deviation_ten_years: Optional[str] = None
        self.sharp_rate_five_years: Optional[str] = None
        self.sharp_rate_ten_years: Optional[str] = None
        self.alpha_to_ind: Optional[str] = None
        self.beta_to_ind: Optional[str] = None
        self.r_squared_to_ind: Optional[str] = None

        # 爬取到的网页数据
        self.http_response_dict: dict[PageType, FundResponse] = dict()

    def to_result_row(self) -> dict[FundAttrKey, Optional[str]]:
        return {
            FundAttrKey.FUND_CODE: self.fund_code,
            FundAttrKey.FUND_SIMPLE_NAME: self.fund_name,
            FundAttrKey.MORNINGSTAR_FUND_ID: self.morningstar_fund_id,
            FundAttrKey.FUND_TYPE: self.fund_type,
            FundAttrKey.FUND_SIZE: self.fund_size,
            FundAttrKey.FUND_COMPANY: self.fund_company,
            FundAttrKey.FUND_VALUE: self.fund_value,
            FundAttrKey.FUND_MANAGER: self.fund_manager,
            FundAttrKey.DATE_OF_APPOINTMENT: self.date_of_appointment,
            FundAttrKey.MANAGEMENT_FEE_RATE: self.management_fee_rate,
            FundAttrKey.CUSTODY_FEE_RATE: self.custody_fee_rate,
            FundAttrKey.SALES_SERVICE_FEE_RATE: self.sales_service_fee_rate,
            FundAttrKey.ANNUALIZED_RETURN_FIVE_YEAR: self.annualized_return_five_year,
            FundAttrKey.ANNUALIZED_RETURN_TEN_YEAR: self.annualized_return_ten_year,
            FundAttrKey.STANDARD_DEVIATION_FIVE_YEARS: self.standard_deviation_five_years,
            FundAttrKey.STANDARD_DEVIATION_TEN_YEARS: self.standard_deviation_ten_years,
            FundAttrKey.SHARP_RATE_FIVE_YEARS: self.sharp_rate_five_years,
            FundAttrKey.SHARP_RATE_TEN_YEARS: self.sharp_rate_ten_years,
            FundAttrKey.ALPHA_TO_IND: self.alpha_to_ind,
            FundAttrKey.BETA_TO_IND: self.beta_to_ind,
            FundAttrKey.R_SQUARED_TO_IND: self.r_squared_to_ind,
        }
