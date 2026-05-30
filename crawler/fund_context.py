"""基金数据载体"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class FundContext:
    """基金爬取数据的上下文"""
    fund_code: str
    fund_name: str
    morningstar_fund_id: Optional[str] = None
    fund_type: Optional[str] = None
    fund_size: Optional[str] = None
    fund_company: Optional[str] = None
    fund_value: Optional[str] = None
    fund_manager: Optional[str] = None
    date_of_appointment: Optional[str] = None
    management_fee_rate: Optional[str] = None
    custody_fee_rate: Optional[str] = None
    sales_service_fee_rate: Optional[str] = None
    annualized_return_five_year: Optional[str] = None
    annualized_return_ten_year: Optional[str] = None
    standard_deviation_five_years: Optional[str] = None
    standard_deviation_ten_years: Optional[str] = None
    sharp_rate_five_years: Optional[str] = None
    sharp_rate_ten_years: Optional[str] = None
    alpha_to_ind: Optional[str] = None
    beta_to_ind: Optional[str] = None
    r_squared_to_ind: Optional[str] = None
