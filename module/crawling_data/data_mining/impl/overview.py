import re
from string import Template
from typing import NoReturn

from requests import Response

from module.crawling_data.data_mining.data_cleaning_strategy_factory import DataCleaningStrategy
from process_manager import FundCrawlingResult


class OverviewDataCleaningStrategy(DataCleaningStrategy):
    """
    数据解析 适用网站
    http://fundf10.eastmoney.com/jbgk_007696.html
    """
    url_template = Template('http://fundf10.eastmoney.com/jbgk_$fund_code.html')

    fund_type_pattern = re.compile(r'基金类型</th><td>(.+?)</td>')
    fund_size_pattern = re.compile(r'资产规模</th><td>(\d+(\.\d+))?\D')
    fund_company_pattern = re.compile(r'基金管理人</th><td><a.*?">(.+?)</a></td>')

    def build_url(self, fund_code: str) -> str:
        return self.url_template.substitute(fund_code=fund_code)

    def fill_result(self, response: Response, result: FundCrawlingResult) -> NoReturn:
        page_text = response.text

        fund_kind_result = self.fund_type_pattern.search(page_text)
        if fund_kind_result:
            result.fund_info_dict[FundCrawlingResult.Header.FUND_TYPE] = fund_kind_result.group(1)
        fund_size_result = self.fund_size_pattern.search(page_text)
        if fund_size_result:
            result.fund_info_dict[FundCrawlingResult.Header.FUND_SIZE] = fund_size_result.group(1)
        fund_company_result = self.fund_company_pattern.search(page_text)
        if fund_company_result:
            result.fund_info_dict[FundCrawlingResult.Header.FUND_COMPANY] = fund_company_result.group(1)
