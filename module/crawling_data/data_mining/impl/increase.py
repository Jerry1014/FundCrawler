import re
from string import Template
from typing import NoReturn

from requests import Response

from module.crawling_data.data_mining.data_cleaning_strategy_factory import DataCleaningStrategy
from module.crawling_data.data_mining.impl.constants import number_in_eng
from process_manager import FundCrawlingResult


class RiseStrategy(DataCleaningStrategy):
    """
    解析基金的基本概况
    """
    url_template = Template('https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jdzf&code=$fund_code')

    fund_3_years_increase_pattern = re.compile(fr'近3年[\s\S]*?({number_in_eng}%|---)')
    fund_5_years_increase_pattern = re.compile(fr'近5年[\s\S]*?({number_in_eng}%|---)')

    def build_url(self, fund_code: str) -> str:
        return self.url_template.substitute(fund_code=fund_code)

    def fill_result(self, response: Response, result: FundCrawlingResult) -> NoReturn:
        page_text = response.text

        fund_3_years_increase = self.fund_3_years_increase_pattern.search(page_text)
        if fund_3_years_increase:
            increase = fund_3_years_increase.group(1)
            increase = increase if increase != '---' else None
            result.fund_info_dict[FundCrawlingResult.Header.THREE_YEARS_INCREASE] = increase

        fund_5_years_increase = self.fund_5_years_increase_pattern.search(page_text)
        if fund_5_years_increase:
            increase = fund_5_years_increase.group(1)
            increase = increase if increase != '---' else None
            result.fund_info_dict[FundCrawlingResult.Header.FIVE_YEARS_INCREASE] = increase
