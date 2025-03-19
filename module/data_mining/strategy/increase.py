import re
from string import Template
from typing import NoReturn

from module.data_mining.strategy.data_mining_strategy_factory import DataCleaningStrategy
from module.downloader.download_by_requests import FundResponse
from module.fund_context import FundContext
from utils.constants import number_in_eng, NO_DATA


class RiseStrategy(DataCleaningStrategy):
    """
    解析基金的基本概况
    """
    url_template = Template('https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jdzf&code=$fund_code')

    fund_3_years_increase_pattern = re.compile(fr'近3年[\s\S]*?({number_in_eng}%|---)')
    fund_5_years_increase_pattern = re.compile(fr'近5年[\s\S]*?({number_in_eng}%|---)')

    def build_url(self, context: FundContext) -> str:
        return self.url_template.substitute(fund_code=context.fund_code)

    def fill_result(self, fund_response: FundResponse, context: FundContext) -> NoReturn:
        page_text = fund_response.response.text

        fund_3_years_increase = self.fund_3_years_increase_pattern.search(page_text)
        if fund_3_years_increase:
            increase = fund_3_years_increase.group(1)
            increase = increase if increase != '---' else NO_DATA
            context.three_years_increase = increase

        fund_5_years_increase = self.fund_5_years_increase_pattern.search(page_text)
        if fund_5_years_increase:
            increase = fund_5_years_increase.group(1)
            increase = increase if increase != '---' else NO_DATA
            context.five_years_increase = increase

