from string import Template
from typing import NoReturn

from module.crawling_data.data_mining.data_cleaning_strategy_factory import DataCleaningStrategy


class OverviewDataCleaningStrategy(DataCleaningStrategy):
    url_template = Template('http://fundf10.eastmoney.com/jbgk_$fund_code.html')

    def build_url(self, fund_code: str) -> str:
        return self.url_template.substitute(fund_code=fund_code)

    def fill_result(self, response, result) -> NoReturn:
        print(f'爬取结果 url:{response.url}')