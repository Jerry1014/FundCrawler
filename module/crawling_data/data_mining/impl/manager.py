from string import Template
from typing import NoReturn

from module.crawling_data.data_mining.data_cleaning_strategy import DataCleaningStrategy
from process_manager import FundCrawlingResult


class ManagerDataCleaningStrategy(DataCleaningStrategy):
    """
    数据解析 适用网站
    http://fundf10.eastmoney.com/jjjl_007696.html
    """
    url_template = Template('http://fundf10.eastmoney.com/jjjl_$fund_code.html')

    def build_url(self, fund_code: str) -> str:
        return self.url_template.substitute(fund_code=fund_code)

    def fill_result(self, response, result: FundCrawlingResult) -> NoReturn:
        pass
