import re
from string import Template
from typing import NoReturn

from module.crawling_data.data_mining.data_cleaning_strategy import DataCleaningStrategy
from process_manager import FundCrawlingResult


class MetricsDataCleaningStrategy(DataCleaningStrategy):
    """
    解析标准差和夏普比率
    """
    url_template = Template('http://fundf10.eastmoney.com/tsdata_$fund_code.html')

    fund_standard_deviation_pattern = re.compile(r'标准差.+?\'>(.+?)<.+?\'>(.+?)<.+?\'>(.+?)<')
    fund_sharpe_ratio_pattern = re.compile(r'夏普比率.+?\'>(.+?)<.+?\'>(.+?)<.+?\'>(.+?)<')

    def build_url(self, fund_code: str) -> str:
        return self.url_template.substitute(fund_code=fund_code)

    def fill_result(self, response, result: FundCrawlingResult) -> NoReturn:
        page_text = response.text

        fund_standard_deviation = self.fund_standard_deviation_pattern.search(page_text)
        if fund_standard_deviation:
            result.fund_info_dict[FundCrawlingResult.Header.STANDARD_DEVIATION_THREE_YEARS] = \
                fund_standard_deviation.group(3)
        fund_sharpe_ratio = self.fund_sharpe_ratio_pattern.search(page_text)
        if fund_sharpe_ratio:
            result.fund_info_dict[FundCrawlingResult.Header.SHARPE_THREE_YEARS] = fund_sharpe_ratio.group(3)
