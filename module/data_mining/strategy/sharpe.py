import re
from string import Template
from typing import NoReturn

from module.data_mining.strategy.data_mining_strategy_factory import DataCleaningStrategy
from module.downloader.download_by_requests_v2 import ResponseV2
from module.fund_context import FundContext


class MetricsStrategy(DataCleaningStrategy):
    """
    解析标准差和夏普比率
    """
    url_template = Template('http://fundf10.eastmoney.com/tsdata_$fund_code.html')

    fund_standard_deviation_pattern = re.compile(r'标准差.+?\'>(.+?)<.+?\'>(.+?)<.+?\'>(.+?)<')
    fund_sharpe_ratio_pattern = re.compile(r'夏普比率.+?\'>(.+?)<.+?\'>(.+?)<.+?\'>(.+?)<')

    def build_url(self, context: FundContext) -> str:
        return self.url_template.substitute(fund_code=context.fund_code)

    def fill_result(self, response: ResponseV2, result: FundContext) -> NoReturn:
        page_text = response.response.text

        fund_standard_deviation = self.fund_standard_deviation_pattern.search(page_text)
        if fund_standard_deviation:
            standard_deviation = fund_standard_deviation.group(3)
            # -- 代表无此数据
            standard_deviation = None if standard_deviation == '--' else standard_deviation
            result.standard_deviation_three_years = standard_deviation
        fund_sharpe_ratio = self.fund_sharpe_ratio_pattern.search(page_text)
        if fund_sharpe_ratio:
            sharpe = fund_sharpe_ratio.group(3)
            # -- 代表无此数据
            sharpe = None if sharpe == '--' else sharpe
            result.sharpe_three_years = sharpe
