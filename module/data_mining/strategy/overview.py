import re
from string import Template
from typing import NoReturn

from module.data_mining.strategy.data_mining_strategy_factory import DataCleaningStrategy
from module.downloader.download_by_requests_v2 import ResponseV2
from module.fund_context import FundContext
from utils.constants import number_in_eng


class OverviewStrategy(DataCleaningStrategy):
    """
    解析基金的基本概况
    """
    url_template = Template('http://fundf10.eastmoney.com/jbgk_$fund_code.html')

    fund_type_pattern = re.compile(r'基金类型</th><td>(.+?)</td></tr><tr><th>发行日期')
    fund_size_pattern = re.compile(fr'资产规模</th><td>({number_in_eng})亿元')
    fund_company_pattern = re.compile(r'基金管理人</th><td><a.*?">(.+?)</a></td><th>基金托管人')
    fund_value_pattern = re.compile(fr'单位净值.*?：[\s\S]*?({number_in_eng})\s')

    def build_url(self, context: FundContext) -> str:
        return self.url_template.substitute(fund_code=context.fund_code)

    def fill_result(self, response: ResponseV2, result: FundContext) -> NoReturn:
        page_text = response.response.text

        fund_kind_result = self.fund_type_pattern.search(page_text)
        if fund_kind_result:
            result.fund_type = fund_kind_result.group(1)
        fund_size_result = self.fund_size_pattern.search(page_text)
        if fund_size_result:
            # 1,179.10 亿元
            fund_size = fund_size_result.group(1).replace(',', '')
            result.fund_size = fund_size
        fund_company_result = self.fund_company_pattern.search(page_text)
        if fund_company_result:
            result.fund_company = fund_company_result.group(1)
        fund_value_result = self.fund_value_pattern.search(page_text)
        if fund_value_result:
            result.fund_value = fund_value_result.group(1)
