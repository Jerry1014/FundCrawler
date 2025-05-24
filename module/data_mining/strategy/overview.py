import re
from string import Template
from typing import Optional

from module.data_mining.strategy.data_mining_strategy_factory import DataCleaningStrategy
from module.downloader.download_by_requests import FundResponse
from module.fund_context import FundContext
from utils.constants import number_in_eng, NO_DATA, DATA_IGNORE


class OverviewStrategy(DataCleaningStrategy):
    """
    解析基金的基本概况
    """
    url_template = Template('http://fundf10.eastmoney.com/jbgk_$fund_code.html')

    fund_type_pattern = re.compile(r'基金类型</th><td>(.*?)</td></tr><tr><th>发行日期')
    fund_size_pattern = re.compile(fr'资产规模</th><td>(---)|({number_in_eng})亿元')
    fund_company_pattern = re.compile(r'基金管理人</th><td><a.*?">(.+?)</a></td><th>基金托管人')
    fund_value_pattern = re.compile(fr'单位净值.*?：[\s\S]*?({number_in_eng})\s')

    management_fee_rate_pattern = re.compile(fr'管理费率</th><td>(({number_in_eng})%|---|<a)')
    custody_fee_rate_pattern = re.compile(fr'托管费率</th><td>(({number_in_eng})%|---)')
    sales_service_fee_rate_pattern = re.compile(fr'销售服务费率</th><td>(({number_in_eng})%|---)')

    def build_url(self, context: FundContext) -> Optional[str]:
        return self.url_template.substitute(fund_code=context.fund_code)

    def fill_result(self, fund_response: FundResponse, context: FundContext) -> None:
        response = fund_response.response
        if response is None:
            return

        page_text = response.text

        fund_kind_result = self.fund_type_pattern.search(page_text)
        if fund_kind_result:
            fund_type = fund_kind_result.group(1)
            # 不知道为什么，这个基金真的没有基金类型 023713
            context.fund_type = NO_DATA if not fund_type and context.fund_code == '023713' else fund_type
        fund_size_result = self.fund_size_pattern.search(page_text)
        if fund_size_result:
            # 1,179.10 亿元
            fund_size = fund_size_result.group(1) if fund_size_result.group(1) \
                else fund_size_result.group(2).replace(',', '')
            context.fund_size = fund_size if fund_size != '---' else NO_DATA
        fund_company_result = self.fund_company_pattern.search(page_text)
        if fund_company_result:
            context.fund_company = fund_company_result.group(1)
        fund_value_result = self.fund_value_pattern.search(page_text)
        if fund_value_result:
            context.fund_value = fund_value_result.group(1)

        management_fee_rate_result = self.management_fee_rate_pattern.search(page_text)
        if management_fee_rate_result:
            fee_rate = management_fee_rate_result.group(1)
            if fee_rate == '<a':
                # 特殊逻辑，部分费用过于复杂，直接是一个跳转链接
                context.management_fee_rate = DATA_IGNORE
            else:
                context.management_fee_rate = fee_rate if fee_rate != '---' else NO_DATA

        custody_fee_rate_result = self.custody_fee_rate_pattern.search(page_text)
        if custody_fee_rate_result:
            context.custody_fee_rate = custody_fee_rate_result.group(1) \
                if custody_fee_rate_result.group(1) != '---' else NO_DATA

        sales_service_fee_rate_result = self.sales_service_fee_rate_pattern.search(page_text)
        if sales_service_fee_rate_result:
            context.sales_service_fee_rate = sales_service_fee_rate_result.group(1) \
                if sales_service_fee_rate_result.group(1) != '---' else NO_DATA
