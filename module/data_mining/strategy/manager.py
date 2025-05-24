import re
from string import Template
from typing import Optional

from module.data_mining.strategy.data_mining_strategy_factory import DataCleaningStrategy
from module.downloader.download_by_requests import FundResponse
from module.fund_context import FundContext


class ManagerStrategy(DataCleaningStrategy):
    """
    解析基金经理信息
    """
    url_template = Template('http://fundf10.eastmoney.com/jjjl_$fund_code.html')

    fund_manager_name_pattern = re.compile(r'现任基金经理简介[\s\S]+?姓名：[\s\S]+?<a.+?>(.+?)</a>')
    fund_manager_date_of_appointment_pattern = re.compile(r'现任基金经理简介[\s\S]+?上任日期：[\s\S]+?>(.+?)</p>')

    def build_url(self, context: FundContext) -> Optional[str]:
        return self.url_template.substitute(fund_code=context.fund_code)

    def fill_result(self, fund_response: FundResponse, context: FundContext) -> None:
        response = fund_response.response
        if response is None:
            return

        page_text = response.text

        fund_manager_name = self.fund_manager_name_pattern.search(page_text)
        if fund_manager_name:
            context.fund_manager = fund_manager_name.group(1)
        fund_date_of_appointment = self.fund_manager_date_of_appointment_pattern.search(page_text)
        if fund_date_of_appointment:
            context.date_of_appointment = fund_date_of_appointment.group(1)
