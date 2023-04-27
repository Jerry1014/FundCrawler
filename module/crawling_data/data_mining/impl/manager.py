import re
from string import Template
from typing import NoReturn

from requests import Response

from module.crawling_data.data_mining.data_cleaning_strategy import DataCleaningStrategy
from process_manager import FundCrawlingResult


class ManagerDataCleaningStrategy(DataCleaningStrategy):
    """
    解析基金经理信息
    """
    url_template = Template('http://fundf10.eastmoney.com/jjjl_$fund_code.html')

    fund_manager_name_pattern = re.compile(r'现任基金经理简介[\s\S]+?姓名：[\s\S]+?<a.+?>([\s\S]+?)</a>')
    fund_manager_date_of_appointment_pattern = re.compile(r'现任基金经理简介[\s\S]+?上任日期：[\s\S]+?>(.+?)</p>')

    def build_url(self, fund_code: str) -> str:
        return self.url_template.substitute(fund_code=fund_code)

    def fill_result(self, response: Response, result: FundCrawlingResult) -> NoReturn:
        page_text = response.text

        fund_manager_name = self.fund_manager_name_pattern.search(page_text)
        if fund_manager_name:
            result.fund_info_dict[FundCrawlingResult.Header.FUND_MANAGER] = fund_manager_name.group(1)
        fund_date_of_appointment = self.fund_manager_date_of_appointment_pattern.search(page_text)
        if fund_date_of_appointment:
            result.fund_info_dict[FundCrawlingResult.Header.DATE_OF_APPOINTMENT] = fund_date_of_appointment.group(1)
