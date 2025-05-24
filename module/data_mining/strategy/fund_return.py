import json
from string import Template
from typing import Optional

from module.data_mining.strategy.data_mining_strategy import DataCleaningStrategy, NoNeedException
from module.downloader.download_by_requests import FundResponse
from module.fund_context import FundContext
from utils.constants import NO_DATA


class ReturnStrategy(DataCleaningStrategy):
    """
    基金历史回报
    """

    url_template = Template(
        'https://www.morningstar.cn/handler/quicktake.ashx?command=return&fcid=$morningstar_fund_id')

    def build_url(self, context: FundContext) -> Optional[str]:
        # 晨星有特殊的基金标识，需要先爬取到这个标识才能继续爬取
        if not context.morningstar_fund_id:
            return None
        if context.morningstar_fund_id == NO_DATA:
            context.annualized_return_five_year = NO_DATA
            context.annualized_return_ten_year = NO_DATA
            raise NoNeedException()
        return self.url_template.substitute(morningstar_fund_id=context.morningstar_fund_id)

    def fill_result(self, fund_response: FundResponse, context: FundContext) -> None:
        response = fund_response.response
        if response is None:
            return

        return_json_list = json.loads(response.text)['CurrentReturn']['Return']
        for fund_return in return_json_list:
            if fund_return['Name'] == '五年回报（年化）':
                context.annualized_return_five_year = fund_return['Return'] if fund_return['Return'] else NO_DATA
                continue
            if fund_return['Name'] == '十年回报（年化）':
                context.annualized_return_ten_year = fund_return['Return'] if fund_return['Return'] else NO_DATA
                continue
