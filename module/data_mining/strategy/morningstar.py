import json
from string import Template
from typing import Optional

from module.data_mining.strategy.data_mining_strategy import DataCleaningStrategy
from module.downloader.download_by_requests import FundResponse
from module.fund_context import FundContext
from utils.constants import NO_DATA


class MorningstarStrategy(DataCleaningStrategy):
    """
    晨星基本信息
    """
    url_template = Template('https://www.morningstar.cn/handler/fundsearch.ashx?q=$fund_code&limit=1')

    def build_url(self, context: FundContext) -> Optional[str]:
        return self.url_template.substitute(fund_code=context.fund_code)

    def fill_result(self, fund_response: FundResponse, context: FundContext) -> None:
        response = fund_response.response
        if response is None:
            return

        return_json = json.loads(response.text)
        if return_json:
            context.morningstar_fund_id = return_json[0]['FundClassId'] if return_json[0]['FundClassId'] else NO_DATA
        else:
            context.morningstar_fund_id = NO_DATA
