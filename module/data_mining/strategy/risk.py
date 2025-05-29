import json
from string import Template
from typing import Optional

from module.data_mining.strategy.data_mining_strategy import DataCleaningStrategy, NoNeedException
from module.downloader.download_by_requests import FundResponse
from module.fund_context import FundContext
from utils.constants import NO_DATA


class RiskStrategy(DataCleaningStrategy):
    """
    基金的风险指标
    """

    url_template = Template(
        'https://www.morningstar.cn/handler/quicktake.ashx?command=rating&fcid=$morningstar_fund_id')

    def build_url(self, context: FundContext) -> Optional[str]:
        # 晨星有特殊的基金标识，需要先爬取到这个标识才能继续爬取
        if not context.morningstar_fund_id:
            return None
        if context.morningstar_fund_id == NO_DATA:
            context.standard_deviation_five_years = NO_DATA
            context.standard_deviation_ten_years = NO_DATA
            context.sharp_rate_five_years = NO_DATA
            context.sharp_rate_ten_years = NO_DATA
            context.alpha_to_ind = NO_DATA
            context.beta_to_ind = NO_DATA
            context.r_squared_to_ind = NO_DATA
            raise NoNeedException()
        return self.url_template.substitute(morningstar_fund_id=context.morningstar_fund_id)

    def fill_result(self, fund_response: FundResponse, context: FundContext) -> None:
        response = fund_response.response
        if response is None or response.text == 'null':
            context.standard_deviation_five_years = NO_DATA
            context.standard_deviation_ten_years = NO_DATA
            context.sharp_rate_five_years = NO_DATA
            context.sharp_rate_ten_years = NO_DATA
            context.alpha_to_ind = NO_DATA
            context.beta_to_ind = NO_DATA
            context.r_squared_to_ind = NO_DATA
            return 

        return_json_list = json.loads(response.text)
        for fund_risk in return_json_list['RiskAssessment']:
            if fund_risk['Name'] == '标准差（%）':
                context.standard_deviation_five_years = fund_risk['Year5'] if fund_risk['Year5'] else NO_DATA
                context.standard_deviation_ten_years = fund_risk['Year10'] if fund_risk['Year10'] else NO_DATA
                continue
            if fund_risk['Name'] == '夏普比率':
                context.sharp_rate_five_years = fund_risk['Year5'] if fund_risk['Year5'] else NO_DATA
                context.sharp_rate_ten_years = fund_risk['Year10'] if fund_risk['Year10'] else NO_DATA
                continue

        for fund_risk in return_json_list['RiskStats']:
            if fund_risk['Name'] == '阿尔法系数（%）':
                context.alpha_to_ind = fund_risk['ToInd'] if fund_risk['ToInd'] else NO_DATA
                continue
            if fund_risk['Name'] == '贝塔系数':
                context.beta_to_ind = fund_risk['ToInd'] if fund_risk['ToInd'] else NO_DATA
                continue
            if fund_risk['Name'] == 'R平方':
                context.r_squared_to_ind = fund_risk['ToInd'] if fund_risk['ToInd'] else NO_DATA
                continue
