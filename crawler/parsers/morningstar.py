"""晨星中国 — morningstar / return / risk 页面解析"""

import json
from string import Template

from crawler.fund_context import FundContext
from utils.constants import NO_DATA

# ── URL 构造 ────────────────────────────────────────────────

_ms_t = Template('https://www.morningstar.cn/handler/fundsearch.ashx?q=$fund_code&limit=1')
_return_t = Template('https://www.morningstar.cn/handler/quicktake.ashx?command=return&fcid=$morningstar_fund_id')
_risk_t = Template('https://www.morningstar.cn/handler/quicktake.ashx?command=rating&fcid=$morningstar_fund_id')


def build_morningstar_url(ctx: FundContext) -> str:
    return _ms_t.substitute(fund_code=ctx.fund_code)


def build_return_url(ctx: FundContext) -> str:
    return _return_t.substitute(morningstar_fund_id=ctx.morningstar_fund_id)


def build_risk_url(ctx: FundContext) -> str:
    return _risk_t.substitute(morningstar_fund_id=ctx.morningstar_fund_id)


# ── morningstar 解析 ────────────────────────────────────────

def parse_morningstar(json_text: str | None, ctx: FundContext) -> None:
    if json_text is None:
        return
    data = json.loads(json_text)
    if data:
        ctx.morningstar_fund_id = data[0]['FundClassId'] if data[0]['FundClassId'] else NO_DATA
    else:
        ctx.morningstar_fund_id = NO_DATA


# ── return 解析 ─────────────────────────────────────────────

def parse_return(json_text: str | None, ctx: FundContext) -> None:
    if json_text is None:
        return
    returns = json.loads(json_text)['CurrentReturn']['Return']
    for r in returns:
        if r['Name'] == '五年回报（年化）':
            ctx.annualized_return_five_year = r['Return'] if r['Return'] else NO_DATA
        elif r['Name'] == '十年回报（年化）':
            ctx.annualized_return_ten_year = r['Return'] if r['Return'] else NO_DATA


# ── risk 解析 ───────────────────────────────────────────────

def parse_risk(json_text: str | None, ctx: FundContext) -> None:
    if json_text is None or json_text == 'null':
        _fill_risk_no_data(ctx)
        return

    data = json.loads(json_text)

    for item in data.get('RiskAssessment', []):
        if item['Name'] == '标准差（%）':
            ctx.standard_deviation_five_years = item['Year5'] if item['Year5'] else NO_DATA
            ctx.standard_deviation_ten_years = item['Year10'] if item['Year10'] else NO_DATA
        elif item['Name'] == '夏普比率':
            ctx.sharp_rate_five_years = item['Year5'] if item['Year5'] else NO_DATA
            ctx.sharp_rate_ten_years = item['Year10'] if item['Year10'] else NO_DATA

    for item in data.get('RiskStats', []):
        if item['Name'] == '阿尔法系数（%）':
            ctx.alpha_to_ind = item['ToInd'] if item['ToInd'] else NO_DATA
        elif item['Name'] == '贝塔系数':
            ctx.beta_to_ind = item['ToInd'] if item['ToInd'] else NO_DATA
        elif item['Name'] == 'R平方':
            ctx.r_squared_to_ind = item['ToInd'] if item['ToInd'] else NO_DATA


def _fill_risk_no_data(ctx: FundContext) -> None:
    ctx.standard_deviation_five_years = NO_DATA
    ctx.standard_deviation_ten_years = NO_DATA
    ctx.sharp_rate_five_years = NO_DATA
    ctx.sharp_rate_ten_years = NO_DATA
    ctx.alpha_to_ind = NO_DATA
    ctx.beta_to_ind = NO_DATA
    ctx.r_squared_to_ind = NO_DATA
