"""晨星中国 — morningstar / return / risk 页面解析"""

import json
from string import Template

from module.constants import NO_DATA
from module.fund_context import FundContext

# ── URL 构造 ────────────────────────────────────────────────

_ms_t = Template('https://www.morningstar.cn/handler/fundsearch.ashx?q=$fund_code&limit=1')
_return_t = Template('https://www.morningstar.cn/handler/quicktake.ashx?command=return&fcid=$morningstar_fund_id')
_risk_t = Template('https://www.morningstar.cn/handler/quicktake.ashx?command=rating&fcid=$morningstar_fund_id')


def build_morningstar_url(ctx: FundContext) -> str:
    return _ms_t.substitute(fund_code=ctx.fund_code)


def _has_ms_id(ctx: FundContext) -> bool:
    """MS 基金 ID 是否已获取（未获取时 P2 步骤应跳过）"""
    msid = ctx.morningstar_fund_id
    return bool(msid) and msid != NO_DATA


def build_return_url(ctx: FundContext) -> str:
    if not _has_ms_id(ctx):
        return ""
    return _return_t.substitute(morningstar_fund_id=ctx.morningstar_fund_id)


def build_risk_url(ctx: FundContext) -> str:
    if not _has_ms_id(ctx):
        return ""
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


def _val(item: dict, key: str) -> str:
    return item[key] if item.get(key) else NO_DATA


# ── return 解析 ─────────────────────────────────────────────

def parse_return(json_text: str | None, ctx: FundContext) -> None:
    if json_text is None:
        return
    returns = json.loads(json_text)['CurrentReturn']['Return']
    for r in returns:
        if r['Name'] == '五年回报（年化）':
            ctx.annualized_return_five_year = _val(r, 'Return')
        elif r['Name'] == '十年回报（年化）':
            ctx.annualized_return_ten_year = _val(r, 'Return')


# ── risk 解析 ───────────────────────────────────────────────

def parse_risk(json_text: str | None, ctx: FundContext) -> None:
    if json_text is None or json_text == 'null':
        _fill_risk_no_data(ctx)
        return

    data = json.loads(json_text)

    for item in data.get('RiskAssessment', []):
        if item['Name'] == '标准差（%）':
            ctx.standard_deviation_five_years = _val(item, 'Year5')
            ctx.standard_deviation_ten_years = _val(item, 'Year10')
        elif item['Name'] == '夏普比率':
            ctx.sharp_rate_five_years = _val(item, 'Year5')
            ctx.sharp_rate_ten_years = _val(item, 'Year10')

    for item in data.get('RiskStats', []):
        if item['Name'] == '阿尔法系数（%）':
            ctx.alpha_to_ind = _val(item, 'ToInd')
        elif item['Name'] == '贝塔系数':
            ctx.beta_to_ind = _val(item, 'ToInd')
        elif item['Name'] == 'R平方':
            ctx.r_squared_to_ind = _val(item, 'ToInd')


_RISK_FIELDS = [
    "standard_deviation_five_years", "standard_deviation_ten_years",
    "sharp_rate_five_years", "sharp_rate_ten_years",
    "alpha_to_ind", "beta_to_ind", "r_squared_to_ind",
]


def _fill_risk_no_data(ctx: FundContext) -> None:
    for field in _RISK_FIELDS:
        setattr(ctx, field, NO_DATA)
