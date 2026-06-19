"""天天基金网 — overview + manager + tsdata 页面解析"""

import re
from string import Template

from module.constants import NO_DATA, DATA_IGNORE
from module.fund_context import FundContext

# 带千分号的数字表达形式 -10,000.12
number_in_eng = r'-?(\d+?(,\d+)*?(\.\d+)?)'

# ── URL 构造 ────────────────────────────────────────────────

_overview_t = Template('http://fundf10.eastmoney.com/jbgk_$fund_code.html')
_manager_t = Template('http://fundf10.eastmoney.com/jjjl_$fund_code.html')
_tsdata_t = Template('http://fundf10.eastmoney.com/tsdata_$fund_code.html')


def build_overview_url(ctx: FundContext) -> str:
    return _overview_t.substitute(fund_code=ctx.fund_code)


def build_manager_url(ctx: FundContext) -> str:
    return _manager_t.substitute(fund_code=ctx.fund_code)


def build_tsdata_url(ctx: FundContext) -> str:
    return _tsdata_t.substitute(fund_code=ctx.fund_code)


# ── overview 解析 ───────────────────────────────────────────

_fund_type_re = re.compile(r'基金类型</th><td>(.*?)</td></tr><tr><th>发行日期')
_fund_size_re = re.compile(fr'(?:净)?资产规模</th><td>(---)|({number_in_eng})亿元')
_fund_company_re = re.compile(r'基金管理人</th><td><a.*?">(.+?)</a></td><th>基金托管人')
_fund_value_re = re.compile(fr'单位净值.*?：[\s\S]*?({number_in_eng})\s')
_management_fee_re = re.compile(fr'管理费率</th><td>(({number_in_eng})%|---|<a)')
_custody_fee_re = re.compile(fr'托管费率</th><td>(({number_in_eng})%|---)')
_sales_service_fee_re = re.compile(fr'销售服务费率</th><td>(({number_in_eng})%|---)')


def parse_overview(html: str | None, ctx: FundContext) -> None:
    if html is None:
        return

    if m := _fund_type_re.search(html):
        ctx.fund_type = m.group(1) or NO_DATA

    if m := _fund_size_re.search(html):
        fund_size = m.group(1) if m.group(1) else m.group(2).replace(',', '')
        ctx.fund_size = fund_size if fund_size != '---' else NO_DATA

    if m := _fund_company_re.search(html):
        ctx.fund_company = m.group(1)

    if m := _fund_value_re.search(html):
        ctx.fund_value = m.group(1)

    def _parse_fee(m, attr: str) -> None:
        if not m:
            return
        val = m.group(1)
        if val == '<a':
            setattr(ctx, attr, DATA_IGNORE)
        else:
            setattr(ctx, attr, val if val != '---' else NO_DATA)

    _parse_fee(_management_fee_re.search(html), "management_fee_rate")
    _parse_fee(_custody_fee_re.search(html), "custody_fee_rate")
    _parse_fee(_sales_service_fee_re.search(html), "sales_service_fee_rate")


# ── manager 解析 ────────────────────────────────────────────

_manager_name_re = re.compile(r'现任基金经理简介[\s\S]+?姓名：[\s\S]+?<a.+?>(.+?)</a>')
_manager_date_re = re.compile(r'现任基金经理简介[\s\S]+?上任日期：[\s\S]+?>(.+?)</p>')


def parse_manager(html: str | None, ctx: FundContext) -> None:
    if html is None:
        return
    if m := _manager_name_re.search(html):
        ctx.fund_manager = m.group(1)
    if m := _manager_date_re.search(html):
        ctx.date_of_appointment = m.group(1)


# ── tsdata 解析 ─────────────────────────────────────────────

# 定位"标准差"行 → 跳过2个<td> → 取第3个 → "近3年"
_tsdata_stddev_re = re.compile(
    r'<td>标准差</td><td[^>]*>.*?</td><td[^>]*>.*?</td><td[^>]*>(.*?)</td>'
)
_tsdata_sharp_re = re.compile(
    r'<td>夏普比率</td><td[^>]*>.*?</td><td[^>]*>.*?</td><td[^>]*>(.*?)</td>'
)


def parse_tsdata(html: str | None, ctx: FundContext) -> None:
    if html is None:
        return
    if m := _tsdata_stddev_re.search(html):
        ctx.standard_deviation_three_years = m.group(1) or NO_DATA
    if m := _tsdata_sharp_re.search(html):
        ctx.sharp_rate_three_years = m.group(1) or NO_DATA
