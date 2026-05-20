"""天天基金网 — overview + manager 页面解析"""

import re
from string import Template

from crawler.fund_context import FundContext
from utils.constants import number_in_eng, NO_DATA, DATA_IGNORE

# ── URL 构造 ────────────────────────────────────────────────

_overview_t = Template('http://fundf10.eastmoney.com/jbgk_$fund_code.html')
_manager_t = Template('http://fundf10.eastmoney.com/jjjl_$fund_code.html')


def build_overview_url(ctx: FundContext) -> str:
    return _overview_t.substitute(fund_code=ctx.fund_code)


def build_manager_url(ctx: FundContext) -> str:
    return _manager_t.substitute(fund_code=ctx.fund_code)


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
        ft = m.group(1)
        ctx.fund_type = NO_DATA if not ft and ctx.fund_code == '023713' else ft

    if m := _fund_size_re.search(html):
        fund_size = m.group(1) if m.group(1) else m.group(2).replace(',', '')
        ctx.fund_size = fund_size if fund_size != '---' else NO_DATA

    if m := _fund_company_re.search(html):
        ctx.fund_company = m.group(1)

    if m := _fund_value_re.search(html):
        ctx.fund_value = m.group(1)

    if m := _management_fee_re.search(html):
        fee_rate = m.group(1)
        if fee_rate == '<a':
            ctx.management_fee_rate = DATA_IGNORE
        else:
            ctx.management_fee_rate = fee_rate if fee_rate != '---' else NO_DATA

    if m := _custody_fee_re.search(html):
        ctx.custody_fee_rate = m.group(1) if m.group(1) != '---' else NO_DATA

    if m := _sales_service_fee_re.search(html):
        ctx.sales_service_fee_rate = m.group(1) if m.group(1) != '---' else NO_DATA


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
