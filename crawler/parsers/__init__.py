"""STEPS 声明 —— 汇总所有数据源的解析函数"""

from crawler.parsers.eastmoney import (
    build_overview_url, parse_overview,
    build_manager_url, parse_manager,
)
from crawler.parsers.morningstar import (
    build_morningstar_url, parse_morningstar,
    build_return_url, parse_return,
    build_risk_url, parse_risk,
)

STEPS = {
    "overview":    dict(deps=(),               build_url=build_overview_url,    parse=parse_overview),
    "manager":     dict(deps=(),               build_url=build_manager_url,     parse=parse_manager),
    "morningstar": dict(deps=(),               build_url=build_morningstar_url, parse=parse_morningstar),
    "return":      dict(deps=("morningstar",), build_url=build_return_url,      parse=parse_return),
    "risk":        dict(deps=("morningstar",), build_url=build_risk_url,        parse=parse_risk),
}
