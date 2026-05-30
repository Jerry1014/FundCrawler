"""STEPS —— 声明爬取步骤及其依赖关系"""

from dataclasses import dataclass
from typing import Callable

from crawler.fund_context import FundContext
from crawler.parsers.eastmoney import (
    build_overview_url, parse_overview,
    build_manager_url, parse_manager,
)
from crawler.parsers.morningstar import (
    build_morningstar_url, parse_morningstar,
    build_return_url, parse_return,
    build_risk_url, parse_risk,
)


@dataclass(frozen=True)
class Step:
    """一个爬取步骤：依赖谁、怎么构造 URL、怎么解析"""
    name: str
    build_url: Callable[[FundContext], str]
    parse: Callable[[str | None, FundContext], None]
    deps: tuple[str, ...] = ()


STEPS: list[Step] = [
    Step("overview",    build_url=build_overview_url,    parse=parse_overview),
    Step("manager",     build_url=build_manager_url,     parse=parse_manager),
    Step("morningstar", build_url=build_morningstar_url, parse=parse_morningstar),
    Step("return",      build_url=build_return_url,      parse=parse_return,      deps=("morningstar",)),
    Step("risk",        build_url=build_risk_url,        parse=parse_risk,        deps=("morningstar",)),
]
