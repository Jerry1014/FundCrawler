"""STEPS —— 声明爬取步骤及其依赖关系"""

from dataclasses import dataclass, field
from typing import Callable

from module.constants import FundAttrKey as K
from module.fund_context import FundContext
from module.page_parser.tiantian import (
    build_overview_url, parse_overview,
    build_manager_url, parse_manager,
    build_tsdata_url, parse_tsdata,
)
from module.page_parser.morningstar import (
    build_morningstar_url, parse_morningstar,
    build_return_url, parse_return,
    build_risk_url, parse_risk,
)


@dataclass(frozen=True)
class Step:
    """一个爬取步骤：依赖谁、怎么构造 URL、怎么解析、产出哪些字段"""
    name: str
    build_url: Callable[[FundContext], str]
    parse: Callable[[str | None, FundContext], None]
    provides: frozenset[K]
    deps: tuple[str, ...] = ()


STEPS: list[Step] = [
    Step("overview",
         build_url=build_overview_url,
         parse=parse_overview,
         provides=frozenset({K.FUND_TYPE, K.FUND_SIZE, K.FUND_COMPANY,
                             K.FUND_VALUE, K.MANAGEMENT_FEE_RATE,
                             K.CUSTODY_FEE_RATE, K.SALES_SERVICE_FEE_RATE})),
    Step("manager",
         build_url=build_manager_url,
         parse=parse_manager,
         provides=frozenset({K.FUND_MANAGER, K.DATE_OF_APPOINTMENT})),
    Step("tsdata",
         build_url=build_tsdata_url,
         parse=parse_tsdata,
         provides=frozenset({K.STANDARD_DEVIATION_THREE_YEARS,
                             K.SHARP_RATE_THREE_YEARS})),
    Step("morningstar",
         build_url=build_morningstar_url,
         parse=parse_morningstar,
         provides=frozenset({K.MORNINGSTAR_FUND_ID})),
    Step("return",
         build_url=build_return_url,
         parse=parse_return,
         provides=frozenset({K.ANNUALIZED_RETURN_FIVE_YEAR, K.ANNUALIZED_RETURN_TEN_YEAR}),
         deps=("morningstar",)),
    Step("risk",
         build_url=build_risk_url,
         parse=parse_risk,
         provides=frozenset({K.STANDARD_DEVIATION_FIVE_YEARS, K.STANDARD_DEVIATION_TEN_YEARS,
                             K.SHARP_RATE_FIVE_YEARS, K.SHARP_RATE_TEN_YEARS,
                             K.ALPHA_TO_IND, K.BETA_TO_IND, K.R_SQUARED_TO_IND}),
         deps=("morningstar",)),
]


def resolve_steps(requested: frozenset[K] | None) -> list[Step]:
    """根据请求字段集合，解析所需的爬取步骤（含传递依赖）"""
    if requested is None:
        return list(STEPS)

    needed_names: set[str] = {s.name for s in STEPS if s.provides & requested}

    changed = True
    while changed:
        changed = False
        for s in STEPS:
            if s.name in needed_names:
                for dep in s.deps:
                    if dep not in needed_names:
                        needed_names.add(dep)
                        changed = True

    return [s for s in STEPS if s.name in needed_names]
