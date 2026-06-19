"""基金筛选分析 — TT_STANDARD compatible"""

import csv
from datetime import date
from heapq import nlargest
from pathlib import Path

from module.constants import FundAttrKey as K

_CSV_PATH = Path("result/result.csv")
_SKIP = {"NO_DATA", "DATA_ERROR", "DATA_IGNORE"}


def _safe_float(row: dict, key: str) -> float | None:
    val = row[key]
    if not val or val in _SKIP:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _tenure_years(row: dict) -> float:
    try:
        return (date.today() - date.fromisoformat(row[K.DATE_OF_APPOINTMENT])).days / 365
    except (ValueError, TypeError):
        return 0.0


def _load(path: Path | None = None) -> list[dict]:
    p = path or _CSV_PATH
    with open(p, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── 策略 ──

def analyse_bond(funds: list[dict]) -> list[dict]:
    """纯债筛选: 规模>50亿 → 经理>5年 → 夏普(近三年)前5"""
    candidates = [
        r for r in funds
        if "债券型" in r[K.FUND_TYPE]
        and "纯债" in r[K.FUND_SIMPLE_NAME]
        and "定开" not in r[K.FUND_SIMPLE_NAME]
        and "定期" not in r[K.FUND_SIMPLE_NAME]
        and (size := _safe_float(r, K.FUND_SIZE)) and size > 20
        and _tenure_years(r) > 5
        and _safe_float(r, K.SHARP_RATE_THREE_YEARS) is not None
    ]
    return nlargest(5, candidates, key=lambda r: _safe_float(r, K.SHARP_RATE_THREE_YEARS))


def analyse_equity(funds: list[dict]) -> list[dict]:
    """指数/混合筛选: 规模>10亿 → 经理>5年 → 排除C/Y → 夏普(近三年)前5"""
    candidates = [
        r for r in funds
        if (
            ("指数型" in r[K.FUND_TYPE] and "海外股票" not in r[K.FUND_TYPE] and "固收" not in r[K.FUND_TYPE])
            or ("混合型" in r[K.FUND_TYPE] and "偏债" not in r[K.FUND_TYPE])
        )
        and (size := _safe_float(r, K.FUND_SIZE)) and size > 10
        and "C" not in r[K.FUND_SIMPLE_NAME]
        and "Y" not in r[K.FUND_SIMPLE_NAME]
        and _tenure_years(r) > 5
        and _safe_float(r, K.SHARP_RATE_THREE_YEARS) is not None
    ]
    return nlargest(5, candidates, key=lambda r: _safe_float(r, K.SHARP_RATE_THREE_YEARS))


# ── 输出 ──

def _print_section(title: str, results: list[dict]) -> None:
    if not results:
        print(f"\n{title} — 无符合条件基金")
        return
    print(f"\n{title} ({len(results)}只)")
    print("-" * 72)
    for i, r in enumerate(results, 1):
        tenure = _tenure_years(r)
        sharpe = _safe_float(r, K.SHARP_RATE_THREE_YEARS)
        size = _safe_float(r, K.FUND_SIZE)
        print(
            f"  {i}. {r[K.FUND_CODE]}  {r[K.FUND_SIMPLE_NAME]}  "
            f"规模{size:.1f}亿  经理{tenure:.1f}年  Sharpe(3Y)={sharpe:.2f}"
        )


if __name__ == "__main__":
    funds = _load()
    print(f"基金总数: {len(funds)}  |  {date.today()}")

    _print_section("纯债基金", analyse_bond(funds))
    _print_section("指数/混合基金", analyse_equity(funds))
