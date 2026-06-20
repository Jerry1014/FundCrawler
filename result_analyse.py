"""基金筛选分析 — TT_STANDARD compatible"""

import csv
from datetime import date
from pathlib import Path

from module.constants import FundAttrKey as K

_CSV_PATH = Path("result/result.csv")
_SKIP = {"NO_DATA", "DATA_ERROR", "DATA_IGNORE"}


def _safe_float(row: dict, key: str) -> float | None:
    val = row[key]
    if not val or val in _SKIP:
        return None
    try:
        return float(val.rstrip("%"))
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

def _total_fee(row: dict) -> float | None:
    m = _safe_float(row, K.MANAGEMENT_FEE_RATE)
    c = _safe_float(row, K.CUSTODY_FEE_RATE)
    if None in (m, c):
        return None
    s = _safe_float(row, K.SALES_SERVICE_FEE_RATE) or 0.0
    return m + c + s


def analyse_bond(funds: list[dict]) -> list[dict]:
    """纯债: 长债/中短债 → 经理≥5年 → 规模>20亿 → 排定开/定期 → 夏普前40% → 费率低20"""
    _BOND_TYPES = {"债券型-长债", "债券型-中短债"}
    _BOND_KW = ("纯债", "信用债", "利率债", "中短债")
    candidates = [
        r for r in funds
        if r[K.FUND_TYPE] in _BOND_TYPES
        and any(kw in r[K.FUND_SIMPLE_NAME] for kw in _BOND_KW)
        and _tenure_years(r) >= 5
        and (size := _safe_float(r, K.FUND_SIZE)) and size > 20
        and "定开" not in r[K.FUND_SIMPLE_NAME]
        and "定期" not in r[K.FUND_SIMPLE_NAME]
        and _safe_float(r, K.SHARP_RATE_THREE_YEARS) is not None
        and _total_fee(r) is not None
    ]
    candidates.sort(key=lambda r: _safe_float(r, K.SHARP_RATE_THREE_YEARS), reverse=True)
    top40_count = max(1, int(len(candidates) * 0.4))
    top40 = candidates[:top40_count]
    top40.sort(key=lambda r: _total_fee(r))
    return top40[:20]


def analyse_equity(funds: list[dict]) -> list[dict]:
    """权益: 偏股/灵活/股票/平衡型 → 经理≥5年 → 规模>10亿 → 销售服务费=0 → 排定开/定期/持有/滚动 → 波动率≥10% → 夏普前30% → 费率低20"""
    _ACTIVE_EQUITY = {"混合型-偏股", "混合型-灵活", "股票型", "混合型-平衡"}
    _CLOSED_KW = ("定开", "定期", "持有", "滚动")
    candidates = [
        r for r in funds
        if r[K.FUND_TYPE] in _ACTIVE_EQUITY
        and _tenure_years(r) >= 5
        and (size := _safe_float(r, K.FUND_SIZE)) and size > 10
        and _safe_float(r, K.SALES_SERVICE_FEE_RATE) in (0.0, None)
        and not any(kw in r[K.FUND_SIMPLE_NAME] for kw in _CLOSED_KW)
        and _safe_float(r, K.SHARP_RATE_THREE_YEARS) is not None
        and _total_fee(r) is not None
        and (stddev := _safe_float(r, K.STANDARD_DEVIATION_THREE_YEARS)) and stddev >= 10
    ]
    candidates.sort(key=lambda r: _safe_float(r, K.SHARP_RATE_THREE_YEARS), reverse=True)
    top30_count = max(1, int(len(candidates) * 0.3))
    top30 = candidates[:top30_count]
    top30.sort(key=lambda r: _total_fee(r))
    return top30[:20]


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
        fee = _total_fee(r)
        stddev = _safe_float(r, K.STANDARD_DEVIATION_THREE_YEARS)
        std_str = f"波动={stddev:.2f}" if stddev else ""
        print(
            f"  {i}. {r[K.FUND_CODE]} / {r[K.FUND_SIMPLE_NAME]} / {r[K.FUND_MANAGER]} / "
            f"{tenure:.1f}年 / {size:.1f}亿 / {fee:.2f}% / Sharpe={sharpe:.2f} / {std_str}"
        )


if __name__ == "__main__":
    funds = _load()
    print(f"基金总数: {len(funds)}  |  {date.today()}")

    _print_section("纯债基金", analyse_bond(funds))
    _print_section("指数/混合基金", analyse_equity(funds))
