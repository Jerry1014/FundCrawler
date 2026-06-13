"""基金筛选分析"""

import csv
import sys
from datetime import date
from heapq import nlargest
from pathlib import Path

from module.constants import FundAttrKey as K

# Windows 终端默认 GBK，强制 UTF-8 以支持 ²╔═╗ 等字符
sys.stdout.reconfigure(encoding='utf-8')

_CSV_PATH = Path("./result/result-bak.csv")
_SKIP = {"NO_DATA", "DATA_ERROR", "DATA_IGNORE"}


# ── 数据加载 ──

def _read_funds() -> list[dict]:
    with open(_CSV_PATH, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── 通用取值 ──

def _safe_float(row: dict, key: str) -> float | None:
    val = row[key]
    if not val or val in _SKIP:
        return None
    return float(val)


def _fee_rate(row: dict, key: str) -> float:
    """解析费率，缺失视为 0"""
    val = row[key]
    if not val or val in _SKIP:
        return 0.0
    return float(val.rstrip("%"))


def _total_fee(row: dict) -> float:
    """管理费 + 托管费 + 销售服务费"""
    return (_fee_rate(row, K.MANAGEMENT_FEE_RATE) +
            _fee_rate(row, K.CUSTODY_FEE_RATE) +
            _fee_rate(row, K.SALES_SERVICE_FEE_RATE))


def _tenure_years(row: dict) -> float:
    try:
        return (date.today() - date.fromisoformat(row[K.DATE_OF_APPOINTMENT])).days / 365
    except (ValueError, TypeError):
        return 0.0


# ── 表格对齐 ──

def _pad(s: str, width: int) -> str:
    """按终端显示宽度补齐，中文字符占 2 格"""
    w = sum(2 if ord(c) > 0x2000 else 1 for c in s)
    return s + " " * max(0, width - w)


# ── 纯债基金 ──

def analyse_bond(funds: list[dict]) -> None:
    """纯债筛选: 规模>50亿 → 经理>5年 → 卡玛比率（或夏普）前5"""

    candidates = [
        r for r in funds
        if "债券型" in r[K.FUND_TYPE]
           and "纯债" in r[K.FUND_SIMPLE_NAME]
           and (size := _safe_float(r, K.FUND_SIZE)) and size > 50
           and _tenure_years(r) > 5
           and _total_fee(r) < 0.8
           and _safe_float(r, K.SHARP_RATE_FIVE_YEARS) is not None
    ]

    top5 = nlargest(5, candidates, key=lambda r: _safe_float(r, K.SHARP_RATE_FIVE_YEARS))

    print(f"\n纯债基金（规模>50亿  经理>5年  总费率<0.8%  五年夏普前5）")
    print(f"  达标 {len(candidates)} 只 → 最终 {len(top5)} 只")
    print()

    for i, r in enumerate(top5, 1):
        print(f"  {i:>2}. {_pad(r[K.FUND_SIMPLE_NAME], 22)}{r[K.FUND_CODE]:>7s}    {r[K.FUND_COMPANY]}")


# ── 指数/混合基金 ──

def analyse_equity(funds: list[dict]) -> None:
    """指数/混合筛选: 规模>10亿 → 经理>8年 → 排C/Y → R²>60 → Alpha前10 → Sharpe前5"""

    candidates = [
        r for r in funds
        if (("指数型" in r[K.FUND_TYPE] and "海外股票" not in r[K.FUND_TYPE])
            or ("混合型" in r[K.FUND_TYPE] and "偏债" not in r[K.FUND_TYPE]))
           and (size := _safe_float(r, K.FUND_SIZE)) and size > 10
           and "C" not in r[K.FUND_SIMPLE_NAME] and "Y" not in r[K.FUND_SIMPLE_NAME]
           and _tenure_years(r) > 8
    ]

    # R² > 60 — Alpha 才有统计意义
    valid = [
        r for r in candidates
        if r[K.ALPHA_TO_IND] not in _SKIP
           and r[K.R_SQUARED_TO_IND] not in _SKIP
           and float(r[K.R_SQUARED_TO_IND]) > 60
           and _safe_float(r, K.SHARP_RATE_TEN_YEARS) is not None
    ]

    # 第一阶段: 按 Alpha 取前 10
    alpha_top10 = nlargest(10, valid,
                           key=lambda r: float(r[K.ALPHA_TO_IND]))

    # 第二阶段: 从中按十年夏普取前 5
    top5 = nlargest(5, alpha_top10,
                    key=lambda r: float(r[K.SHARP_RATE_TEN_YEARS]))

    print(f"\n指数/混合基金（规模>10亿  经理>8年  排C/Y  R²>60  Alpha前10→Sharpe前5）")
    print(f"  达标 {len(candidates)} 只 → R²>60 有效 {len(valid)} 只 → 最终 {len(top5)} 只")
    print()

    for i, r in enumerate(top5, 1):
        print(f"  {i:>2}. {_pad(r[K.FUND_SIMPLE_NAME], 22)}{r[K.FUND_CODE]:>7s}    {r[K.FUND_COMPANY]}")


# ── 入口 ──

if __name__ == "__main__":
    funds = _read_funds()

    W = 64

    def _box(s: str) -> str:
        return "║  " + _pad(s, W - 4) + "║"

    today_str = str(date.today())
    count_str = f"基金总数: {len(funds):,}"
    info = f"数据日期: {today_str}    {count_str}"

    print("╔" + "═" * (W - 2) + "╗")
    print(_box("FundCrawler 基金筛选分析"))
    print(_box(info))
    print("╚" + "═" * (W - 2) + "╝")

    analyse_bond(funds)
    analyse_equity(funds)
