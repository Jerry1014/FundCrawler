"""基金筛选分析"""

import csv
from datetime import date
from heapq import nlargest
from pathlib import Path

from utils.constants import FundAttrKey
from utils.top_k_holder import TopKHolder

_CSV_PATH = Path("./result/result.csv")
_SKIP = {"NO_DATA", "DATA_ERROR", "DATA_IGNORE"}


def _read_funds():
    with open(_CSV_PATH, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _fee_rate(row, key):
    val = row[key]
    if not val or val in _SKIP:
        return 0.0
    return float(val.rstrip("%"))


def _annual_fee(row):
    return (_fee_rate(row, FundAttrKey.MANAGEMENT_FEE_RATE) +
            _fee_rate(row, FundAttrKey.CUSTODY_FEE_RATE) +
            _fee_rate(row, FundAttrKey.SALES_SERVICE_FEE_RATE))


def _safe_float(row, key):
    val = row[key]
    if not val or val in _SKIP:
        return None
    return float(val)


def _tenure_days(row):
    try:
        return (date.today() - date.fromisoformat(row[FundAttrKey.DATE_OF_APPOINTMENT])).days
    except (ValueError, TypeError):
        return 0


def analyse(fund_filter, tenure_day_filter):
    funds = _read_funds()

    by_type = [r for r in funds
               if fund_filter(r[FundAttrKey.FUND_SIMPLE_NAME],
                              r[FundAttrKey.FUND_TYPE],
                              _safe_float(r, FundAttrKey.FUND_SIZE))]
    print(f"符合类型要求的基金数量为{len(by_type)}")

    by_tenure = [r for r in by_type if tenure_day_filter(_tenure_days(r))]
    print(f"符合时间要求的基金数量为{len(by_tenure)}")

    # 夏普前 10%（R² > 60）
    top_n = max(len(by_tenure) // 10, 1)
    sharp_holder = TopKHolder(
        lambda r: float(r[FundAttrKey.SHARP_RATE_TEN_YEARS]), top_n)
    for r in by_tenure:
        if (r[FundAttrKey.SHARP_RATE_TEN_YEARS] not in _SKIP
                and r[FundAttrKey.R_SQUARED_TO_IND] not in _SKIP
                and float(r[FundAttrKey.R_SQUARED_TO_IND]) > 60):
            sharp_holder.put(r)
    by_sharp = sharp_holder.cur_k()

    # 阿尔法前三（扣费）
    alpha_top = nlargest(3, by_sharp,
                         key=lambda r: (_safe_float(r, FundAttrKey.ALPHA_TO_IND) or 0) - _annual_fee(r))
    alpha_top = [r for r in alpha_top
                 if r[FundAttrKey.ALPHA_TO_IND] not in _SKIP
                 and r[FundAttrKey.MANAGEMENT_FEE_RATE] != "DATA_IGNORE"]
    print("根据阿尔法选择的基金是：")
    for r in alpha_top:
        print(f"  {r[FundAttrKey.FUND_SIMPLE_NAME]}  {r[FundAttrKey.FUND_CODE]}"
              f"  阿尔法={r[FundAttrKey.ALPHA_TO_IND]}"
              f"  年费={_annual_fee(r):.2f}%")

    # 年化回报前三（扣费）
    return_top = nlargest(3, by_tenure,
                          key=lambda r: (_safe_float(r, FundAttrKey.ANNUALIZED_RETURN_TEN_YEAR) or 0) - _annual_fee(r))
    return_top = [r for r in return_top
                  if r[FundAttrKey.ANNUALIZED_RETURN_TEN_YEAR] not in _SKIP
                  and r[FundAttrKey.MANAGEMENT_FEE_RATE] != "DATA_IGNORE"]

    # 年化好但阿尔法差的基金
    alpha_names = {r[FundAttrKey.FUND_SIMPLE_NAME] for r in alpha_top}
    return_no_alpha = [r for r in return_top
                       if r[FundAttrKey.FUND_SIMPLE_NAME] not in alpha_names]
    if return_no_alpha:
        print("年化回报优秀但阿尔法落后的基金：")
        for r in return_no_alpha:
            print(f"  {r[FundAttrKey.FUND_SIMPLE_NAME]}"
                  f"  年化={r[FundAttrKey.ANNUALIZED_RETURN_TEN_YEAR]}%"
                  f"  阿尔法={r[FundAttrKey.ALPHA_TO_IND]}")


if __name__ == "__main__":
    print("⬇️ 纯债基金分析 ⬇️")
    analyse(
        lambda name, ftype, size: ("债券型" in ftype and "纯债" in name
                                    and size and size > 10
                                    and "C" not in name and "Y" not in name),
        lambda days: days > 7 * 365,
    )
    print("⬆️ 纯债基金分析 ⬆️\n")

    print("⬇️ 国内指数/混合基金分析 ⬇️")
    analyse(
        lambda name, ftype, size: (("指数型" in ftype and "海外股票" not in ftype)
                                    or ("混合型" in ftype and "偏债" not in ftype))
                                   and size and size > 10
                                   and "C" not in name and "Y" not in name,
        lambda days: days > 10 * 365,
    )
    print("⬆️ 国内指数/混合基金分析 ⬆️\n")

    print("⬇️ 全部基金比较 ⬇️")
    analyse(
        lambda name, ftype, size: size and size > 10
                                  and "C" not in name and "Y" not in name,
        lambda days: days > 5 * 365,
    )
    print("⬆️ 全部基金比较 ⬆️")
