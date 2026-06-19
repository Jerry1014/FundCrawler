"""基金数据载体"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class FundContext:
    """基金爬取数据的上下文 —— 各 Step 解析后写入对应字段"""

    # ── 基础信息（来自基金列表）──
    fund_code: str              # 基金代码，如 "000001"
    fund_name: str              # 基金简称，如 "华夏成长混合"

    # ── Phase 1: morningstar Step ──
    morningstar_fund_id: Optional[str] = None  # 晨星内部 ID，如 "F020000001"

    # ── Phase 1: overview Step（天天基金网 HTML）──
    fund_type: Optional[str] = None              # 基金类型，如 "债券型"
    fund_size: Optional[str] = None              # 资产规模（亿元）
    fund_company: Optional[str] = None           # 基金管理人
    fund_value: Optional[str] = None             # 单位净值
    management_fee_rate: Optional[str] = None    # 管理费率（%/年）
    custody_fee_rate: Optional[str] = None       # 托管费率（%/年）
    sales_service_fee_rate: Optional[str] = None # 销售服务费率（%/年）

    # ── Phase 1: manager Step（天天基金网 HTML）──
    fund_manager: Optional[str] = None           # 基金经理（最近连续最长任职）
    date_of_appointment: Optional[str] = None    # 基金经理上任日期

    # ── Phase 2: return Step（晨星 quicktake JSON）──
    annualized_return_five_year: Optional[str] = None  # 五年年化回报（%）
    annualized_return_ten_year: Optional[str] = None   # 十年年化回报（%）

    # ── Phase 1: tsdata Step（天天基金网 HTML）──
    standard_deviation_three_years: Optional[str] = None  # 标准差（近三年，%）
    sharp_rate_three_years: Optional[str] = None          # 夏普比率（近三年）

    # ── Phase 2: risk Step（晨星 quicktake JSON）──
    standard_deviation_five_years: Optional[str] = None  # 五年标准差（%）
    standard_deviation_ten_years: Optional[str] = None   # 十年标准差（%）
    sharp_rate_five_years: Optional[str] = None          # 五年夏普比率
    sharp_rate_ten_years: Optional[str] = None           # 十年夏普比率
    alpha_to_ind: Optional[str] = None                   # 阿尔法系数（%，相对基准）
    beta_to_ind: Optional[str] = None                    # 贝塔系数（相对基准）
    r_squared_to_ind: Optional[str] = None               # R²（相对基准）
