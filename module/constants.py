"""常量和枚举"""

from enum import unique, StrEnum


@unique
class FundAttrKey(StrEnum):
    """
    基金属性枚举
    """
    # 东方财富 — 基本概况 + 基金经理
    FUND_CODE = '基金代码',
    FUND_SIMPLE_NAME = '基金简称',
    FUND_TYPE = '基金类型',
    FUND_SIZE = '资产规模(亿)',
    MANAGEMENT_FEE_RATE = '管理费率(每年)',
    CUSTODY_FEE_RATE = '托管费率(每年)',
    SALES_SERVICE_FEE_RATE = '销售服务费率(每年)',

    FUND_COMPANY = '基金管理人',
    FUND_VALUE = '基金净值',
    # 兼容带新场景，A+B -> B -> B+C，此时基金经理为时长最长的B，对应的任职时间为 这三段 B连续任职的任职时间
    FUND_MANAGER = '基金经理(最近连续最长任职)',
    DATE_OF_APPOINTMENT = '基金经理的上任时间',

    # 东方财富 — 特色数据
    STANDARD_DEVIATION_THREE_YEARS = '标准差(近三年)',
    SHARP_RATE_THREE_YEARS = '夏普比率(近三年)',

    # 晨星
    MORNINGSTAR_FUND_ID = '(晨星)基金代码',
    ANNUALIZED_RETURN_FIVE_YEAR = "五年回报(年化)",
    ANNUALIZED_RETURN_TEN_YEAR = "十年回报(年化)",
    STANDARD_DEVIATION_FIVE_YEARS = '标准差(五年%)',
    STANDARD_DEVIATION_TEN_YEARS = '标准差(十年%)',
    SHARP_RATE_FIVE_YEARS = '夏普比率(五年)',
    SHARP_RATE_TEN_YEARS = '夏普比率(十年)',
    ALPHA_TO_IND = '阿尔法系数(相对于基准指数%)',
    BETA_TO_IND = '贝塔系数(相对于基准指数)',
    R_SQUARED_TO_IND = 'R平方(相对于基准指数)',


# ── 预定义字段组合 ──

# 东方财富 — 基本数据（天天基金网：基本概况 + 基金经理）
EM_BASIC: frozenset[FundAttrKey] = frozenset({
    FundAttrKey.FUND_TYPE, FundAttrKey.FUND_SIZE, FundAttrKey.FUND_COMPANY,
    FundAttrKey.FUND_VALUE, FundAttrKey.MANAGEMENT_FEE_RATE,
    FundAttrKey.CUSTODY_FEE_RATE, FundAttrKey.SALES_SERVICE_FEE_RATE,
    FundAttrKey.FUND_MANAGER, FundAttrKey.DATE_OF_APPOINTMENT,
})

# 东方财富 — 标准数据（基本数据 + 特色数据：近三年标准差、夏普比率）
EM_STANDARD: frozenset[FundAttrKey] = EM_BASIC | frozenset({
    FundAttrKey.STANDARD_DEVIATION_THREE_YEARS,
    FundAttrKey.SHARP_RATE_THREE_YEARS,
})

# 东方财富 + 晨星 — 完整数据
# 注意：晨星有反爬策略（WAF + 限流），完整爬取速度较慢，适合少量基金使用
EM_MS_FULL: frozenset[FundAttrKey] = EM_BASIC | frozenset({
    FundAttrKey.MORNINGSTAR_FUND_ID,
    FundAttrKey.ANNUALIZED_RETURN_FIVE_YEAR, FundAttrKey.ANNUALIZED_RETURN_TEN_YEAR,
    FundAttrKey.STANDARD_DEVIATION_FIVE_YEARS, FundAttrKey.STANDARD_DEVIATION_TEN_YEARS,
    FundAttrKey.SHARP_RATE_FIVE_YEARS, FundAttrKey.SHARP_RATE_TEN_YEARS,
    FundAttrKey.ALPHA_TO_IND, FundAttrKey.BETA_TO_IND, FundAttrKey.R_SQUARED_TO_IND,
})

# 最终爬取结果文件的占位，用于区分是真的木有数据，还是爬取可能失败/遗漏
NO_DATA = 'NO_DATA'
DATA_ERROR = 'DATA_ERROR'
DATA_IGNORE = 'DATA_IGNORE'

# 日志输出格式
log_format = '%(asctime)s [%(processName)s/%(levelname)s] %(message)s'
