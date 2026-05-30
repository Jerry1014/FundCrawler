"""异步 CSV 结果输出 —— 拓展点：怎么保存"""

import asyncio
import csv
import typing
from pathlib import Path

from crawler.fund_context import FundContext
from utils.constants import FundAttrKey, DATA_ERROR

# CSV 列头 → FundContext 属性名 映射（唯一的数据源）
_COLUMNS: list[tuple[str, str]] = [
    (FundAttrKey.FUND_CODE.value,                       "fund_code"),
    (FundAttrKey.FUND_SIMPLE_NAME.value,                "fund_name"),
    (FundAttrKey.MORNINGSTAR_FUND_ID.value,             "morningstar_fund_id"),
    (FundAttrKey.FUND_TYPE.value,                       "fund_type"),
    (FundAttrKey.FUND_SIZE.value,                       "fund_size"),
    (FundAttrKey.FUND_COMPANY.value,                    "fund_company"),
    (FundAttrKey.FUND_VALUE.value,                      "fund_value"),
    (FundAttrKey.FUND_MANAGER.value,                    "fund_manager"),
    (FundAttrKey.DATE_OF_APPOINTMENT.value,             "date_of_appointment"),
    (FundAttrKey.MANAGEMENT_FEE_RATE.value,             "management_fee_rate"),
    (FundAttrKey.CUSTODY_FEE_RATE.value,                "custody_fee_rate"),
    (FundAttrKey.SALES_SERVICE_FEE_RATE.value,          "sales_service_fee_rate"),
    (FundAttrKey.ANNUALIZED_RETURN_FIVE_YEAR.value,     "annualized_return_five_year"),
    (FundAttrKey.ANNUALIZED_RETURN_TEN_YEAR.value,      "annualized_return_ten_year"),
    (FundAttrKey.STANDARD_DEVIATION_FIVE_YEARS.value,   "standard_deviation_five_years"),
    (FundAttrKey.STANDARD_DEVIATION_TEN_YEARS.value,    "standard_deviation_ten_years"),
    (FundAttrKey.SHARP_RATE_FIVE_YEARS.value,           "sharp_rate_five_years"),
    (FundAttrKey.SHARP_RATE_TEN_YEARS.value,            "sharp_rate_ten_years"),
    (FundAttrKey.ALPHA_TO_IND.value,                    "alpha_to_ind"),
    (FundAttrKey.BETA_TO_IND.value,                     "beta_to_ind"),
    (FundAttrKey.R_SQUARED_TO_IND.value,                "r_squared_to_ind"),
]

_CSV_HEADERS = [header for header, _ in _COLUMNS]


class ResultWriter:
    """异步 CSV 写入器"""

    def __init__(self, path: str = "./result/", filename: str = "result.csv"):
        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)
        self._filepath = self._path / filename
        self._lock = asyncio.Lock()
        self._file: typing.TextIO | None = None
        self._writer: csv.DictWriter | None = None
        self._initialized = False

    async def _ensure_open(self) -> None:
        if self._initialized:
            return
        self._file = open(str(self._filepath), "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=_CSV_HEADERS)
        self._writer.writeheader()
        self._initialized = True

    async def write(self, ctx: FundContext) -> None:
        async with self._lock:
            await self._ensure_open()
            row = {header: getattr(ctx, attr) or DATA_ERROR
                   for header, attr in _COLUMNS}
            self._writer.writerow(row)

    async def flush(self) -> None:
        async with self._lock:
            if self._file:
                self._file.flush()

    async def close(self) -> None:
        async with self._lock:
            if self._file:
                self._file.flush()
                self._file.close()
                self._file = None
                self._writer = None
                self._initialized = False
