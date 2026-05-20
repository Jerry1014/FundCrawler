"""异步 CSV 结果输出 —— 拓展点：怎么保存"""

import asyncio
import csv
from pathlib import Path

from utils.constants import DATA_ERROR

# 列名常量 —— 与 FundContext.to_result_row() 的 key 顺序一致
_CSV_FIELDNAMES = [
    "基金代码", "基金简称", "(晨星)基金代码", "基金类型", "资产规模(亿)",
    "基金管理人", "基金净值", "基金经理(最近连续最长任职)", "基金经理的上任时间",
    "管理费率(每年)", "托管费率(每年)", "销售服务费率(每年)",
    "五年回报(年化)", "十年回报(年化)", "标准差(五年%)", "标准差(十年%)",
    "夏普比率(五年)", "夏普比率(十年)", "阿尔法系数(相对于基准指数%)", "贝塔系数(相对于基准指数)", "R平方(相对于基准指数)",
]


class ResultWriter:
    """异步 CSV 写入器"""

    def __init__(self, path: str = "./result/", filename: str = "result.csv"):
        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)
        self._filepath = self._path / filename
        self._lock = asyncio.Lock()
        self._file = None
        self._writer = None
        self._initialized = False

    async def _ensure_open(self) -> None:
        if self._initialized:
            return
        self._file = open(str(self._filepath), 'w', newline='', encoding='utf-8')
        self._writer = csv.DictWriter(self._file, fieldnames=_CSV_FIELDNAMES)
        self._writer.writeheader()
        self._initialized = True

    async def write(self, ctx: FundContext) -> None:
        async with self._lock:
            await self._ensure_open()
            row = {header.value: value if value else DATA_ERROR
                   for header, value in ctx.to_result_row().items()}
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
