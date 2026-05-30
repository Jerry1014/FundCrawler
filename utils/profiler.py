"""请求性能采集器 —— 记录每次 HTTP 请求的耗时，用于瓶颈分析"""

import csv
from pathlib import Path


class RequestProfiler:
    """记录每次 fetch 调用的耗时数据到 CSV"""

    def __init__(self, path: str = "./result/", filename: str = "profile.csv"):
        self._path = Path(path) / filename
        self._file = open(str(self._path), "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        self._writer.writerow([
            "fund_code", "phase", "domain", "endpoint",
            "attempt", "success", "duration_ms", "sem_wait_ms",
        ])

    def record(self, fund_code: str, phase: int, url: str,
               attempt: int, success: bool, duration_ms: float,
               sem_wait_ms: float) -> None:
        domain = "morningstar" if "morningstar" in url else "eastmoney"
        endpoint = url.split("?")[0].split("/")[-1]
        self._writer.writerow([
            fund_code, phase, domain, endpoint,
            attempt, int(success), round(duration_ms, 1), round(sem_wait_ms, 1),
        ])
        self._file.flush()

    def close(self) -> None:
        self._file.close()
