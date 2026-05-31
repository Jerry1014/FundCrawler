"""HTTP 客户端 + 自适应速率控制 + 重试"""

import asyncio
import logging

import aiohttp
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)


class RateController:
    """自适应速率控制 + 信号量 —— AIMD（加法增、乘法减）。

    调整策略:
    - fail_rate >= 20% → 速率 ×0.75 (温和降速, 避免重试误伤)
    - fail_rate <  20% → 速率 +1
    """

    _FAIL_THRESHOLD = 0.2
    _DECREASE_FACTOR = 0.75

    def __init__(self, initial_rate: int = 1, max_rate: int = 100,
                 min_rate: int = 1, refresh_interval: float = 0.5):
        self._cur_rate = float(initial_rate)
        self._max_rate = max_rate
        self._min_rate = min_rate
        self._refresh_interval = refresh_interval
        self._success = 0
        self._fail = 0
        self._permits = initial_rate
        self._available = initial_rate
        self._cond = asyncio.Condition()
        self._running = False
        self._waiting = {0: 0, 1: 0}  # priority → 等待中的协程数

    async def acquire(self, priority: int = 0) -> None:
        """获取许可。priority=1 (Phase 2) 优先于 priority=0 (Phase 1)。"""
        async with self._cond:
            self._waiting[priority] += 1
            try:
                while self._available <= 0 or (priority == 0 and self._waiting[1] > 0):
                    await self._cond.wait()
                self._available -= 1
            finally:
                self._waiting[priority] -= 1

    async def release(self) -> None:
        async with self._cond:
            self._available += 1
            self._cond.notify_all()

    async def _resize(self, new_permits: int) -> None:
        async with self._cond:
            delta = new_permits - self._permits
            self._permits = new_permits
            if delta > 0:
                self._available += delta
                self._cond.notify_all()

    def record(self, success: bool) -> None:
        if success:
            self._success += 1
        else:
            self._fail += 1

    @property
    def cur_rate(self) -> float:
        return self._cur_rate

    async def start(self) -> None:
        self._running = True
        asyncio.create_task(self._adjust_loop())

    def stop(self) -> None:
        self._running = False

    async def _adjust_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._refresh_interval)
            await self._adjust()

    async def _adjust(self) -> None:
        total = self._success + self._fail
        fail_rate = self._fail / total if total > 0 else 0.0

        old_rate = self._cur_rate
        if fail_rate >= self._FAIL_THRESHOLD:
            self._cur_rate = max(self._min_rate, self._cur_rate * self._DECREASE_FACTOR)
        elif total > 0:
            self._cur_rate = min(self._max_rate, self._cur_rate + 1)

        if self._cur_rate != old_rate:
            logger.debug(
                f"RateController: {old_rate:.1f} → {self._cur_rate:.1f} "
                f"(success={self._success}, fail={self._fail}, fail_rate={fail_rate:.2%})"
            )

        self._success = 0
        self._fail = 0
        await self._resize(int(self._cur_rate))


class Fetcher:
    """带限流、重试、UA 轮换的异步 HTTP 客户端。

    两个域名级 RateController 管控所有请求的并发，AIMD 自动收敛到各域名安全上限。
    """

    _BASE_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    def __init__(self, retry_backoff: float = 1.5):
        self._eastmoney = RateController(initial_rate=20, max_rate=200)
        self._morningstar = RateController(initial_rate=8, min_rate=3, max_rate=200,
                                           refresh_interval=1.0)
        self._retry_backoff = retry_backoff
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "Fetcher":
        await self._eastmoney.start()
        await self._morningstar.start()
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0, limit_per_host=50),
        )
        return self

    async def __aexit__(self, *args) -> None:
        self._eastmoney.stop()
        self._morningstar.stop()
        if self._session:
            await self._session.close()
            self._session = None

    # ── 路由 ──

    @staticmethod
    def _select_rc(url: str, rc_em: RateController, rc_ms: RateController) -> RateController:
        """域名维度：morningstar.cn → rc_ms，其余 → rc_em"""
        return rc_ms if "morningstar" in url else rc_em

    @staticmethod
    def _endpoint_params(url: str) -> tuple[int, int]:
        """返回 (timeout, max_retries)，同域名不同接口按响应速度差异化"""
        if "morningstar" in url:
            if "quicktake" in url:
                return 12, 2  # 详情接口慢
            return 8, 2      # 搜索接口快
        return 10, 3         # EastMoney

    # ── 请求 ──

    async def fetch(self, url: str, fund_code: str = "", phase: int = 0) -> str | None:
        if not url:
            return None

        rc = self._select_rc(url, self._eastmoney, self._morningstar)
        timeout, max_retries = self._endpoint_params(url)
        priority = 1 if phase == 2 else 0  # Phase 2 高优先级

        result: str | None = None
        success = False

        for attempt in range(max_retries):
            await rc.acquire(priority=priority)

            try:
                headers = {**self._BASE_HEADERS, "User-Agent": UserAgent().random}
                req_timeout = aiohttp.ClientTimeout(total=timeout)
                async with self._session.get(url, headers=headers, timeout=req_timeout) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if text:
                            result = text
                            success = True
                            break
                    raise ValueError(f"status={resp.status} or empty")
            except Exception:
                if attempt < max_retries - 1:
                    await asyncio.sleep(self._retry_backoff ** attempt)
            finally:
                await rc.release()

        rc.record(success=success)
        return result
