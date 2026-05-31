"""HTTP 客户端 + 自适应速率控制 + 重试"""

import asyncio
import logging

import aiohttp

from utils.fake_ua_getter import singleton_fake_ua

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

    速率控制以**域名**为维度 —— 同一个域名的所有请求共享一个 RateController，
    确保该域名承受的总并发被统一管控，而非按 API 路径分散限流。

    EastMoney（fundf10.eastmoney.com）: 10s 超时, 3 次重试, 初始 20 并发
    Morningstar（www.morningstar.cn）: 初始 8 并发, 1s 探测窗口, 保守起步避反爬
      - 搜索接口（/handler/fundsearch）:  8s 超时, 2 次重试
      - 详情接口（/handler/quicktake）:  12s 超时, 2 次重试（接口慢）
    """

    # 模拟浏览器请求头（CloudFront WAF 可能校验 Accept / Accept-Language）
    _BASE_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    # 域名维度: 速率控制器（AIMD 自动收敛到各域名的安全上限）
    # 终端维度: 超时 + 重试次数（同域名不同接口响应速度不同）
    _ENDPOINT_CONFIG: dict[str, dict] = {
        "eastmoney":   {"timeout": 10, "max_retries": 3},
        "ms_search":   {"timeout": 8,  "max_retries": 2},
        "ms_quicktake": {"timeout": 12, "max_retries": 2},
    }

    def __init__(self, timeout: float = 10, max_retries: int = 3,
                 retry_backoff: float = 1.5):
        # 域名级 RateController —— 各自独立探测, AIMD 收敛到安全上限
        self._eastmoney = RateController(initial_rate=20, max_rate=200)
        self._morningstar = RateController(initial_rate=8, min_rate=3, max_rate=200,
                                           refresh_interval=1.0)
        self._default_timeout = aiohttp.ClientTimeout(total=timeout)
        self._default_max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "Fetcher":
        await self._eastmoney.start()
        await self._morningstar.start()
        self._session = aiohttp.ClientSession(
            timeout=self._default_timeout,
            connector=aiohttp.TCPConnector(limit=0, limit_per_host=50),
        )
        return self

    async def __aexit__(self, *args) -> None:
        self._eastmoney.stop()
        self._morningstar.stop()
        if self._session:
            await self._session.close()
            self._session = None

    def _get_rc(self, url: str) -> RateController:
        """域名维度路由 —— 同域名所有请求共享一个 RateController"""
        if "morningstar" in url:
            return self._morningstar
        return self._eastmoney

    def _get_endpoint_config(self, url: str) -> dict:
        """同域名不同接口的超时/重试差异化（搜索快、详情慢）"""
        if "morningstar" in url:
            if "quicktake" in url:
                return self._ENDPOINT_CONFIG["ms_quicktake"]
            return self._ENDPOINT_CONFIG["ms_search"]
        return self._ENDPOINT_CONFIG["eastmoney"]

    async def fetch(self, url: str, fund_code: str = "", phase: int = 0) -> str | None:
        # 空 URL 直接跳过（上游已判定不可请求）
        if not url:
            return None

        rc = self._get_rc(url)
        cfg = self._get_endpoint_config(url)
        domain_timeout = aiohttp.ClientTimeout(total=cfg["timeout"])
        domain_max_retries = cfg["max_retries"]

        result: str | None = None
        success = False

        for attempt in range(domain_max_retries):
            await rc.acquire(priority=1 if phase == 2 else 0)

            try:
                headers = {**self._BASE_HEADERS, "User-Agent": singleton_fake_ua.get_random_ua()}
                async with self._session.get(url, headers=headers, timeout=domain_timeout) as resp:
                    if resp.status == 200:
                        text = await resp.text()  # type: ignore[no-any-return]
                        if text:
                            result = text
                            success = True
                            break
                    raise ValueError(f"status={resp.status} or empty")
            except Exception:
                if attempt < domain_max_retries - 1:
                    await asyncio.sleep(self._retry_backoff ** attempt)
            finally:
                await rc.release()

        # 只按最终结果调整速率, 避免重试过程误伤
        rc.record(success=success)

        return result
