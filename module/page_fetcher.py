"""HTTP 客户端 + 自适应速率控制 + 重试"""

import asyncio
import logging
from dataclasses import dataclass

import aiohttp
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Endpoint:
    """域名级配置：速率控制器键 + 超时 + 最大尝试次数（None = 无限）"""
    rc_key: str
    timeout: int
    max_attempts: int | None


class RateController:
    """自适应速率控制 + 信号量 —— AIMD。

    双阈值降速:
    - fail_rate >  50% → ×0.5  (重度失败，快速退让)
    - fail_rate >= 20% → ×0.75 (轻度失败，温和退让)
    - fail_rate <  20% → +step (线性爬升)
    """

    _FAIL_THRESHOLD = 0.2
    _HEAVY_FAIL_THRESHOLD = 0.5

    def __init__(self, initial_rate: int = 1, max_rate: int = 100,
                 min_rate: int = 1, refresh_interval: float = 0.5,
                 increase_step: int = 1):
        self._cur_rate = float(initial_rate)
        self._max_rate = max_rate
        self._min_rate = min_rate
        self._refresh_interval = refresh_interval
        self._increase_step = increase_step
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
            elif self._available > self._permits:
                self._available = self._permits

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
        if fail_rate >= self._HEAVY_FAIL_THRESHOLD:
            self._cur_rate = max(self._min_rate, self._cur_rate * 0.5)
        elif fail_rate >= self._FAIL_THRESHOLD:
            self._cur_rate = max(self._min_rate, self._cur_rate * 0.75)
        elif total > 0:
            self._cur_rate = min(self._max_rate, self._cur_rate + self._increase_step)

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

    两个域名级 RateController 管控所有请求（含重试）的并发，AIMD 自动收敛到各域名安全上限。
    RC 基于请求维度：每次 HTTP 尝试（含重试）都要 acquire/release，以匹配网站视角的真实 QPS。
    """

    _BASE_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    _ENDPOINTS: dict[str, _Endpoint] = {
        "morningstar.cn": _Endpoint("morningstar", timeout=3, max_attempts=3),
        "eastmoney.com":   _Endpoint("tiantian",   timeout=3, max_attempts=3),
    }

    def __init__(self, retry_backoff: float = 1.0, max_backoff: float = 30.0):
        rc_configs = {
            "tiantian":   dict(initial_rate=20, max_rate=200),
            "morningstar": dict(initial_rate=8, min_rate=1, max_rate=200,
                                refresh_interval=1.0, increase_step=3),
        }
        self._rc = {name: RateController(**cfg) for name, cfg in rc_configs.items()}
        self._retry_backoff = retry_backoff
        self._max_backoff = max_backoff
        self._ua = UserAgent()
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "Fetcher":
        for rc in self._rc.values():
            await rc.start()
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0, limit_per_host=50),
        )
        return self

    async def __aexit__(self, *args) -> None:
        for rc in self._rc.values():
            rc.stop()
        if self._session:
            await self._session.close()

    # ── 路由 ──

    def _resolve(self, url: str) -> _Endpoint:
        for domain, ep in self._ENDPOINTS.items():
            if domain in url:
                return ep
        raise ValueError(f"Unknown host, no endpoint config for URL: {url}")

    # ── 请求 ──

    async def _request(self, url: str, timeout: int) -> str:
        """单次 HTTP GET，失败时抛出异常。"""
        headers = {**self._BASE_HEADERS, "User-Agent": self._ua.random}
        req_timeout = aiohttp.ClientTimeout(total=timeout)
        async with self._session.get(url, headers=headers, timeout=req_timeout) as resp:
            if resp.status != 200:
                raise ValueError(f"HTTP {resp.status}")
            text = await resp.text()
            if not text:
                raise ValueError("empty response body")
            return text

    async def fetch(self, url: str, phase: int = 0) -> str | None:
        if not url:
            return None

        ep = self._resolve(url)
        rc = self._rc[ep.rc_key]
        priority = 1 if phase == 2 else 0

        attempt = 0
        while ep.max_attempts is None or attempt < ep.max_attempts:
            if attempt > 0:
                delay = min(self._retry_backoff * (2 ** (attempt - 1)), self._max_backoff)
                await asyncio.sleep(delay)

            await rc.acquire(priority=priority)
            try:
                text = await self._request(url, ep.timeout)
                rc.record(success=True)
                return text
            except Exception:
                rc.record(success=False)
            finally:
                await rc.release()

            attempt += 1

        return None
