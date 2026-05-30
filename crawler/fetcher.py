"""HTTP 客户端 + 自适应速率控制 + 重试"""

import asyncio

import aiohttp

from utils.fake_ua_getter import singleton_fake_ua


class RateController:
    """
    自适应速率控制 —— AIMD（加法增、乘法减）。

    借鉴 TCP 拥塞控制：失败率 ≥ 10% 时并发减半，否则每次评估 +1。
    """

    _FAIL_THRESHOLD = 0.1

    def __init__(self, initial_rate: int = 10, max_rate: int = 50,
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

    # ── 信号量接口 ────────────────────────────────────────

    async def acquire(self) -> None:
        async with self._cond:
            while self._available <= 0:
                await self._cond.wait()
            self._available -= 1

    async def release(self) -> None:
        async with self._cond:
            self._available += 1
            self._cond.notify(1)

    async def _resize(self, new_permits: int) -> None:
        async with self._cond:
            delta = new_permits - self._permits
            self._permits = new_permits
            if delta > 0:
                self._available += delta
                self._cond.notify(delta)

    # ── 统计接口 ──────────────────────────────────────────

    def record(self, success: bool) -> None:
        if success:
            self._success += 1
        else:
            self._fail += 1

    @property
    def cur_rate(self) -> float:
        return self._cur_rate

    # ── 自适应循环 ────────────────────────────────────────

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

        if fail_rate >= self._FAIL_THRESHOLD:
            self._cur_rate = max(self._min_rate, self._cur_rate * 0.5)
        elif total > 0:
            self._cur_rate = min(self._max_rate, self._cur_rate + 1)

        self._success = 0
        self._fail = 0
        await self._resize(int(self._cur_rate))


class Fetcher:
    """带限流、重试、UA 轮换的异步 HTTP 客户端"""

    def __init__(self, timeout: float = 10, max_retries: int = 3,
                 retry_backoff: float = 1.5):
        self._rc = RateController()
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "Fetcher":
        await self._rc.start()
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            connector=aiohttp.TCPConnector(limit=0),
        )
        return self

    async def __aexit__(self, *args) -> None:
        self._rc.stop()
        if self._session:
            await self._session.close()
            self._session = None

    def acquire(self) -> "asyncio.CoroutineType":
        """占用一个基金级并发槽位"""
        return self._rc.acquire()

    def release(self) -> "asyncio.CoroutineType":
        return self._rc.release()

    async def fetch(self, url: str, fund_code: str) -> str | None:
        """发起单次 HTTP GET（不占槽位，槽位在基金级别控制）。
        带指数退避重试，成功返回响应文本，失败返回 None。
        """
        for attempt in range(self._max_retries):
            try:
                headers = {"User-Agent": singleton_fake_ua.get_random_ua()}
                async with self._session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        text = await resp.text()  # type: ignore[no-any-return]
                        if text:
                            self._rc.record(success=True)
                            return text
                    raise ValueError(f"status={resp.status} or empty")
            except Exception:
                self._rc.record(success=False)
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_backoff ** attempt)
        return None
