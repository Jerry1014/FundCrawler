"""HTTP 客户端 + 自适应速率控制 + 重试"""

import asyncio

import aiohttp

from utils.fake_ua_getter import singleton_fake_ua


# ═══════════════════════════════════════════════════════════════
# 可调并发数的异步信号量
# ═══════════════════════════════════════════════════════════════

class _ResizableSemaphore:
    """支持动态调整并发上限的异步信号量"""

    def __init__(self, permits: int):
        self._permits = permits
        self._available = permits
        self._cond = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._cond:
            while self._available <= 0:
                await self._cond.wait()
            self._available -= 1

    async def release(self) -> None:
        async with self._cond:
            self._available += 1
            self._cond.notify(1)

    async def resize(self, new_permits: int) -> None:
        """调整并发上限，delta > 0 时立即释放对应许可"""
        async with self._cond:
            delta = new_permits - self._permits
            self._permits = new_permits
            if delta > 0:
                self._available += delta
                self._cond.notify(delta)


# ═══════════════════════════════════════════════════════════════
# 自适应速率控制器
# ═══════════════════════════════════════════════════════════════

class RateController:
    """
    自适应速率控制：在线探测失败率，动态调节并发上限。

    算法（继承原 RateControl）：
    - 每 refresh_interval 秒评估一次
    - 失败率 > 0：cur -= fail_rate * change_factor
    - 失败率 = 0：cur += change_factor
    - change_factor 随迭代递减：(1100 - iter) / 100（最低 1）
    - cur 限定在 [min_rate, max_rate]
    """

    def __init__(self, initial_rate: int = 10, max_rate: int = 50,
                 min_rate: int = 1, refresh_interval: float = 0.5):
        self._cur_rate = float(initial_rate)
        self._max_rate = max_rate
        self._min_rate = min_rate
        self._refresh_interval = refresh_interval

        self._success = 0
        self._fail = 0
        self._iteration = 0

        self._sem = _ResizableSemaphore(initial_rate)
        self._running = False

    # ── 对外接口 ──────────────────────────────────────────

    async def acquire(self) -> None:
        await self._sem.acquire()

    async def release(self) -> None:
        await self._sem.release()

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
        """启动后台调整循环"""
        self._running = True
        asyncio.create_task(self._adjust_loop())

    def stop(self) -> None:
        self._running = False

    async def _adjust_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._refresh_interval)
            await self._adjust()

    async def _adjust(self) -> None:
        """核心算法：根据当前窗口的失败率调整并发上限"""
        total = self._success + self._fail
        fail_rate = self._fail / total if total > 0 else 1.0
        self._iteration += 1

        change_factor = max(1.0, (1100 - self._iteration) / 100)

        if fail_rate > 0.0:
            self._cur_rate = max(self._min_rate,
                                 self._cur_rate - fail_rate * change_factor)
        else:
            self._cur_rate = min(self._max_rate,
                                 self._cur_rate + change_factor)

        # 重置窗口
        self._success = 0
        self._fail = 0

        # 生效新并发上限
        await self._sem.resize(int(self._cur_rate))


# ═══════════════════════════════════════════════════════════════
# 异步 HTTP 客户端
# ═══════════════════════════════════════════════════════════════

class Fetcher:
    """带限流、重试、UA 轮换的异步 HTTP 客户端"""

    def __init__(self, rate_controller: RateController,
                 timeout: float = 10, max_retries: int = 3,
                 retry_backoff: float = 1.5):
        self._rc = rate_controller
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "Fetcher":
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            connector=aiohttp.TCPConnector(limit=0),
        )
        return self

    async def __aexit__(self, *args) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def fetch(self, url: str, fund_code: str) -> str | None:
        """
        发起单次 HTTP GET，带全局限流和指数退避重试。
        成功返回响应文本，失败返回 None。
        """
        for attempt in range(self._max_retries):
            await self._rc.acquire()
            try:
                headers = {"User-Agent": singleton_fake_ua.get_random_ua()}
                async with self._session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if text:
                            self._rc.record(success=True)
                            return text
                    raise ValueError(f"status={resp.status} or empty")
            except Exception:
                self._rc.record(success=False)
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_backoff ** attempt)
            finally:
                await self._rc.release()
        return None
