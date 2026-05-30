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
    """带限流、重试、UA 轮换的异步 HTTP 客户端

    EastMoney:     10s 超时, 3 次重试, 初始并发 20（无流控顾虑）
    MS search:      8s 超时, 2 次重试, 初始并发  3（保守起步避反爬）
    MS quicktake:  12s 超时, 2 次重试, 初始并发  5（慢接口需保守）
    """

    # 模拟浏览器请求头（CloudFront WAF 校验 Accept / Accept-Language，缺则 403/超时）
    _BASE_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    # ── 按域名差异化配置 ──
    # Morningstar 保守起步，靠 AIMD 自然爬升；EastMoney 无此限制
    _DOMAIN_CONFIG: dict[str, dict] = {
        "eastmoney": {"timeout": 10, "max_retries": 3, "initial_rate": 20},
        "morningstar_search": {"timeout": 8, "max_retries": 2, "initial_rate": 3},
        "morningstar_quicktake": {"timeout": 12, "max_retries": 2, "initial_rate": 5},
    }

    def __init__(self, timeout: float = 10, max_retries: int = 3,
                 retry_backoff: float = 1.5):
        self._eastmoney = RateController(initial_rate=20)
        self._ms_search = RateController(initial_rate=3, min_rate=1,
                                         refresh_interval=1.0)
        self._ms_quicktake = RateController(initial_rate=5, min_rate=3,
                                            refresh_interval=1.0)
        self._default_timeout = aiohttp.ClientTimeout(total=timeout)
        self._default_max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "Fetcher":
        await self._eastmoney.start()
        await self._ms_search.start()
        await self._ms_quicktake.start()
        self._session = aiohttp.ClientSession(
            timeout=self._default_timeout,
            connector=aiohttp.TCPConnector(limit=0, limit_per_host=50),
        )
        return self

    async def __aexit__(self, *args) -> None:
        self._eastmoney.stop()
        self._ms_search.stop()
        self._ms_quicktake.stop()
        if self._session:
            await self._session.close()
            self._session = None

    def _get_rc(self, url: str, phase: int = 0) -> RateController:
        """根据 URL 和 phase 确定速率控制器"""
        if "morningstar" in url:
            if phase == 2:
                return self._ms_quicktake
            return self._ms_search
        return self._eastmoney

    def _get_domain_config(self, rc: RateController) -> dict:
        """获取域名的超时和重试配置"""
        if rc is self._eastmoney:
            return self._DOMAIN_CONFIG["eastmoney"]
        elif rc is self._ms_search:
            return self._DOMAIN_CONFIG["morningstar_search"]
        else:
            return self._DOMAIN_CONFIG["morningstar_quicktake"]

    async def fetch(self, url: str, fund_code: str, phase: int = 0) -> str | None:
        # 空 URL 直接跳过（上游已判定不可请求）
        if not url:
            return None

        rc = self._get_rc(url, phase)
        cfg = self._get_domain_config(rc)
        domain_timeout = aiohttp.ClientTimeout(total=cfg["timeout"])
        domain_max_retries = cfg["max_retries"]

        result: str | None = None
        success = False

        for attempt in range(domain_max_retries):
            await rc.acquire()

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
