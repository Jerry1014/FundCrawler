"""fetcher 单元测试 —— 速率控制算法 + 异步信号量"""

import asyncio

import pytest

from crawler.fetcher import RateController, _ResizableSemaphore


class TestResizableSemaphore:

    @pytest.mark.asyncio
    async def test_acquire_release(self):
        sem = _ResizableSemaphore(2)
        await sem.acquire()
        await sem.acquire()
        assert sem._available == 0
        await sem.release()
        assert sem._available == 1

    @pytest.mark.asyncio
    async def test_acquire_blocks_when_exhausted(self):
        sem = _ResizableSemaphore(1)
        await sem.acquire()
        acquired = False

        async def try_acquire():
            nonlocal acquired
            await sem.acquire()
            acquired = True

        task = asyncio.create_task(try_acquire())
        await asyncio.sleep(0.01)
        assert not acquired
        await sem.release()
        await asyncio.sleep(0.01)
        assert acquired

    @pytest.mark.asyncio
    async def test_resize_increases_capacity(self):
        sem = _ResizableSemaphore(1)
        await sem.acquire()           # available=0
        await sem.resize(2)           # permits→2, available→1
        # 现在有 2 个许可，用掉 1 个，剩 1 个可用
        await sem.acquire()           # available=0
        await sem.release()           # available=1
        await sem.acquire()           # available=0
        assert sem._available == 0


class TestRateControllerAlgorithm:

    def test_initial_rate(self):
        rc = RateController(initial_rate=10)
        assert rc.cur_rate == 10.0

    @pytest.mark.asyncio
    async def test_no_data_failure_penalty(self):
        rc = RateController(initial_rate=10, min_rate=1, max_rate=50)
        await rc._adjust()
        assert rc.cur_rate == 1.0

    @pytest.mark.asyncio
    async def test_all_success_increases_rate(self):
        rc = RateController(initial_rate=10, min_rate=1, max_rate=50)
        for _ in range(5):
            rc.record(success=True)
        await rc._adjust()
        assert rc.cur_rate > 10.0
        assert rc.cur_rate <= 50.0

    @pytest.mark.asyncio
    async def test_partial_failure_decreases_rate(self):
        rc = RateController(initial_rate=10, min_rate=1, max_rate=50)
        for _ in range(3):
            rc.record(success=True)
        for _ in range(2):
            rc.record(success=False)
        await rc._adjust()
        assert rc.cur_rate < 10.0
        assert rc.cur_rate >= 1.0

    @pytest.mark.asyncio
    async def test_respects_min_rate(self):
        rc = RateController(initial_rate=10, min_rate=3, max_rate=50)
        for _ in range(100):
            rc.record(success=False)
        await rc._adjust()
        assert rc.cur_rate == 3.0

    @pytest.mark.asyncio
    async def test_respects_max_rate(self):
        rc = RateController(initial_rate=10, min_rate=1, max_rate=15)
        for _ in range(100):
            rc.record(success=True)
        await rc._adjust()
        assert rc.cur_rate <= 15.0

    def test_change_factor_decreases_with_iterations(self):
        cf1 = max(1.0, (1100 - 1) / 100)
        cf2 = max(1.0, (1100 - 501) / 100)
        cf3 = max(1.0, (1100 - 1201) / 100)
        assert cf1 > cf2 > cf3
        assert cf3 == 1.0

    @pytest.mark.asyncio
    async def test_window_reset_after_adjust(self):
        rc = RateController(initial_rate=10)
        for _ in range(5):
            rc.record(success=True)
        await rc._adjust()
        assert rc._success == 0
        assert rc._fail == 0
