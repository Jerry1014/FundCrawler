"""fetcher 单元测试 —— 速率控制算法 + 信号量并发"""

import asyncio

import pytest

from crawler.fetcher import RateController


class TestSignalSemantics:

    @pytest.mark.asyncio
    async def test_acquire_release(self):
        rc = RateController(initial_rate=2, max_rate=2)
        await rc.acquire()
        await rc.acquire()
        assert rc._available == 0
        await rc.release()
        assert rc._available == 1

    @pytest.mark.asyncio
    async def test_acquire_blocks_when_exhausted(self):
        rc = RateController(initial_rate=1, max_rate=1)
        await rc.acquire()
        acquired = False

        async def try_acquire():
            nonlocal acquired
            await rc.acquire()
            acquired = True

        asyncio.create_task(try_acquire())
        await asyncio.sleep(0.01)
        assert not acquired
        await rc.release()
        await asyncio.sleep(0.01)
        assert acquired

    @pytest.mark.asyncio
    async def test_resize_increases_capacity(self):
        rc = RateController(initial_rate=1, max_rate=2)
        await rc.acquire()
        await rc._resize(2)
        await rc.acquire()
        await rc.release()
        await rc.acquire()
        assert rc._available == 0


class TestRateControllerAIMD:

    def test_initial_rate(self):
        rc = RateController(initial_rate=10)
        assert rc.cur_rate == 10.0

    @pytest.mark.asyncio
    async def test_no_data_idle(self):
        rc = RateController(initial_rate=10)
        await rc._adjust()
        assert rc.cur_rate == 10.0

    @pytest.mark.asyncio
    async def test_success_adds_one(self):
        rc = RateController(initial_rate=10)
        for _ in range(5):
            rc.record(success=True)
        await rc._adjust()
        assert rc.cur_rate == 11.0

    @pytest.mark.asyncio
    async def test_high_failure_decreases(self):
        rc = RateController(initial_rate=10)
        for _ in range(8):
            rc.record(success=True)
        for _ in range(2):
            rc.record(success=False)
        await rc._adjust()
        assert rc.cur_rate == 7.5  # ×0.75 (温和降速)

    @pytest.mark.asyncio
    async def test_below_threshold_ignored(self):
        rc = RateController(initial_rate=10)
        for _ in range(19):
            rc.record(success=True)
        rc.record(success=False)
        await rc._adjust()
        assert rc.cur_rate == 11.0

    @pytest.mark.asyncio
    async def test_respects_min_rate(self):
        rc = RateController(initial_rate=10, min_rate=3)
        for _ in range(100):
            rc.record(success=False)
        await rc._adjust()
        assert rc.cur_rate == 7.5  # 10 × 0.75 = 7.5 > min=3

    @pytest.mark.asyncio
    async def test_respects_max_rate(self):
        rc = RateController(initial_rate=10, max_rate=15)
        for _ in range(100):
            rc.record(success=True)
        await rc._adjust()
        assert rc.cur_rate == 11.0

    @pytest.mark.asyncio
    async def test_window_reset_after_adjust(self):
        rc = RateController(initial_rate=10)
        for _ in range(5):
            rc.record(success=True)
        await rc._adjust()
        assert rc._success == 0
        assert rc._fail == 0
