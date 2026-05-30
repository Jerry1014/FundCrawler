"""engine 集成测试"""

import tempfile

import pytest

from crawler.engine import _crawl_one
from crawler.fund_context import FundContext
from crawler.writer import ResultWriter


class MockFetcher:
    async def acquire(self): pass
    async def release(self): pass

    async def fetch(self, url, fund_code):
        return "OK"


class TestCrawlOne:

    @pytest.mark.asyncio
    async def test_all_phases_completed(self):
        call_order = []
        mock = MockFetcher()
        mock.fetch = lambda url, fc: (call_order.append(url), "OK")[1]  # 仍需要 mock

        ctx = FundContext("000001", "测试")
        with tempfile.TemporaryDirectory() as tmp:
            writer = ResultWriter(path=tmp)
            await _crawl_one(ctx, MockFetcher(), writer)
            await writer.close()
