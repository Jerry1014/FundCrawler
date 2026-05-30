"""engine 集成测试"""

import tempfile

import pytest

from crawler.engine import _crawl_one
from crawler.fund_context import FundContext
from crawler.writer import ResultWriter


class MockFetcher:
    async def fetch(self, url, fund_code, phase=0):
        return "OK"


class TestCrawlOne:

    @pytest.mark.asyncio
    async def test_all_phases_completed(self):
        ctx = FundContext("000001", "测试")
        with tempfile.TemporaryDirectory() as tmp:
            writer = ResultWriter(path=tmp)
            await _crawl_one(ctx, MockFetcher(), writer)
            await writer.close()
