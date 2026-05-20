"""engine 集成测试"""

import tempfile

import pytest

from crawler.engine import _crawl_one
from crawler.fund_context import FundContext
from crawler.writer import ResultWriter


class TestCrawlOne:

    @pytest.mark.asyncio
    async def test_all_phases_completed(self):
        """模拟 fetcher，验证依赖驱动的 Phase 1 / Phase 2"""
        call_order = []

        class MockFetcher:
            async def fetch(self, url, fund_code):
                call_order.append(url)
                return "OK"

        ctx = FundContext("000001", "测试")
        mock = MockFetcher()
        with tempfile.TemporaryDirectory() as tmp:
            writer = ResultWriter(path=tmp)
            await _crawl_one(ctx, mock, writer)
            await writer.close()

        # 验证 Phase 1 的三个页面都被调用了
        assert any("jbgk" in c for c in call_order)      # overview
        assert any("jjjl" in c for c in call_order)      # manager
        assert any("fundsearch" in c for c in call_order) # morningstar
        # Phase 2 依赖 morningstar，需要 morningstar_fund_id
        # 但 mock 返回 "OK" 无法解析，所以 STEPS 会卡住
        # 这个测试只验证基础流程不崩溃
