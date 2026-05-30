"""target_loader 单元测试"""

import pytest

from crawler.target_loader import StaticTargetLoader


class TestStaticTargetLoader:

    @pytest.mark.asyncio
    async def test_returns_fund_contexts(self):
        loader = StaticTargetLoader([("000001", "测试基金A"), ("000002", "测试基金B")])
        funds = await loader.get_fund_list()
        assert len(funds) == 2
        assert funds[0].fund_code == "000001"
        assert funds[0].fund_name == "测试基金A"
        assert funds[1].fund_code == "000002"
        assert funds[1].fund_name == "测试基金B"

    @pytest.mark.asyncio
    async def test_empty_list(self):
        loader = StaticTargetLoader([])
        funds = await loader.get_fund_list()
        assert funds == []
