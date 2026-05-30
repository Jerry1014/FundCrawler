"""FundContext 单元测试"""

from crawler.fund_context import FundContext


class TestFundContext:
    def test_init_sets_code_and_name(self):
        ctx = FundContext("000001", "测试基金")
        assert ctx.fund_code == "000001"
        assert ctx.fund_name == "测试基金"

    def test_fields_default_to_none(self):
        ctx = FundContext("000001", "测试基金")
        assert ctx.fund_type is None
        assert ctx.fund_manager is None
        assert ctx.morningstar_fund_id is None

    def test_fields_mutable(self):
        ctx = FundContext("000001", "测试基金")
        ctx.fund_type = "债券型"
        assert ctx.fund_type == "债券型"
