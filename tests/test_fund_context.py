"""FundContext 单元测试"""

from crawler.fund_context import FundContext
from utils.constants import FundAttrKey


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

    def test_to_result_row_contains_all_keys(self):
        ctx = FundContext("000001", "测试基金")
        row = ctx.to_result_row()
        assert row[FundAttrKey.FUND_CODE] == "000001"
        assert row[FundAttrKey.FUND_SIMPLE_NAME] == "测试基金"
        # 未设置的字段值为 None
        assert row[FundAttrKey.FUND_TYPE] is None

    def test_to_result_row_reflects_updated_fields(self):
        ctx = FundContext("000001", "测试基金")
        ctx.fund_type = "债券型"
        ctx.fund_manager = "张三"
        row = ctx.to_result_row()
        assert row[FundAttrKey.FUND_TYPE] == "债券型"
        assert row[FundAttrKey.FUND_MANAGER] == "张三"
