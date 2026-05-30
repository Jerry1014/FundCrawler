"""writer 单元测试"""

import tempfile
from pathlib import Path

import pytest

from crawler.fund_context import FundContext
from crawler.writer import ResultWriter


class TestResultWriter:

    @pytest.mark.asyncio
    async def test_write_and_close(self):
        with tempfile.TemporaryDirectory() as tmp:
            writer = ResultWriter(path=tmp, filename="test.csv")
            ctx = FundContext("000001", "测试基金")
            ctx.fund_type = "债券型"
            await writer.write(ctx)
            await writer.close()

            content = Path(tmp, "test.csv").read_text(encoding="utf-8")
            assert "基金代码" in content
            assert "000001" in content
            assert "债券型" in content

    @pytest.mark.asyncio
    async def test_empty_fields_become_data_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            writer = ResultWriter(path=tmp, filename="test.csv")
            ctx = FundContext("000001", "测试基金")
            await writer.write(ctx)
            await writer.close()

            content = Path(tmp, "test.csv").read_text(encoding="utf-8")
            assert "DATA_ERROR" in content
