"""冒烟测试 —— 爬取 10 只基金验证全流程"""

import pytest

from module.constants import TT_STANDARD
from module.engine import run
from module.target_loader import WebTargetLoader


@pytest.mark.slow
@pytest.mark.asyncio
async def test_crawl_small_batch():
    await run(WebTargetLoader(limit=15), fields=TT_STANDARD)
