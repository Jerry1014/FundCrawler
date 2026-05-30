"""冒烟测试 —— 爬取少量基金验证全流程"""

import pytest

from crawler.engine import run
from crawler.target_loader import SmallBatchLoader


@pytest.mark.slow
@pytest.mark.asyncio
async def test_crawl_small_batch():
    loader = SmallBatchLoader(limit=100)
    await run(loader)
