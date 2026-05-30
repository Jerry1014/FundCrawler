"""冒烟测试 —— 爬取 10 只基金验证全流程"""

import pytest

from crawler.engine import run
from crawler.target_loader import SmallBatchLoader


@pytest.mark.slow
@pytest.mark.asyncio
async def test_crawl_small_batch():
    loader = SmallBatchLoader(limit=10)
    await run(loader)
