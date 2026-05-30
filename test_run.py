"""冒烟测试 —— 爬取 500 只基金验证全流程"""

import pytest

from crawler.engine import run
from crawler.target_loader import SmallBatchLoader


@pytest.mark.slow
@pytest.mark.asyncio
async def test_crawl_small_batch():
    await run(SmallBatchLoader(limit=500))
