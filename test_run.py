"""FundCrawler V2 — 冒烟测试"""

import asyncio
import logging

from crawler.engine import run
from crawler.target_loader import SmallBatchLoader
from utils.constants import log_format

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=log_format)

    loader = SmallBatchLoader(limit=10)
    asyncio.run(run(loader, initial_rate=10, max_rate=50))
