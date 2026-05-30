"""FundCrawler V2 — 测试：只爬几只基金验证流程"""

import asyncio
import logging

from crawler.engine import run
from crawler.target_loader import SmallBatchLoader
from utils.constants import log_format

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=log_format)
    asyncio.run(run(SmallBatchLoader(limit=10)))
