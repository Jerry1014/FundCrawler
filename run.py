"""FundCrawler V2 — 入口"""

import asyncio
import logging

from crawler.engine import run
from crawler.target_loader import SmallBatchLoader
from utils.constants import log_format

if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN, format=log_format)

    # 测试模式：爬取少量基金
    loader = SmallBatchLoader(limit=10)
    # 全量模式：WebTargetLoader()
    # 断点续传：RetryTargetLoader(WebTargetLoader())
    # 指定基金：StaticTargetLoader([("000001", "测试")])

    asyncio.run(run(loader, initial_rate=10, max_rate=50))
