"""FundCrawler V2 — 入口"""

import asyncio
import logging

from crawler.engine import run
from crawler.target_loader import WebTargetLoader
from utils.constants import log_format

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=log_format)
    asyncio.run(run(WebTargetLoader()))
