"""FundCrawler V2 — 入口"""

import asyncio
import logging

from module.constants import log_format
from module.engine import run
from module.target_loader import WebTargetLoader

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=log_format)
    asyncio.run(run(WebTargetLoader()))
