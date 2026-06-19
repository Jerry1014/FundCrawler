"""FundCrawler V2 — 入口

默认爬取天天基金标准数据。修改 fields 切换数据范围:
    TT_BASIC    — 仅基本概况 + 基金经理
    TT_STANDARD — 基本数据 + 特色数据（默认）
    TT_MS_FULL  — 天天基金 + 晨星（慢，适合少量基金）
"""

import asyncio
import logging

from module.constants import log_format, TT_STANDARD
from module.engine import run
from module.target_loader import WebTargetLoader

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=log_format)

    fields = TT_STANDARD

    asyncio.run(run(WebTargetLoader(), fields=fields))
