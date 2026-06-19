"""FundCrawler V2 — 入口

用法:
    # 全量爬取（默认）
    python run.py

    # 按需爬取：只爬东方财富字段，跳过晨星
    修改下方 fields 变量即可，示例见注释。
"""

import asyncio
import logging

from module.constants import FundAttrKey as K, log_format
from module.engine import run
from module.target_loader import WebTargetLoader

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=log_format)

    # fields = None → 全量爬取
    # fields = frozenset({K.FUND_TYPE, K.FUND_SIZE, K.FUND_COMPANY, ...}) → 按需爬取
    fields = None

    asyncio.run(run(WebTargetLoader(), fields=fields))
