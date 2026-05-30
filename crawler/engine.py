"""爬虫引擎 —— 组装各模块，驱动爬取流程"""

import asyncio
import logging

from crawler.fetcher import Fetcher
from crawler.fund_context import FundContext
from crawler.parsers import STEPS, Step
from crawler.writer import ResultWriter

logger = logging.getLogger(__name__)


async def run(target_loader,  # 鸭子类型：async get_fund_list() → list[FundContext]
              writer: ResultWriter | None = None) -> None:
    if writer is None:
        writer = ResultWriter()

    async with Fetcher() as fetcher:
        fund_list = await target_loader.get_fund_list()
        logger.info(f"共 {len(fund_list)} 只基金待爬取")

        tasks = [asyncio.create_task(_crawl_one(fund, fetcher, writer))
                 for fund in fund_list]
        await asyncio.gather(*tasks)

    await writer.close()
    logger.info("爬取完成")


async def _crawl_one(ctx: FundContext, fetcher: Fetcher, writer: ResultWriter) -> None:
    """爬取单只基金：根据 STEPS 依赖声明，自动分组并发"""
    completed: set[str] = set()

    while True:
        ready: list[Step] = [s for s in STEPS
                             if s.name not in completed
                             and all(d in completed for d in s.deps)]

        if not ready:
            break

        urls = [s.build_url(ctx) for s in ready]
        results = await asyncio.gather(
            *[fetcher.fetch(url, ctx.fund_code) for url in urls]
        )
        for step, raw in zip(ready, results):
            try:
                step.parse(raw, ctx)
            except Exception:
                logger.exception(f"{ctx.fund_code} {step.name} 解析失败")
            completed.add(step.name)

    await writer.write(ctx)
