"""爬虫引擎 —— 组装各模块，驱动爬取流程"""

import asyncio
import logging

import tqdm

from crawler.fetcher import Fetcher
from crawler.fund_context import FundContext
from crawler.parsers import STEPS, Step
from crawler.writer import ResultWriter

logger = logging.getLogger(__name__)


async def run(target_loader,  # 鸭子类型：async get_fund_list() → list[FundContext]
              writer: ResultWriter | None = None) -> None:
    if writer is None:
        writer = ResultWriter()

    logger.info("正在获取基金列表 …")
    fund_list = await target_loader.get_fund_list()
    logger.info(f"共 {len(fund_list)} 只基金待爬取")

    async with Fetcher() as fetcher:
        tasks = [asyncio.create_task(_crawl_one(fund, fetcher, writer))
                 for fund in fund_list]
        for coro in tqdm.tqdm(asyncio.as_completed(tasks),
                               total=len(tasks), desc="爬取进度", unit="只"):
            await coro

    await writer.close()
    logger.info("爬取完成")


async def _crawl_one(ctx: FundContext, fetcher: Fetcher, writer: ResultWriter) -> None:
    """爬取单只基金——槽位在基金级别，内部请求自由并发"""
    await fetcher.acquire()
    try:
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
    finally:
        await fetcher.release()
