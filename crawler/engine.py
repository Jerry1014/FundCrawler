"""爬虫引擎 —— 组装各模块，驱动爬取流程"""

import asyncio
import logging

import tqdm

from crawler.fetcher import Fetcher
from crawler.fund_context import FundContext
from crawler.parsers import STEPS, Step
from crawler.writer import ResultWriter

logger = logging.getLogger(__name__)

_PIPELINE_SLOTS = 20


async def run(target_loader,  # 鸭子类型：async get_fund_list() → list[FundContext]
              writer: ResultWriter | None = None) -> None:
    if writer is None:
        writer = ResultWriter()

    logger.info("正在获取基金列表 …")
    fund_list = await target_loader.get_fund_list()
    total = len(fund_list)
    logger.info(f"共 {total} 只基金待爬取")

    fund_sem = asyncio.Semaphore(_PIPELINE_SLOTS)

    async def _crawl_with_limit(ctx: FundContext) -> None:
        async with fund_sem:
            await _crawl_one(ctx, fetcher, writer)

    async with Fetcher() as fetcher:
        tasks = [asyncio.create_task(_crawl_with_limit(fund))
                 for fund in fund_list]

        for coro in tqdm.tqdm(asyncio.as_completed(tasks),
                               total=total, desc="爬取进度", unit="只"):
            await coro

    await writer.close()
    logger.info("爬取完成")


async def _crawl_one(ctx: FundContext, fetcher: Fetcher, writer: ResultWriter) -> None:
    completed: set[str] = set()
    phase = 0

    while True:
        ready: list[Step] = [s for s in STEPS
                             if s.name not in completed
                             and all(d in completed for d in s.deps)]

        if not ready:
            break

        phase += 1
        urls = [s.build_url(ctx) for s in ready]
        results = await asyncio.gather(
            *[fetcher.fetch(url, ctx.fund_code, phase=phase)
              for url in urls]
        )

        for step, raw in zip(ready, results):
            try:
                step.parse(raw, ctx)
            except Exception:
                logger.exception(f"{ctx.fund_code} {step.name} 解析失败")
            completed.add(step.name)

    await writer.write(ctx)
