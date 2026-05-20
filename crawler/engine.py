"""爬虫引擎 —— 组装各模块，驱动爬取流程"""

import asyncio
import logging

from crawler.fetcher import RateController, Fetcher
from crawler.fund_context import FundContext
from crawler.parsers import STEPS
from crawler.writer import ResultWriter

logger = logging.getLogger(__name__)


async def run(target_loader, writer: ResultWriter | None = None,
              initial_rate: int = 10, max_rate: int = 50) -> None:
    """
    主入口：获取基金列表 → 并发爬取每只基金 → 写结果。

    Args:
        target_loader: 具有 async get_fund_list() → list[FundContext] 的对象
        writer: 结果输出器，默认创建 CSV writer
        initial_rate: 初始并发数
        max_rate: 最大并发数
    """
    if writer is None:
        writer = ResultWriter()

    rc = RateController(initial_rate=initial_rate, max_rate=max_rate)
    await rc.start()

    async with Fetcher(rc) as fetcher:
        fund_list = await target_loader.get_fund_list()
        logger.info(f"共 {len(fund_list)} 只基金待爬取")

        tasks = [asyncio.create_task(_crawl_one(fund, fetcher, writer))
                 for fund in fund_list]
        await asyncio.gather(*tasks)

    rc.stop()
    await writer.close()
    logger.info("爬取完成")


async def _crawl_one(ctx: FundContext, fetcher: Fetcher, writer: ResultWriter) -> None:
    """爬取单只基金：根据 STEPS 依赖声明，自动分组并发"""
    completed: set[str] = set()

    while True:
        ready = [(name, s) for name, s in STEPS.items()
                 if name not in completed
                 and all(d in completed for d in s["deps"])]

        if not ready:
            break

        urls = [step["build_url"](ctx) for _, step in ready]
        results = await asyncio.gather(
            *[fetcher.fetch(url, ctx.fund_code) for url in urls]
        )
        for (name, step), raw in zip(ready, results):
            try:
                step["parse"](raw, ctx)
            except Exception:
                logger.exception(f"{ctx.fund_code} {name} 解析失败")
            completed.add(name)

    await writer.write(ctx)
