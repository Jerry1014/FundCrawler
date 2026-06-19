"""爬虫引擎 —— 组装各模块，驱动爬取流程"""

import asyncio
import logging

import tqdm

from module.constants import FundAttrKey as K
from module.fund_context import FundContext
from module.page_fetcher import Fetcher
from module.page_parser import Step, resolve_steps
from module.result_writer import ResultWriter

logger = logging.getLogger(__name__)


async def run(target_loader,  # 鸭子类型：async get_fund_list() → list[FundContext]
              fields: frozenset[K] | None = None,
              writer: ResultWriter | None = None) -> None:
    steps = resolve_steps(fields)
    step_names = [s.name for s in steps]
    logger.info(f"爬取步骤: {step_names}")

    if writer is None:
        writer = ResultWriter(fields=fields)

    logger.info("正在获取基金列表 …")
    fund_list = await target_loader.get_fund_list()
    total = len(fund_list)
    logger.info(f"共 {total} 只基金待爬取")

    async with Fetcher() as fetcher:
        tasks = [asyncio.create_task(_crawl_one(fund, fetcher, writer, steps))
                 for fund in fund_list]

        pbar = tqdm.tqdm(total=total, unit="只",
                         bar_format="{desc}{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")

        async def _blink(pbar) -> None:
            try:
                on = True
                while True:
                    pbar.set_description(f"{'●' if on else ' '} 爬取进度 ")
                    on = not on
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                pass

        blink_task = asyncio.create_task(_blink(pbar))

        try:
            for coro in asyncio.as_completed(tasks):
                await coro
                pbar.update(1)
        finally:
            blink_task.cancel()
            await blink_task
            pbar.close()

    await writer.close()
    logger.info("爬取完成")


async def _crawl_one(ctx: FundContext, fetcher: Fetcher, writer: ResultWriter,
                     steps: list[Step]) -> None:
    completed: set[str] = set()
    phase = 0

    while True:
        ready_to_fetch: list[Step] = [s for s in steps
                                      if s.name not in completed
                                      and all(d in completed for d in s.deps)]

        if not ready_to_fetch:
            break

        phase += 1
        urls = [s.build_url(ctx) for s in ready_to_fetch]
        results = await asyncio.gather(
            *[fetcher.fetch(url, phase=phase) for url in urls]
        )

        for step, raw in zip(ready_to_fetch, results):
            try:
                step.parse(raw, ctx)
            except Exception:
                logger.exception(f"{ctx.fund_code} {step.name} 解析失败")
            completed.add(step.name)

    await writer.write(ctx)
