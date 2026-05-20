"""基金列表加载器 —— 拓展点：爬取哪些基金"""

import csv
import re
from pathlib import Path

import aiohttp

from crawler.fund_context import FundContext
from utils.fake_ua_getter import singleton_fake_ua


def _parse_fund_list(text: str) -> list[FundContext]:
    """从天天基金网响应文本中提取基金代码和名称"""
    items = re.findall(r'"[0-9]{6}",".+?"', text)
    return [FundContext(i[1:7], i[10:-1]) for i in items]


class StaticTargetLoader:
    """指定若干基金代码"""

    def __init__(self, funds: list[tuple[str, str]]):
        self._funds = funds

    async def get_fund_list(self) -> list[FundContext]:
        return [FundContext(code, name) for code, name in self._funds]


class WebTargetLoader:
    """从天天基金网拉取全量开放式基金列表"""

    URL = 'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?page=1,&onlySale=0'

    def __init__(self, session: aiohttp.ClientSession | None = None):
        self._session = session

    async def get_fund_list(self) -> list[FundContext]:
        if not self._session:
            async with aiohttp.ClientSession() as session:
                return await self._fetch(session)
        return await self._fetch(self._session)

    async def _fetch(self, session: aiohttp.ClientSession) -> list[FundContext]:
        headers = {"User-Agent": singleton_fake_ua.get_random_ua()}
        async with session.get(self.URL, headers=headers) as resp:
            return _parse_fund_list(await resp.text())


class SmallBatchLoader:
    """拉取指定数量基金（测试用）"""

    URL = 'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?page=1,{limit}&onlySale=0'

    def __init__(self, limit: int = 10, session: aiohttp.ClientSession | None = None):
        self._limit = limit
        self._session = session

    async def get_fund_list(self) -> list[FundContext]:
        url = self.URL.format(limit=self._limit)
        if not self._session:
            async with aiohttp.ClientSession() as session:
                return await self._fetch(session, url)
        return await self._fetch(self._session, url)

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> list[FundContext]:
        headers = {"User-Agent": singleton_fake_ua.get_random_ua()}
        async with session.get(url, headers=headers) as resp:
            return _parse_fund_list(await resp.text())


class RetryTargetLoader:
    """断点续传：跳过已有 CSV 记录"""

    CSV_PATH = Path("./result/result.csv")

    def __init__(self, inner: WebTargetLoader):
        self._inner = inner

    async def get_fund_list(self) -> list[FundContext]:
        all_funds = await self._inner.get_fund_list()
        fund_dict = {f.fund_code: f for f in all_funds}

        if not self.CSV_PATH.exists():
            return all_funds

        with open(self.CSV_PATH, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get('基金代码', '')
                fund_dict.pop(code, None)

        return list(fund_dict.values())
