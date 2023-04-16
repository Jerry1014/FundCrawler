import asyncio
import time
from typing import NoReturn
from unittest import TestCase

from manager.task_manager import NeedCrawledFundModule, CrawlingDataModule, FundCrawlingResult, SaveResultModule, \
    TaskManager


class TestNeedCrawledFundModule(NeedCrawledFundModule):

    def init(self):
        self.total = 2
        self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(str(i), str(i)) for i in range(2))


class TestCrawlingDataModule(CrawlingDataModule):
    def __init__(self):
        self._task_list = list()
        self._result_list = list()

    async def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund):
        print(f"do_crawling {task} start")
        while len(self._task_list) > 0:
            await asyncio.sleep(0.1)
        print(f"do_crawling {task} end")
        self._task_list.append(task)

        self._result_list.append(FundCrawlingResult({FundCrawlingResult.FundInfoHeader.FUND_NAME: task.name}))

    def is_end(self) -> bool:
        return len(self._task_list) == 0 and len(self._result_list) == 0

    async def get_an_result(self) -> FundCrawlingResult:
        print("get_an_result start")
        while len(self._result_list) == 0:
            await asyncio.sleep(0.1)
        result = self._result_list.pop()
        print(f"get_an_result {result} end")
        return result


class TestSaveResultModule(SaveResultModule):

    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        print(result)


class TestTaskManager(TestCase):
    manager = TaskManager(TestNeedCrawledFundModule(), TestCrawlingDataModule(), TestSaveResultModule())
    asyncio.run(manager.run())
