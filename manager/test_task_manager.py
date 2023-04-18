import asyncio
import time
from typing import NoReturn, Optional
from unittest import TestCase

from manager.task_manager import NeedCrawledFundModule, CrawlingDataModule, FundCrawlingResult, SaveResultModule, \
    TaskManager


class TestNeedCrawledFundModule(NeedCrawledFundModule):

    def init_generator(self):
        self.total = 2
        self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(str(i), str(i)) for i in range(100))


class TestCrawlingDataModule(CrawlingDataModule):
    def __init__(self):
        self._task_list: list[NeedCrawledFundModule.NeedCrawledOnceFund] = list()
        self._result_list: list[FundCrawlingResult] = list()

    def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund):
        # 模拟从队列中取结果时的block
        while len(self._task_list) > 0:
            time.sleep(0.1)

        self._task_list.append(task)

        task = self._task_list.pop()
        self._result_list.append(FundCrawlingResult({FundCrawlingResult.FundInfoHeader.FUND_NAME: task.name}))

    def empty_request_and_result(self) -> bool:
        return len(self._task_list) == 0 and len(self._result_list) == 0

    def get_an_result(self) -> Optional[FundCrawlingResult]:
        # 模拟从队列中取结果时的block
        while len(self._result_list) == 0:
            time.sleep(0.1)

        result = self._result_list.pop()
        return result


class TestSaveResultModule(SaveResultModule):

    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        if result:
            print(f'the result is {result.fund_info_dict}')


class TestTaskManager(TestCase):
    def test_run(self):
        manager = TaskManager(TestNeedCrawledFundModule(), TestCrawlingDataModule(), TestSaveResultModule())
        asyncio.run(manager.run())
