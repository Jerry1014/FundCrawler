import asyncio
import time
from typing import NoReturn, Optional
from unittest import TestCase

from manager.task_manager import NeedCrawledFundModule, CrawlingDataModule, FundCrawlingResult, SaveResultModule, \
    TaskManager


class TestNeedCrawledFundModule(NeedCrawledFundModule):

    def init(self):
        self.total = 2
        self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(str(i), str(i)) for i in range(2))


class TestCrawlingDataModule(CrawlingDataModule):
    def __init__(self):
        self._task_list: list[NeedCrawledFundModule.NeedCrawledOnceFund] = list()
        self._result_list: list[FundCrawlingResult] = list()
        self._is_end = False

    def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund):
        # 模拟从队列中取结果时的block和超时
        max_iteration = 10
        while len(self._task_list) > 0 and max_iteration > 0:
            time.sleep(0.1)
            max_iteration -= 1
        if max_iteration == 0:
            return

        self._task_list.append(task)

        task = self._task_list.pop()
        self._result_list.append(FundCrawlingResult({FundCrawlingResult.FundInfoHeader.FUND_NAME: task.name}))

    def is_end(self) -> bool:
        return len(self._task_list) == 0 and len(self._result_list) == 0 and self._is_end

    def get_an_result(self) -> Optional[FundCrawlingResult]:
        # 模拟从队列中取结果时的block和超时
        max_iteration = 10
        while len(self._result_list) == 0 and max_iteration > 0:
            time.sleep(0.1)
            max_iteration -= 1
        if max_iteration == 0:
            return None

        result = self._result_list.pop()
        return result


class TestSaveResultModule(SaveResultModule):

    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        if result:
            print(f'the result is {result.fund_info_dict}')


class TestTaskManager(TestCase):
    manager = TaskManager(TestNeedCrawledFundModule(), TestCrawlingDataModule(), TestSaveResultModule())
    asyncio.run(manager.run())
