"""
负责统领和协调数据爬取的流程
"""
from threading import Thread
from time import sleep
from typing import NoReturn

from tqdm import tqdm

from module.abstract_crawling_data_module import CrawlingDataModule
from module.abstract_crawling_target_module import CrawlingTargetModule
from module.abstract_saving_result_module import SavingResultModule
from module.fund_info_bo import NeedCrawledOnceFund


class TaskManager:
    """
    爬取核心
    """

    def __init__(self, need_crawled_fund_module: CrawlingTargetModule, crawling_data_module: CrawlingDataModule,
                 save_result_module: SavingResultModule):
        """
        :param need_crawled_fund_module: 负责给出 基金爬取任务
        :param crawling_data_module: 负责 数据爬取和清洗
        :param save_result_module: 负责 数据保存
        """
        self._need_crawled_fund_module = need_crawled_fund_module
        self._crawling_data_module = crawling_data_module
        self._save_result_module = save_result_module

        self._finished_task_count = 0
        self._total_task_count = self._need_crawled_fund_module.total
        self._all_task_finished = False

    def get_task_and_crawling(self):
        generator = self._need_crawled_fund_module.task_generator

        while True:
            try:
                task: NeedCrawledOnceFund = next(generator)
            except StopIteration:
                break
            self._crawling_data_module.do_crawling(task)

        self._crawling_data_module.shutdown()

    def get_result_and_save(self):
        with self._save_result_module:
            while self._crawling_data_module.has_next_result():
                result: FundCrawlingResult = self._crawling_data_module.get_an_result()
                if result:
                    self._save_result_module.save_result(result)
                    self._finished_task_count += 1

        self._all_task_finished = True

    def show_process(self):
        with tqdm(total=self._total_task_count) as pbar:
            last_count = None
            while not self._all_task_finished:
                pbar.update(self._finished_task_count - (last_count if last_count else 0))
                last_count = self._finished_task_count
                sleep(1)

    def run(self) -> NoReturn:
        """
        爬取主流程
        从 基金爬取任务模块 将任务传递给 数据爬取和清洗模块
        从 数据爬取和清洗模块 将结果传递给 数据保存模块
        两部分的任务都是阻塞的（主要会阻塞在 数据爬取和清洗）
        """
        thread1 = Thread(target=self.get_task_and_crawling)
        thread2 = Thread(target=self.get_result_and_save)
        thread3 = Thread(target=self.show_process)

        thread1.start()
        thread2.start()
        thread3.start()

        thread1.join()
        thread2.join()
        thread3.join()
