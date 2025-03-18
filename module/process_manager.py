"""
负责统领和协调数据爬取的流程
"""
from multiprocessing import Queue, Event
from os import cpu_count
from threading import Thread
from time import sleep
from typing import List
from typing import NoReturn

from tqdm import tqdm

from module.abstract_crawling_data_module import CrawlingDataModule
from module.abstract_crawling_target_module import CrawlingTargetModule
from module.abstract_saving_result_module import SavingResultModule
from module.downloader.download_by_requests import Request, Response
from module.downloader.download_by_requests_v2 import GetPageByMultiThreadingV2
from module.fund_info_bo import NeedCrawledOnceFund, FundCrawlingResult


class TaskManager:
    """
    爬取的核心流程
    """

    def __init__(self, need_crawled_fund_module: CrawlingTargetModule, crawling_data_module: CrawlingDataModule,
                 save_result_module: SavingResultModule):
        # 事件列表
        self._http_request_queue: Queue[Request] = Queue(cpu_count())
        self._http_response_queue: Queue[Response] = Queue()
        self._exit_sign: Event = Event()
        self._result_save_queue: List[FundCrawlingResult] = list()

        # 相关模块
        self._need_crawled_fund_module = need_crawled_fund_module
        self._crawling_data_module = crawling_data_module
        self._save_result_module = save_result_module
        self._downloader = GetPageByMultiThreadingV2(self._http_request_queue, self._http_response_queue,
                                                     self._exit_sign)

        # 总共的任务数量
        self._total_task_num = self._need_crawled_fund_module.total
        # 当前已经完成的任务数量
        self._finished_task_num = 0

    def show_process(self):
        """
        爬取进度提示
        """
        with tqdm(total=self._total_task_num) as pbar:
            last_finished_task_num = None
            while self._finished_task_num < self._total_task_num:
                cur_finished_task_num = self._finished_task_num
                pbar.update(cur_finished_task_num - (last_finished_task_num if last_finished_task_num else 0))
                last_finished_task_num = cur_finished_task_num
                sleep(1)

    def run(self) -> NoReturn:
        try:
            # 独立的爬取进程（避免GIL）
            self._downloader.start()

            # 独立的进度展示线程
            show_process_thread = Thread(target=self.show_process)
            show_process_thread.start()

            # 爬取主流程
            self.do_run()
        finally:
            # downloader是子进程，一定要shutdown
            self._http_request_queue.close()
            self._http_response_queue.close()
            self._exit_sign.set()

    def do_run(self):
        """
        http请求是异步的，为了提高并发度，这里略微借鉴redis的事件驱动机制（没有严格地实现每个事件的回调处理类）
        优先响应 http请求事件 其次 http返回事件（数据挖掘） 最后 结果保存
        """
        # todo 拆开成事件驱动
        generator = self._need_crawled_fund_module.task_generator
        while True:
            try:
                task: NeedCrawledOnceFund = next(generator)
            except StopIteration:
                break
            self._crawling_data_module.do_crawling(task)
        self._crawling_data_module.shutdown()
        with self._save_result_module:
            while self._crawling_data_module.has_next_result():
                result: FundCrawlingResult = self._crawling_data_module.get_an_result()
                if result:
                    self._save_result_module.save_result(result)
                    self._finished_task_num += 1
