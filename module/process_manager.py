"""
负责统领和协调数据爬取的流程
"""
import logging
from multiprocessing import Queue, Event
from os import cpu_count
from threading import Thread
from time import sleep
from typing import List
from typing import NoReturn

from tqdm import tqdm

from module.abstract_crawling_target_module import CrawlingTargetModule
from module.abstract_data_mining_module import DataMiningModule
from module.abstract_saving_result_module import SavingResultModule
from module.downloader.download_by_requests import FundRequest, FundResponse, GetPageByMultiThreading
from module.fund_context import FundContext
from utils.constants import PageType


class TaskManager:
    """
    爬取的核心流程
    """

    def __init__(self, need_crawled_fund_module: CrawlingTargetModule, data_mining_module: DataMiningModule,
                 save_result_module: SavingResultModule):
        # 事件列表等(模块间的协作)
        self._http_FundRequest_queue: Queue[FundRequest] = Queue(cpu_count())
        self._http_FundRequest_list = list()
        self._http_response_queue: Queue[FundResponse] = Queue()
        self._exit_sign: Event = Event()
        self._result_save_queue: List[FundContext] = list()
        self._fund_context_dict: dict[str, FundContext] = dict()
        self._fund_waiting_dict: dict[str, List[PageType]] = dict()

        # 相关模块
        self._need_crawled_fund_module = need_crawled_fund_module
        self._data_mining_module = data_mining_module
        self._save_result_module = save_result_module
        self._downloader = GetPageByMultiThreading(self._http_FundRequest_queue, self._http_response_queue,
                                                   self._exit_sign)

        # 总共需要的步骤(当前一个基金只算一步)
        self._total_step_count = None
        # 当前已经完成的
        self._finished_step_count = None

    def show_process(self):
        """
        爬取进度提示
        """
        logging.info("开始获取需要爬取的基金任务")
        while self._total_step_count is None:
            # 等待任务开始
            sleep(1)

        logging.info("开始爬取基金数据")
        with tqdm(total=self._total_step_count) as pbar:
            last_finished_task_num = None
            while self._finished_step_count < self._total_step_count:
                cur_finished_task_num = self._finished_step_count
                pbar.update(cur_finished_task_num - (last_finished_task_num if last_finished_task_num else 0))
                last_finished_task_num = cur_finished_task_num
                sleep(1)

    def run(self) -> NoReturn:
        try:
            # 独立的爬取进程（避免GIL）
            self._downloader.start()

            # 独立的进度展示线程
            Thread(target=self.show_process).start()

            # 爬取主流程
            self.do_run()
        except:
            logging.exception("报错啦，完蛋啦")
        finally:
            # downloader是子进程，一定要shutdown
            self._exit_sign.set()
            self._http_FundRequest_queue.close()
            self._http_response_queue.close()

            self._save_result_module.exit()

    def do_run(self):
        """
        http请求是异步的，为了提高并发度，这里略微借鉴redis的事件驱动机制（没有严格地实现每个事件的回调处理类）
        优先响应 http请求事件 其次 http返回事件（数据挖掘） 最后 结果保存
        """
        # 获取任务
        fund_context_list = self._need_crawled_fund_module.get_fund_list()
        self._fund_context_dict = {fund.fund_code: fund for fund in fund_context_list}
        self._total_step_count = len(fund_context_list)
        self._finished_step_count = 0

        while self._finished_step_count < self._total_step_count:
            # 提交http请求
            if (not self._http_FundRequest_queue.full() and
                    len(self._http_FundRequest_list) != 0 and not self._http_response_queue.full()):
                self._http_FundRequest_queue.put(self._http_FundRequest_list.pop())
                continue

            # 处理http结果
            if not self._http_response_queue.empty():
                cur_res = self._http_response_queue.get()
                self._fund_waiting_dict[cur_res.fund_code].remove(cur_res.page_type)
                self._fund_context_dict[cur_res.fund_code].http_response_dict[cur_res.page_type] = cur_res

            # 数据挖掘及保存
            for fund_code, context in self._fund_context_dict.items():
                if fund_code in self._fund_waiting_dict and len(self._fund_waiting_dict[fund_code]) > 0:
                    continue
                elif fund_code in self._fund_waiting_dict and len(self._fund_waiting_dict[fund_code]) == 0:
                    self._fund_waiting_dict.pop(fund_code)

                # 没有/不存在等待队列，认为数据已经OK，可以传递给数据挖掘模块
                fund_context = self._fund_context_dict.pop(fund_code)
                FundRequest_list = self._data_mining_module.summit_context(fund_context)

                if FundRequest_list:
                    # 数据挖掘模块提出新的爬取请求
                    self._http_FundRequest_list.extend(FundRequest_list)
                    self._fund_context_dict[fund_context.fund_code] = fund_context
                    self._fund_waiting_dict[fund_context.fund_code] = [req.page_type for req in FundRequest_list]
                else:
                    # 没有新的爬取请求，保存爬取结果
                    self._finished_step_count += 1
                    self._save_result_module.save_result(fund_context)

                # 只处理一个数据，继续重复大循环
                break

        logging.info("爬取结束")
