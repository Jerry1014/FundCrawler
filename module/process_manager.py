"""
负责统领和协调数据爬取的流程
"""
import logging
from multiprocessing import Queue, Event
from queue import Empty
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
    # 请求队列最大的堆积任务数量
    MAX_REQUEST_SIZE = 20

    def __init__(self, need_crawled_fund_module: CrawlingTargetModule, data_mining_module: DataMiningModule,
                 save_result_module: SavingResultModule):
        # 事件列表等(模块间的协作)
        self._http_request_queue: Queue[FundRequest] = Queue()
        self._http_response_queue: Queue[FundResponse] = Queue()
        self._exit_sign: Event = Event()
        self._fund_context_dict: dict[str, FundContext] = dict()
        self._fund_waiting_dict: dict[str, List[PageType]] = dict()

        # 相关模块
        self._need_crawled_fund_module = need_crawled_fund_module
        self._data_mining_module = data_mining_module
        self._save_result_module = save_result_module
        self._downloader = GetPageByMultiThreading(self._http_request_queue, self._http_response_queue,
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
            # http的解析和数据的保存
            # 所有的req请求都由解析模块提出，因为判断一下当时请求队列是否打满
            if self._http_request_queue.qsize() < self.MAX_REQUEST_SIZE:
                first_meet_fund_code = None
                for fund_code in self._fund_context_dict.keys():
                    # 这里要注意req的顺序和context的遍历顺序，避免堆积大量处于中间状态的任务
                    # 寻找第一个waiting队列已经处理完毕的context
                    if fund_code in self._fund_waiting_dict and len(self._fund_waiting_dict[fund_code]) > 0:
                        continue
                    elif fund_code in self._fund_waiting_dict and len(self._fund_waiting_dict[fund_code]) == 0:
                        self._fund_waiting_dict.pop(fund_code)
                    first_meet_fund_code = fund_code
                    break

                if first_meet_fund_code:
                    fund_context = self._fund_context_dict[first_meet_fund_code]
                    request_list = self._data_mining_module.summit_context(fund_context)

                    if request_list:
                        # 数据挖掘模块提出新的爬取请求
                        for req in request_list:
                            self._http_request_queue.put(req)
                        self._fund_waiting_dict[fund_context.fund_code] = [req.page_type for req in request_list]
                    else:
                        # 没有新的爬取请求，保存爬取结果
                        self._fund_context_dict.pop(first_meet_fund_code)
                        self._finished_step_count += 1
                        self._save_result_module.save_result(fund_context)

            # 处理http请求结果
            # 请求队列已满的时候，这里直接阻塞等待结果，避免忙等待
            block = self._http_request_queue.qsize() > self.MAX_REQUEST_SIZE
            try:
                cur_res = self._http_response_queue.get(block=block)
                self._fund_waiting_dict[cur_res.fund_code].remove(cur_res.page_type)
                self._fund_context_dict[cur_res.fund_code].http_response_dict[cur_res.page_type] = cur_res
            except Empty:
                pass

        logging.info("爬取结束")
