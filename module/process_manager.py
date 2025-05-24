"""
负责统领和协调数据爬取的流程
"""
import logging
from queue import Empty
from threading import Thread
from time import sleep
from typing import List, Optional

from tqdm import tqdm

from module.abstract_crawling_target_module import CrawlingTargetModule
from module.abstract_data_mining_module import DataMiningModule
from module.abstract_saving_result_module import SavingResultModule
from module.downloader.download_by_requests import FundRequest, GetPageOnSubProcess
from module.fund_context import FundContext
from utils.constants import PageType


class TaskManager:
    """
    爬取的核心流程
    """

    def __init__(self, need_crawled_fund_module: CrawlingTargetModule, data_mining_module: DataMiningModule,
                 save_result_module: SavingResultModule):
        # 事件列表等(模块间的协作)
        self._fund_context_dict: dict[str, FundContext] = dict()
        self._fund_waiting_dict: dict[str, List[PageType]] = dict()

        # 相关模块
        self._need_crawled_fund_module = need_crawled_fund_module
        self._data_mining_module = data_mining_module
        self._save_result_module = save_result_module
        self._downloader = GetPageOnSubProcess(logging.root.level)

        # 总共需要的步骤(当前一个基金只算一步)
        self._total_step_count: Optional[int] = None
        # 当前已经完成的
        self._finished_step_count: Optional[int] = None

        self._exit_sign: bool = False

    def show_process(self) -> None:
        """
        爬取进度提示
        """
        logging.info("开始获取需要爬取的基金任务")
        while not self._exit_sign and (self._total_step_count is None or self._finished_step_count is None):
            # 等待任务开始
            sleep(0.1)

        logging.info("开始爬取基金数据")
        with tqdm(total=self._total_step_count) as pbar:
            last_finished_task_num = None
            while not self._exit_sign and self._finished_step_count < self._total_step_count:
                cur_finished_task_num = self._finished_step_count
                pbar.update(cur_finished_task_num - (last_finished_task_num if last_finished_task_num else 0))
                last_finished_task_num = cur_finished_task_num
                sleep(0.1)

    def run(self) -> None:
        try:
            # 独立的爬取进程（避免GIL）
            self._downloader.start()

            # 独立的进度展示线程
            Thread(target=self.show_process).start()

            # 爬取主流程
            self.do_run()
        except Exception as e:
            logging.exception(f"报错啦，主进程完蛋啦 {e}")
        finally:
            self._exit_sign = True
            self._downloader.close_downloader()
            self._save_result_module.exit()
            self._downloader.join_downloader()

        logging.info('主进程退出')

    def do_run(self) -> None:
        # 获取任务
        fund_context_list = self._need_crawled_fund_module.get_fund_list()
        self._fund_context_dict = {fund.fund_code: fund for fund in fund_context_list}
        self._total_step_count = len(fund_context_list)
        self._finished_step_count = 0

        while self._finished_step_count < self._total_step_count:
            # http请求发起和解析
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

            has_new_req = False
            if first_meet_fund_code:
                fund_context = self._fund_context_dict[first_meet_fund_code]
                page_req_list = self._data_mining_module.summit_context(fund_context)

                if page_req_list:
                    # 数据挖掘模块提出新爬取请求
                    fund_wait_list = list()
                    for page_req in page_req_list:
                        # 特殊请求，代表需要等待其他的解析结果才能爬取，不必处理
                        if page_req[1] is None:
                            continue
                        self._downloader.apply(FundRequest(fund_context.fund_code, page_req[0], page_req[1]))
                        fund_wait_list.append(page_req[0])

                    if fund_wait_list:
                        self._fund_waiting_dict[fund_context.fund_code] = fund_wait_list
                        has_new_req = True
                else:
                    # 没有新爬取请求，保存爬取结果
                    self._fund_context_dict.pop(first_meet_fund_code)
                    self._finished_step_count += 1
                    self._save_result_module.save_result(fund_context)

            while True:
                try:
                    # 上一步处理了一圈，发现没有事情可以干的时候，可以block等待返回，避免忙等待
                    block = first_meet_fund_code is None
                    cur_res = self._downloader.get_result(block)
                    self._fund_waiting_dict[cur_res.fund_code].remove(cur_res.page_type)
                    self._fund_context_dict[cur_res.fund_code].http_response_dict[cur_res.page_type] = cur_res
                except Empty:
                    pass

                # 下载器不忙的时候 优先发起请求
                # 没有新请求时 优先保存文件
                if not self._downloader.if_downloader_busy() or not has_new_req:
                    break

        logging.info("爬取结束")
