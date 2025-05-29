"""
通过requests进行http下载
"""
import logging
import multiprocessing
from concurrent.futures import Future, ThreadPoolExecutor
from multiprocessing import Queue, Process, Event
from os import cpu_count
from queue import Empty
from sys import maxsize
from time import sleep
from typing import Optional, List, Dict
from urllib.parse import urlparse

from requests import RequestException, get, Response

from module.downloader.rate_control.rate_control import RateControl
from utils.constants import PageType, log_format
from utils.fake_ua_getter import singleton_fake_ua

# 日志配置
logger: logging.Logger = multiprocessing.get_logger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(handler)


class FundRequest:
    def __init__(self, fund_code: str, page_type: PageType, url, retry_time: int = maxsize):
        self.fund_code: str = fund_code
        self.page_type: PageType = page_type
        self.url: str = url
        self.domain: str = urlparse(url).netloc

        if retry_time < 1:
            raise Exception()
        self.retry_time = retry_time


class FundResponse:
    def __init__(self, request: FundRequest, response: Optional[Response]):
        self.fund_code: str = request.fund_code
        self.page_type: PageType = request.page_type
        self.url: str = request.url
        self.domain: str = urlparse(request.url).netloc
        self.remain_retry_time: int = request.retry_time - 1
        # 特别地，当下载失败时 res为None
        self.response: Optional[Response] = response

    def build_request(self):
        return FundRequest(self.fund_code, self.page_type, self.url)


class GetPageOnSubProcess(Process):
    """
    多线程http下载(单独进程)
    """
    MAX_WORKER = (cpu_count() or 1) * 5

    def __init__(self, log_level: int):
        super().__init__()
        # 父子进程间的通信
        self._request_queue: Queue[FundRequest] = Queue()
        self._result_queue: Queue[FundResponse] = Queue()
        self._exit_sign: Event = Event()

        # 请求队列最大的堆积任务数量
        self.max_request_size = 20

        # 爬取速率控制
        self._request_result_dict: Dict[str, List[bool]] = dict()

        logger.setLevel(log_level)

    def apply(self, request: FundRequest):
        if self._exit_sign.is_set():
            raise Exception()
        self._request_queue.put(request)

    def get_result(self, block: bool = True) -> Optional[FundResponse]:
        return self._result_queue.get(block=block, timeout=1)

    def if_downloader_busy(self) -> bool:
        return self._request_queue.qsize() >= self.MAX_WORKER

    def close_downloader(self):
        self._exit_sign.set()

    def join_downloader(self):
        while self._exit_sign.is_set():
            sleep(0.1)

        # By default, if a process is not the creator of the queue
        # then on exit it will attempt to join the queue’s background thread.
        # 说人话就是，主进程必须将队列清理干净，否则子进程不会结束
        logging.info(f'队列情况{self._request_queue.qsize()} and {self._result_queue.qsize()}')
        while True:
            try:
                self._request_queue.get(timeout=0.1)
            except Empty:
                break
        self._request_queue.close()
        while True:
            try:
                self._result_queue.get(timeout=0.1)
            except Empty:
                break
        self._result_queue.close()
        self._request_queue.join_thread()
        self._result_queue.join_thread()

        # 等待子进程退出
        self.join()
        logger.info("子进程完全退出")

    @staticmethod
    def get_page(request: FundRequest) -> FundResponse:
        """
        通过requests下载页面
        """
        header = {"User-Agent": singleton_fake_ua.get_random_ua()}
        try:
            page = get(request.url, headers=header, timeout=2)
            if page.status_code != 200 or not page.text:
                # 反爬虫策略之 给你返回空白的 200
                raise AttributeError
            return FundResponse(request, page)
        except (RequestException, AttributeError) as e:
            return FundResponse(request, None)

    def future_callback(self, future: Future[FundResponse]):
        """
        页面下载的callback流程
        """
        result = future.result()
        if result.response is None and result.remain_retry_time > 0:
            # 失败重试
            self._request_queue.put(result.build_request())
            if result.domain in self._request_result_dict:
                self._request_result_dict[result.domain].append(False)
            else:
                self._request_result_dict[result.domain] = [False]
        else:
            self._result_queue.put(result)
            if result.domain in self._request_result_dict:
                self._request_result_dict[result.domain].append(True)
            else:
                self._request_result_dict[result.domain] = [True]

    def run(self) -> None:
        """
        爬取主流程
        """
        executor_dict: Dict[str, ThreadPoolExecutor] = dict()
        rate_control_dict: Dict[str, RateControl] = dict()

        try:
            logger.info("子进程开启循环")
            self.do_run(executor_dict, rate_control_dict)
        except Exception as e:
            logging.exception(f"报错啦，子进程完蛋啦 {e}")
        finally:
            logger.info("子进程退出循环")
            for executor in executor_dict.values():
                executor.shutdown()
            for rc in rate_control_dict.values():
                rc.exit()
            self._exit_sign.clear()

    def do_run(self, executor_dict: Dict[str, ThreadPoolExecutor], rate_control_dict: Dict[str, RateControl]):
        while True:
            # 爬取结束
            if self._exit_sign.is_set() and self._request_queue.empty():
                break

            # 速率控制
            # 一个稍微巧妙的设计 list的append方法是线程安全的，因此各个请求线程可以直接将结果加入列表
            # clear线程不安全，但是无所谓，毕竟我们关注的是某一段时间内的 成功失败率
            for domain, result_list in self._request_result_dict.items():
                rate_control_dict[domain].record(sum(result_list), len(result_list)
                                                 , executor_dict[domain]._work_queue.qsize())
                result_list.clear()

            # 处理爬取请求
            counter = 0
            while counter < 100 and not self._request_queue.empty():
                counter += 1

                try:
                    request = self._request_queue.get(timeout=1)

                    if request.domain not in executor_dict:
                        executor_dict[request.domain] = ThreadPoolExecutor(max_workers=self.MAX_WORKER)
                    if request.domain not in rate_control_dict:
                        rate_control_dict[request.domain] = RateControl(request.domain, float(self.MAX_WORKER))

                    domain_executor = executor_dict[request.domain]
                    cur_rate = rate_control_dict[request.domain].get_cur_rate()
                    if cur_rate > domain_executor._work_queue.qsize():
                        future = domain_executor.submit(self.get_page, request)
                        future.add_done_callback(self.future_callback)
                    else:
                        self._request_queue.put(request)
                except Empty:
                    pass
