"""
通过requests进行http下载
"""
import logging
import multiprocessing
from concurrent.futures import Future, ThreadPoolExecutor
from multiprocessing import Queue, Process, Event
from queue import Empty
from sys import maxsize
from typing import Optional

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
        self.fund_code = fund_code
        self.page_type = page_type
        self.url = url

        if retry_time < 1:
            raise AttributeError
        self.retry_time = retry_time


class FundResponse:
    def __init__(self, request: FundRequest, response: Optional[Response]):
        self.fund_code = request.fund_code
        self.page_type = request.page_type
        self.url = request.url
        self.remain_retry_time = request.retry_time - 1
        # 特别地，当下载失败时 res为None
        self.response = response

    def build_request(self):
        return FundRequest(self.fund_code, self.page_type, self.url)


class GetPageOnSubProcess(Process):
    """
    多线程http下载(单独进程)
    """

    def __init__(self, request_queue: Queue, result_queue: Queue, exit_sign: Event, log_level: int):
        super().__init__()
        # 和父进程之间的通信
        self._request_queue = request_queue
        self._result_queue = result_queue
        self._exit_sign = exit_sign

        # 爬取速率控制
        self._rate_control: Optional[RateControl] = None
        self._executor: Optional[ThreadPoolExecutor] = None

        logger.setLevel(log_level)

    @staticmethod
    def get_page(request: FundRequest) -> FundResponse:
        """
        通过requests下载页面
        """
        header = {"User-Agent": singleton_fake_ua.get_random_ua()}
        try:
            page = get(request.url, headers=header, timeout=1)
            if page.status_code != 200 or not page.text:
                # 反爬虫策略之 给你返回空白的 200结果
                raise AttributeError
            return FundResponse(request, page)
        except (RequestException, AttributeError):
            return FundResponse(request, None)

    def future_callback(self, future: Future[FundResponse]):
        """
        页面下载的callback流程
        """
        result = future.result()
        if result.response is None and result.remain_retry_time > 0:
            # 失败重试
            self._request_queue.put(result.build_request())
        else:
            self._result_queue.put(result)

    def run(self) -> None:
        """
        爬取主流程
        """
        logger.info("子进程开启循环")
        self._executor = ThreadPoolExecutor()
        self._rate_control = RateControl(self._executor._max_workers)

        while True:
            # 爬取结束
            if self._exit_sign.is_set() and self._request_queue.empty():
                self._executor.shutdown()
                self._rate_control.shutdown()
                self._result_queue.close()
                break

            # 速率控制
            cur_rate = 100

            # 处理爬取请求
            while not self._request_queue.empty() and cur_rate > self._executor._work_queue.qsize():
                # 优先处理需要重试的任务
                try:
                    request = self._request_queue.get(timeout=1)
                    future = self._executor.submit(self.get_page, request)
                    future.add_done_callback(self.future_callback)
                except Empty:
                    break

        logger.info("子进程退出循环")
        # 确保数据都写入后，再退出主线程
        # OS pipes are not infinitely long, so the process which queues data could be blocked in the OS during the
        # put() operation until some other process uses get() to retrieve data from the queue
        self._result_queue.join_thread()
        logger.info("子进程完全退出")
