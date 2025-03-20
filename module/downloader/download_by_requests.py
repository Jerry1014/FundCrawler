"""
通过requests进行http下载
"""
import logging
import multiprocessing
from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum, auto, unique
from multiprocessing import Queue, Process, Event
from sys import maxsize

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
    """
    在基础的请求上, 增加了重试次数
    """

    def __init__(self, fund_code: str, page_type: PageType, url, retry_time: int = maxsize):
        self.fund_code = fund_code
        self.page_type = page_type
        self.url = url

        if retry_time < 1:
            raise AttributeError
        self.retry_time = retry_time


class FundResponse:
    """
    在基础的返回上, 增加了请求状态(用于重试)
    """

    @unique
    class State(Enum):
        SUCCESS = auto()
        FALSE = auto()

    def __init__(self, request: FundRequest, state: State, response: Response):
        self.fund_code = request.fund_code
        self.page_type = request.page_type
        self.url = request.url
        self.remain_retry_time = request.retry_time - 1
        self.response = response
        self.state = state

    def build_request(self):
        return FundRequest(self.fund_code, self.page_type, self.url)


class GetPageByMultiThreading(Process):
    """
    独立爬取进程
    内部维护了一个线程池 来进行请求的爬取
    """

    def __init__(self, request_queue: Queue, result_queue: Queue, exit_sign: Event, log_level: int):
        super().__init__()
        # 和父进程之间的通信
        self._request_queue = request_queue
        self._result_queue = result_queue
        self._exit_sign = exit_sign

        # 爬取速率控制
        self._rate_control = RateControl()

        logger.setLevel(log_level)

    @staticmethod
    def get_page(request: FundRequest) -> FundResponse:
        """
        页面下载
        """
        header = {"User-Agent": singleton_fake_ua.get_random_ua()}
        try:
            page = get(request.url, headers=header, timeout=1)
            if page.status_code != 200 or not page.text:
                # 反爬虫策略之 给你返回空白的 200结果
                raise AttributeError
            return FundResponse(request, FundResponse.State.SUCCESS, page)
        except (RequestException, AttributeError):
            return FundResponse(request, FundResponse.State.FALSE, Response())

    def run(self) -> None:
        """
        爬取主流程
        """
        executor = ThreadPoolExecutor()
        future_list: list[Future] = []
        need_retry_task_list: list[FundRequest] = list()

        logger.info("子进程开启循环")
        while True:
            # 爬取结束
            if self._exit_sign.is_set() and self._request_queue.empty() and not future_list \
                    and not need_retry_task_list:
                executor.shutdown()
                self._rate_control.shutdown()
                self._result_queue.close()
                self._exit_sign.clear()
                break

            # 获取已完成的task
            need_handle_result_list: list[FundResponse] = list()
            for future in future_list:
                if future.done():
                    result: FundResponse = future.result()
                    need_handle_result_list.append(result)
                    future_list.remove(future)
                    continue

            # 处理爬取结果
            for result in need_handle_result_list:
                if result.state == FundResponse.State.FALSE and result.remain_retry_time > 0:
                    # 失败重试
                    self._request_queue.put(result.build_request())
                    continue
                self._result_queue.put(result)

            # 爬取速率控制
            # todo 首要任务 建立起准确的爬取数据 现在这个数据分片大小并不稳定
            # 单独出一个线程 每秒清空一下积攒的成功/失败计数
            success_count = sum(
                [1 if result.state == FundResponse.State.SUCCESS else 0 for result in need_handle_result_list])
            number_of_concurrent_tasks = self._rate_control \
                .get_cur_number_of_concurrent_tasks(success_count, len(need_handle_result_list) - success_count,
                                                    len(future_list))

            # 处理爬取请求
            while (not self._request_queue.empty() or len(need_retry_task_list) > 0) \
                    and number_of_concurrent_tasks > len(future_list):
                # 优先处理需要重试的任务
                request = self._request_queue.get()
                future_list.append(executor.submit(self.get_page, request))
                number_of_concurrent_tasks -= 1

        logger.info("子进程退出循环")
        # 确保数据都写入后，再退出主线程
        # OS pipes are not infinitely long, so the process which queues data could be blocked in the OS during the
        # put() operation until some other process uses get() to retrieve data from the queue
        self._result_queue.join_thread()
        logger.info("子进程完全退出")
