"""
通过requests进行http下载
"""
from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum, auto, unique
from multiprocessing import Queue, Process, Event, synchronize
from os import cpu_count
from queue import Empty
from sys import maxsize
from time import sleep
from typing import Optional, NoReturn

from requests import Response as RequestsResponse, RequestException, get

from utils.downloader.async_downloader import AsyncHttpDownloader, BaseRequest, BaseResponse
from utils.downloader.rate_control.rate_control import RateControl
from utils.fake_ua_getter import singleton_fake_ua


class Request(BaseRequest):
    """
    在基础的请求上, 增加了重试次数
    """

    def __init__(self, unique_key: BaseRequest.UniqueKey, url, retry_time: int = maxsize):
        super().__init__(unique_key, url)
        if retry_time < 1:
            raise AttributeError

        self.retry_time = retry_time


class Response(BaseResponse):
    """
    在基础的返回上, 增加了请求状态(用于重试)
    """

    @unique
    class State(Enum):
        SUCCESS = auto()
        FALSE = auto()

    def __init__(self, request: Request, state: State, response: Optional[RequestsResponse]):
        super().__init__(request, response)
        self.state = state


class AsyncHttpRequestDownloader(AsyncHttpDownloader):
    """
    通过request进行http下载的实现
    新起一个进程 以避免和主进程间的竞争，通过队列进行通信
    进程内通过线程池消费需要爬取的任务
    """

    def __init__(self):
        # 和爬取进程间的通信
        self._request_queue: Queue[Request] = Queue(cpu_count() * 5)
        self._result_queue: Queue[Response] = Queue()
        self._exit_sign: synchronize.Event = Event()

        # 独立的爬取进程
        self._child_process = AsyncHttpRequestDownloader.GetPageByMultiThreading(self._request_queue,
                                                                                 self._result_queue,
                                                                                 self._exit_sign)
        self._child_process.start()

    def summit(self, request: Request):
        self._request_queue.put(request)

    def has_next_result(self) -> bool:
        return self._child_process.is_alive()

    def get_result(self) -> Optional[Response]:
        try:
            return self._result_queue.get(timeout=1)
        except Empty:
            return None

    def shutdown(self):
        self._request_queue.close()
        self._exit_sign.set()
        # 这里不要join子进程
        # 在子进程的队列没有被消费完时，子进程不会退出，join会阻塞住队列的消费，导致死锁

    class GetPageByMultiThreading(Process):
        """
        独立爬取进程
        内部维护了一个线程池 来进行请求的爬取
        """

        def __init__(self, request_queue: Queue, result_queue: Queue, exit_sign: synchronize.Event):
            super().__init__()
            # 和父进程之间的通信
            self._request_queue = request_queue
            self._result_queue = result_queue
            self._exit_sign = exit_sign

            # 爬取速率控制
            self._rate_control = RateControl()

        @staticmethod
        def get_page(request: Request) -> Response:
            """
            页面下载
            """
            header = {"User-Agent": singleton_fake_ua.get_random_ua()}
            try:
                page = get(request.url, headers=header, timeout=1)
                if page.status_code != 200 or not page.text:
                    # 反爬虫策略之 给你返回空白的 200结果
                    raise AttributeError
                return Response(request, Response.State.SUCCESS, page)
            except (RequestException, AttributeError):
                return Response(request, Response.State.FALSE, None)

        def run(self) -> NoReturn:
            """
            爬取主流程
            """
            executor = ThreadPoolExecutor()
            future_list: list[Future] = []
            need_retry_task_list: list[Request] = list()

            self._rate_control.start_analyze()

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
                need_handle_result_list: list[Response] = list()
                for future in future_list:
                    if future.done():
                        result: Response = future.result()
                        need_handle_result_list.append(result)
                        future_list.remove(future)
                        continue

                # 处理爬取结果
                for result in need_handle_result_list:
                    if result.state == Response.State.FALSE and result.request.retry_time > 0:
                        # 失败重试
                        result.request.retry_time -= 1
                        need_retry_task_list.append(result.request)
                        continue
                    self._result_queue.put(result)

                # 爬取速率控制
                success_count = sum(
                    [1 if result.state == Response.State.SUCCESS else 0 for result in need_handle_result_list])
                number_of_concurrent_tasks = self._rate_control \
                    .get_cur_number_of_concurrent_tasks(success_count, len(need_handle_result_list) - success_count)

                # 处理爬取请求
                while (not self._request_queue.empty() or len(need_retry_task_list) > 0) \
                        and number_of_concurrent_tasks > 0:
                    # 优先处理需要重试的任务
                    request = need_retry_task_list.pop() if len(need_retry_task_list) > 0 else self._request_queue.get()
                    future_list.append(executor.submit(self.get_page, request))
                    number_of_concurrent_tasks -= 1

                # 休眠主线程，避免循环占用过多的cpu时间
                sleep(0.1)

            # 确保数据都写入后，再退出主线程
            # OS pipes are not infinitely long, so the process which queues data could be blocked in the OS during the
            # put() operation until some other process uses get() to retrieve data from the queue
            self._result_queue.join_thread()
