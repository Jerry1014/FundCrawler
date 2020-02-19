# -*- coding:UTF-8 -*-
"""
负责接受url并爬取该网页
"""
import threading
from abc import ABC
from multiprocessing import Process, Queue, Event
from time import time

from FakeUA import fake_ua


class GetPage:
    """
    获取页面基类，从_task_queue中获取任务，输出结果到_result_queue中
    """

    def __init__(self):
        self._task_queue = None
        self._result_queue = None


class RetryException(Exception):
    """
    用于提示重试
    """
    pass


class GetPageByWeb(GetPage, ABC):
    """
    从网页中获取页面基类
    """

    @classmethod
    def get_page_context(cls, url, timeout, *args) -> tuple:
        """
        用于爬取页面 爬取特定的网页
        :param timeout: 爬取timeout设置，可为一个数字，或一个元组
        :param url:要爬取的url
        :return: 返回二元组 爬取结果，网页内容
        """
        header = {"User-Agent": fake_ua.random}
        import requests
        state = None
        text = None
        try:
            page = requests.get(url, headers=header, timeout=timeout)
            page.encoding = 'utf-8'
            text = page.text
            if page.status_code != 200 or not text:
                raise RetryException()
            state = 'success'
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError):
            state = 'error'
        except RetryException:
            state = 'retry'
        finally:
            result = (state, text, *args)
            return result


class GetPageByWebWithAnotherProcessAndMultiThreading(Process, GetPageByWeb):
    """
    启动另一个进程，并在这个进程中使用多线程来爬取网页
    将爬取任务发送到 task_queue，并在完成后将 exit_sign 设置为True
    进程会在所有的任务都完成后，将 exit_sign 设置为False，并在result_queue中的item取完后退出
    """
    # 描述在持续几秒连接失败之后向用户展示提示信息，单位 秒
    SHOW_NETWORK_DOWN_LIMIT_TIME = 3

    def __init__(self, task_queue: Queue, result_queue: Queue, exit_sign: Event, network_health: Event, timeout):
        super().__init__()
        self._task_queue = task_queue
        self._result_queue = result_queue
        self._threading_pool = list()
        self._exit_when_task_queue_empty = exit_sign
        self._max_threading_number = 2
        self._record_network_down_last_time = None
        self._network_health = network_health
        self._timeout = timeout

    def add_task(self, task):
        self._task_queue.put(task)

    def get_result(self) -> Queue:
        return self._result_queue

    def get_page_context_and_return_in_queue(self, url, *args):
        result = super().get_page_context(url, self._timeout, *args)
        if result[0] == 'success':
            self._max_threading_number += 1
            if self._network_health.is_set():
                self._record_network_down_last_time = None
                self._network_health.clear()
            self._result_queue.put(result)
        elif result[0] == 'retry':
            self._max_threading_number = self._max_threading_number - 1 if self._max_threading_number > 1 else 1
            self._task_queue.put((url, *args))
        else:
            self._max_threading_number = self._max_threading_number >> 1 if self._max_threading_number > 1 else 1
            if self._max_threading_number == 1 and not self._network_health.is_set():
                if self._record_network_down_last_time is None:
                    self._record_network_down_last_time = time()
                elif time() - self._record_network_down_last_time > \
                        GetPageByWebWithAnotherProcessAndMultiThreading.SHOW_NETWORK_DOWN_LIMIT_TIME:
                    self._network_health.set()
            # 在此处，若有一直爬取失败的任务，则任务队列永远不能为空，即不能终止。未来加入爬取失败队列
            self._task_queue.put((url, *args))

    def run(self) -> None:
        while True:
            if self._task_queue.empty() and len(self._threading_pool) == 0:
                if self._exit_when_task_queue_empty.is_set():
                    self._exit_when_task_queue_empty.clear()
                    break
                else:
                    continue
            else:
                # 1 清除死去的线程
                # 2 新建新的线程
                for t in self._threading_pool:
                    if not t.is_alive():
                        self._threading_pool.remove(t)

                while self._task_queue.qsize() > 0 and len(self._threading_pool) < self._max_threading_number:
                    task = self._task_queue.get()
                    t = threading.Thread(target=self.get_page_context_and_return_in_queue,
                                         args=(task[0], *task[1:]))
                    self._threading_pool.append(t)
                    t.start()
