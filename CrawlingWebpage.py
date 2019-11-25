# -*- coding:UTF-8 -*-
"""
负责接受url并爬取该网页
"""
import threading
from abc import ABC
from multiprocessing import Process, Queue, Event

import requests

from FakeUA import fake_ua


class GetPage:
    """
    获取页面基类
    """

    def __init__(self):
        self._task_queue = None
        self._result_queue = None

    def add_task(self, task):
        raise NotImplementedError()

    def get_result(self):
        raise NotImplementedError()


class GetPageByWeb(GetPage, ABC):
    """
    从网页中获取页面基类
    """

    @classmethod
    def get_page_context(cls, url, *args) -> tuple:
        """
        用于爬取页面 爬取特定的网页
        :param url:要爬取的url
        :return: 返回二元组 爬取结果，网页内容
        """
        header = {"User-Agent": fake_ua.random}
        try:
            page = requests.get(url, headers=header, timeout=(30, 70))
            page.encoding = 'utf-8'
            result = ('success', page.text, *args)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError):
            result = ('error', url, *args)
        return result


class GetPageByWebWithAnotherProcessAndMultiThreading(Process, GetPageByWeb):
    """
    启动另一个进程，并在这个进程中使用多线程来爬取网页
    将爬取任务发送到 task_queue，并在完成后将 exit_sign 设置为True
    进程会在所有的任务都完成后，将 exit_sign 设置为False，并在result_queue中的item取完后退出
    """

    def __init__(self, task_queue: Queue, result_queue: Queue, exit_sign: Event, network_health: Event):
        super().__init__()
        self._task_queue = task_queue
        self._result_queue = result_queue
        self._threading_pool = list()
        self._exit_when_task_queue_empty = exit_sign
        self._max_threading_number = 2
        self._network_health = network_health

    def add_task(self, task):
        self._task_queue.put(task)

    def get_result(self) -> Queue:
        return self._result_queue

    def get_page_context_and_return_in_queue(self, url, *args):
        result = super().get_page_context(url, *args)
        if result[0] == 'success':
            self._max_threading_number += 1
            if self._network_health.is_set():
                self._network_health.clear()
        else:
            self._max_threading_number = self._max_threading_number >> 1 if self._max_threading_number > 1 else 1
            if self._max_threading_number == 1:
                self._network_health.set()
        self._result_queue.put(result)

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
