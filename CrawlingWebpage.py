# -*- coding:UTF-8 -*-
"""
负责接受url并爬取该网页
"""
import threading
from abc import ABC
from multiprocessing import Process, Queue

import requests

from FakeUA import fake_ua


class GetPage:
    def __init__(self):
        self._task_queue = None
        self._result_queue = None

    def add_task(self, task):
        raise NotImplementedError()

    def get_result(self):
        raise NotImplementedError()


class GetPageByWeb(GetPage, ABC):
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
            result = ('success', page.text, args)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError):
            result = ('error', None, args)
        return result


class GetPageByWebWithAnotherProcessAndMultiThreading(GetPageByWeb, Process):
    def __init__(self, task_queue: Queue, result_queue: Queue):
        super().__init__()
        self._task_queue = task_queue
        self._result_queue = result_queue
        self._threading_pool = list()
        self._exit_when_task_queue_empty = False
        self._max_threading_number = 1

    def add_task(self, task):
        self._task_queue.put(task)

    def get_result(self) -> Queue:
        return self._result_queue

    def get_page_context(self, url, *args) -> tuple:
        result = super().get_page_context(url, *args)
        if result[0] == 'success':
            self._max_threading_number += 1
        else:
            self._max_threading_number = self._max_threading_number << 1 if self._max_threading_number > 1 else 1
        return result

    def run(self) -> None:
        while True:
            if self._task_queue.empty():
                if self._exit_when_task_queue_empty:
                    break
                else:
                    continue
            else:
                # 1 清除死去的线程
                # 2 新建新的线程
                for t in self._threading_pool:
                    if not t.is_alive():
                        self._threading_pool.remove(t)

                while len(self._threading_pool) < self._max_threading_number:
                    task = self._task_queue.get()
                    t = threading.Thread(target=self.get_page_context,
                                         args=('http://fund.eastmoney.com/' + task[0] + '.html', *task[1:]))
                    self._threading_pool.append(t)
                    t.start()
