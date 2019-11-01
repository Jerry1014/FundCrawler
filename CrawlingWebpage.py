# -*- coding:UTF-8 -*-
"""
负责接受url并爬取该网页
"""
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
    def get_page_context(cls, url) -> tuple:
        """
        用于爬取页面 爬取特定的网页
        :param url:要爬取的url
        :return: 返回二元组 爬取结果，网页内容
        """
        header = {"User-Agent": fake_ua.random}
        try:
            page = requests.get(url, headers=header, timeout=(30, 70))
            page.encoding = 'utf-8'
            result = ('success', page.text)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError):
            result = ('error', None)
        return result


class GetPageByWebWithAnotherProcessAndMultiThreading(GetPageByWeb, Process):
    def __init__(self, task_queue: Queue, result_queue: Queue):
        super().__init__()
        self._task_queue = task_queue
        self._result_queue = result_queue
        self._threading_pool = list()
        self._exit_when_task_queue_empty = False

    def add_task(self, task):
        self._task_queue.put(task)

    def get_result(self) -> Queue:
        return self._result_queue

    def run(self) -> None:
        while True:
            if self._task_queue.empty():
                if self._exit_when_task_queue_empty:
                    break
                else:
                    continue
            else:
                pass
                # todo 线程数量管理
