from typing import NoReturn

from manager.task_manager import CrawlingDataModule, FundCrawlingResult, NeedCrawledFundModule
from utils.downloader.impl.http_request_downloader import AsyncHttpRequestDownloader


class AsyncCrawlingData(CrawlingDataModule):

    def __init__(self):
        self._downloader = AsyncHttpRequestDownloader()
        self._context_dict: dict[int, AsyncCrawlingData.Context] = dict()

        self._context_id = 0
        self._task_id = 0

    def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund) -> NoReturn:
        """
        构造爬取上下文，并加入到集合中
        """
        context_id = self.get_context_id_and_increase()
        self._context_dict[context_id] = AsyncCrawlingData.Context(context_id, task)

    def empty_request_and_result(self) -> bool:
        """
        请求、结果没有队列，因此只需要看当前是否还有未处理的上下文
        """
        return len(self._context_dict) == 0

    def get_an_result(self) -> FundCrawlingResult:
        """
        1 在下载器中取回一个结果, 并将结果填充到对应的 context的 pageTask中
        2 当某个context的pageTask全部处理完成时, 走到第三步, 否则重复1 直到某个context被全部处理完
        3 清洗 context中 所有的pageTask中的数据, 构造得到最终的爬取结果
        """
        # todo

    def get_context_id_and_increase(self):
        tem = self._context_id
        self._context_id += 1
        return tem

    def get_task_id_and_increase(self):
        tem = self._task_id
        self._task_id += 1
        return tem

    class Context:
        def __init__(self, context_id: int, fund_task: NeedCrawledFundModule.NeedCrawledOnceFund):
            # todo 创建每一个页面的爬取任务，并加入到下载器中
            self._task_dict: dict[int, AsyncCrawlingData.PageCrawlingTask] = dict()

            #

    class PageCrawlingTask:
        def __init__(self):
            pass
