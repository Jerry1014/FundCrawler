"""
数据爬取模块
"""
from typing import NoReturn

from module.crawling_data.data_mining.data_cleaning_strategy_factory import DataCleaningStrategyFactory
from module.crawling_data.data_mining.data_mining_type import PageType
from task_manager import CrawlingDataModule, FundCrawlingResult, NeedCrawledFundModule
from utils.downloader.async_downloader import AsyncHttpDownloader, BaseRequest
from utils.downloader.impl.http_request_downloader import AsyncHttpRequestDownloader


class AsyncCrawlingData(CrawlingDataModule):
    """
    数据爬取模块类
    负责处理基金爬取任务, 爬取若干个页面, 并对爬取到的数据进行清洗, 得到最终的爬取结果
    """

    def __init__(self):
        self._downloader = AsyncHttpRequestDownloader()
        self._context_dict: dict[int, AsyncCrawlingData.Context] = {}

        self._context_id = 0

    def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund) -> NoReturn:
        """
        构造爬取上下文，并加入到集合中
        """
        context_id = self.get_context_id_and_increase()
        self._context_dict[context_id] = AsyncCrawlingData.Context(context_id, task, self._downloader)

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
        while True:
            result = self._downloader.get_result()
            unique_key: AsyncCrawlingData.Context.UniqueKey = result.request.unique_key
            context = self._context_dict.get(unique_key.context_id)
            context.task_finish(unique_key.task_id, result.response)

            # 如果爬取上下文中所有需要爬取的任务都完成了, 就可以取出进行数据清洗并返回结果
            if context.all_task_finished():
                result = FundCrawlingResult(
                    {FundCrawlingResult.FundInfoHeader.FUND_CODE: context.fund_task.code
                        , FundCrawlingResult.FundInfoHeader.FUND_SIMPLE_NAME: context.fund_task.name})
                for task in context.finished_task:
                    strategy = DataCleaningStrategyFactory.get_strategy(task.page_type)
                    strategy.fill_result(task.response, result)
                return result

    def get_context_id_and_increase(self) -> int:
        """
        获取唯一 爬取上下文id
        :return: id
        """
        tem = self._context_id
        self._context_id += 1
        return tem

    class Context:
        """
        爬取上下文
        包含若干个需要爬取的页面
        """

        def __init__(self, context_id: int, fund_task: NeedCrawledFundModule.NeedCrawledOnceFund,
                     downloader: AsyncHttpDownloader):
            self._task_id = 0
            self._context_id = context_id
            self._downloader = downloader
            self.fund_task = fund_task
            self.finished_task: list[AsyncCrawlingData.PageCrawlingTask] = []

            # 构造页面爬取任务
            self._running_task_dict: dict[int, AsyncCrawlingData.PageCrawlingTask] = {}

            task_id = self.get_task_id_and_increase()
            page_type = PageType.OVERVIEW
            url = page_type.value().substitute(fund_code=fund_task.code)
            self._downloader.summit(BaseRequest(AsyncCrawlingData.Context.UniqueKey(self._context_id, task_id), url))
            self._running_task_dict[task_id] = AsyncCrawlingData.PageCrawlingTask(page_type, url)

        def get_task_id_and_increase(self) -> int:
            """
            获取唯一 页面下载任务id
            :return: id
            """
            tem = self._task_id
            self._task_id += 1
            return tem

        def all_task_finished(self) -> bool:
            """
            爬取上下文, 所有的下载任务都完成了
            :return: bool
            """
            return len(self._running_task_dict) == 0

        def task_finish(self, task_id: int, response) -> NoReturn:
            """
            页面爬取任务完成
            """
            task = self._running_task_dict.pop(task_id)
            task.response = response

            self.finished_task.append(task)

        class UniqueKey(BaseRequest.UniqueKey):
            def __init__(self, context_id: int, task_id: int):
                self.context_id = context_id
                self.task_id = task_id

    class PageCrawlingTask:
        """
        页面爬取任务
        """

        def __init__(self, page_type: PageType, url: str):
            self.page_type = page_type
            self.url = url
            self.response = None
