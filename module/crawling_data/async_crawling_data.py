"""
数据爬取模块
"""
import logging
from typing import NoReturn, Optional, Any

from module.crawling_data.data_mining.data_cleaning_strategy_factory import DataCleaningStrategyFactory
from module.crawling_data.data_mining.data_mining_type import PageType
from process_manager import CrawlingDataModule, FundCrawlingResult, NeedCrawledFundModule
from utils.downloader.async_downloader import AsyncHttpDownloader, BaseRequest
from utils.downloader.impl.http_request_downloader import AsyncHttpRequestDownloader, Request


class AsyncCrawlingData(CrawlingDataModule):
    """
    数据爬取模块类
    负责处理基金爬取任务, 爬取若干个页面, 并对爬取到的数据进行清洗, 得到最终的爬取结果
    """

    def __init__(self, need_data_type_list: list[PageType] = None):
        self._need_data_type_list = need_data_type_list if need_data_type_list else [i for i in PageType]

        self._shutdown = False
        self._downloader = AsyncHttpRequestDownloader()

        self._unfinished_context_dict: dict[int, AsyncCrawlingData.Context] = {}
        self._cur_context_id = 0

    def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund) -> NoReturn:
        """
        构造爬取上下文，并加入到集合中
        """
        context_id = self.get_context_id_and_increase()
        self._unfinished_context_dict[context_id] = AsyncCrawlingData.Context(context_id, task, self._downloader,
                                                                              self._need_data_type_list)

    def has_next_result(self) -> bool:
        """
        请求、结果没有队列，因此只需要看当前是否还有未处理的上下文
        """
        return not self._shutdown or len(self._unfinished_context_dict) != 0

    def get_an_result(self) -> Optional[FundCrawlingResult]:
        """
        1 在下载器中取回一个结果, 并将结果填充到对应的 context的 pageTask中
        2 当某个context的pageTask全部处理完成时, 走到第三步, 否则重复1 直到某个context被全部处理完
        3 清洗 context中 所有的pageTask中的数据, 构造得到最终的爬取结果
        """
        while True:
            crawling_result = self._downloader.get_result()
            if not crawling_result:
                return None

            unique_key: AsyncCrawlingData.Context.UniqueKey = crawling_result.request.unique_key
            context = self._unfinished_context_dict.get(unique_key.context_id)
            context.finish_task(unique_key.task_id, crawling_result.response)

            # 如果爬取上下文中所有需要爬取的任务都完成了, 就可以取出进行数据清洗并返回结果
            if context.all_task_finished():
                del self._unfinished_context_dict[unique_key.context_id]

                fund_result = FundCrawlingResult(context.fund_task.code, context.fund_task.name)
                for task in context.finished_task:
                    if task.response:
                        try:
                            strategy = DataCleaningStrategyFactory.get_strategy(task.page_type)
                            strategy.fill_result(task.response, fund_result)
                        except Exception as e:
                            logging.error(f"基金{context.fund_task.code} {task.page_type}数据 数据解析失败", exc_info=e)
                    else:
                        logging.error(f"基金{context.fund_task.code} {task.page_type}数据 爬取失败")

                return fund_result

    def get_context_id_and_increase(self) -> int:
        """
        获取唯一 爬取上下文id
        """
        tem = self._cur_context_id
        self._cur_context_id += 1
        return tem

    def shutdown(self):
        self._downloader.shutdown()
        self._shutdown = True

    class Context:
        """
        爬取上下文
        包含若干个需要爬取的页面
        """

        def __init__(self, context_id: int, fund_task: NeedCrawledFundModule.NeedCrawledOnceFund,
                     downloader: AsyncHttpDownloader, need_data_type_list: list[PageType]):
            self._context_id = context_id
            self._downloader = downloader
            self.fund_task = fund_task

            self._cur_task_id = 0
            self.finished_task: list[AsyncCrawlingData.PageCrawlingTask] = []
            self._running_task_dict: dict[int, AsyncCrawlingData.PageCrawlingTask] = {}

            # 构造页面爬取任务
            for date_type in need_data_type_list:
                task_id = self.get_task_id_and_increase()

                strategy = DataCleaningStrategyFactory.get_strategy(date_type)
                url = strategy.build_url(fund_task.code)
                self._downloader.summit(Request(AsyncCrawlingData.Context.UniqueKey(self._context_id, task_id), url))
                self._running_task_dict[task_id] = AsyncCrawlingData.PageCrawlingTask(date_type, url)

        def get_task_id_and_increase(self) -> int:
            """
            获取唯一 页面下载任务id
            """
            tem = self._cur_task_id
            self._cur_task_id += 1
            return tem

        def all_task_finished(self) -> bool:
            """
            爬取上下文, 所有的下载任务都完成了
            """
            return not self._running_task_dict

        def finish_task(self, task_id: int, response: Optional[Any]) -> NoReturn:
            """
            页面爬取任务完成
            :param response: None代表数据爬取失败了
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
