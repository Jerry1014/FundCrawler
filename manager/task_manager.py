"""
爬取核心
对爬取过程的管理
"""
from abc import abstractmethod, ABC
from asyncio import TaskGroup
from collections.abc import Generator
from enum import Enum, unique, auto
from typing import NoReturn


class NeedCrawledFundModule(ABC):
    """
    基金爬取任务模块
    通过生成器逐个给出 需要爬取的基金
    """

    class NeedCrawledOnceFund:
        """
        需要爬取的 单个基金信息
        """

        def __init__(self, code: str, name: str):
            self.code = code
            self.name = name

    def __init__(self):
        self.total = None
        self.task_generator: Generator[NeedCrawledFundModule.NeedCrawledOnceFund] = None

        self.init()

    @abstractmethod
    def init(self):
        return NotImplemented


class FundCrawlingResult:
    """
    基金的最终爬取结果定义
    """

    @unique
    class FundInfoHeader(Enum):
        FUND_CODE = auto(),
        FUND_NAME = auto()

    def __init__(self, fund_info_dict: dict[FundInfoHeader, str]):
        self._fund_info_dict = fund_info_dict


class CrawlingDataModule(ABC):
    """
    数据爬取模块
    包括数据的下载和清洗
    """

    @abstractmethod
    def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund):
        return NotImplemented

    @abstractmethod
    def is_end(self) -> bool:
        return NotImplemented

    @abstractmethod
    def get_an_result(self) -> FundCrawlingResult:
        return NotImplemented


class SaveResultModule(ABC):
    """
    基金数据的保存模块
    """

    @abstractmethod
    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        """
        爬取结果的保存
        """
        return NotImplemented


class TaskManager:
    def __init__(self, need_crawled_fund_module: NeedCrawledFundModule, crawling_data_module: CrawlingDataModule,
                 save_result_module: SaveResultModule):
        """
        :param need_crawled_fund_module: 负责给出 基金爬取任务
        :param crawling_data_module: 负责 数据爬取和清洗
        :param save_result_module: 负责 数据保存
        """
        self._need_crawled_fund_module = need_crawled_fund_module
        self._crawling_data_module = crawling_data_module
        self._save_result_module = save_result_module

    async def get_task_and_crawling(self):
        generator = self._need_crawled_fund_module.task_generator
        next(generator)

        while True:
            try:
                task: NeedCrawledFundModule.NeedCrawledOnceFund = next(generator)
            except StopIteration:
                break
            await self._crawling_data_module.do_crawling(task)

    async def get_result_and_save(self):
        while not self._crawling_data_module.is_end():
            result: FundCrawlingResult = self._crawling_data_module.get_an_result()
            self._save_result_module.save_result(result)

    async def run(self) -> NoReturn:
        """
        爬取主流程
        从 基金爬取任务模块 将任务传递给 数据爬取和清洗模块
        从 数据爬取和清洗模块 将结果传递给 数据保存模块
        两部分的任务都是阻塞的（主要会阻塞在 数据爬取和清洗）
        """
        async with TaskGroup() as tg:
            await tg.create_task(self.get_task_and_crawling())
            await tg.create_task(self.get_result_and_save())
