"""
爬取核心
对爬取过程的管理
"""
from abc import abstractmethod, ABC
from asyncio import TaskGroup
from collections.abc import Generator
from enum import unique, StrEnum
from typing import NoReturn, Optional


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
        self.task_generator: Optional[Generator[NeedCrawledFundModule.NeedCrawledOnceFund]] = None

        self.init_generator()

    @abstractmethod
    def init_generator(self) -> NoReturn:
        """
        初始化 生成器
        """
        return NotImplemented


class FundCrawlingResult:
    """
    基金的最终爬取结果定义
    """

    @unique
    class FundInfoHeader(StrEnum):
        FUND_CODE = '基金代码',
        FUND_NAME = '基金名称',
        FUND_TYPE = '基金类型',
        FUND_SIZE = '基金规模',
        # 兼容带新场景，A+B -> B -> B+C，此时基金经理为时长最长的B，对应的任职时间为 这三段B连续任职的任职时间
        FUND_MANAGERS = '基金经理',
        LENGTH_OF_TENURE_IN_CUR_FUND = '本基金任职时间',
        TOTAL_LENGTH_OF_TENURE_OF_MANAGER = '总任职时间',
        SHARPE_LAST_THREE_YEARS = '近三年夏普'

    def __init__(self, fund_info_dict: dict[FundInfoHeader, Optional[str]]):
        self.fund_info_dict = fund_info_dict


class CrawlingDataModule(ABC):
    """
    数据爬取模块
    包括数据的下载和清洗
    """

    @abstractmethod
    def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund) -> NoReturn:
        """
        提交任务
        当任务处理不过来时，阻塞
        """
        return NotImplemented

    @abstractmethod
    def empty_request_and_result(self) -> bool:
        """
        请求已经全部处理完, 且结果都被取出了
        """
        return NotImplemented

    @abstractmethod
    def get_an_result(self) -> FundCrawlingResult:
        """
        (阻塞)获取一个处理好的结果
        """
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


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
        # 请求是否都塞到 数据爬取和清洗 模块了,用于判断是否可以结束
        self._has_put_all_request = False

    async def get_task_and_crawling(self):
        generator = self._need_crawled_fund_module.task_generator

        while True:
            try:
                task: NeedCrawledFundModule.NeedCrawledOnceFund = next(generator)
            except StopIteration:
                break
            self._crawling_data_module.do_crawling(task)

        self._has_put_all_request = True

    async def get_result_and_save(self):
        with self._save_result_module:
            while not (self._has_put_all_request and self._crawling_data_module.empty_request_and_result()):
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
            tg.create_task(self.get_task_and_crawling())
            tg.create_task(self.get_result_and_save())
