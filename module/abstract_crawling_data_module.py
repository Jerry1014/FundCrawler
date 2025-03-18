from abc import abstractmethod, ABC
from typing import NoReturn, Optional

from module.fund_info_bo import NeedCrawledOnceFund, FundCrawlingResult


class CrawlingDataModule(ABC):
    """
    数据爬取模块
    包括数据的下载和清洗
    """

    @abstractmethod
    def do_crawling(self, task: NeedCrawledOnceFund) -> NoReturn:
        """
        提交任务
        需要有任务堆积时的阻塞, 以便可以将时间片让出来 处理结果
        """
        return NotImplemented

    @abstractmethod
    def has_next_result(self) -> bool:
        """
        请求已经全部处理完, 且结果都被取出了
        在shutdown后调用
        """
        return NotImplemented

    @abstractmethod
    def get_an_result(self) -> Optional[FundCrawlingResult]:
        """
        (阻塞, 有超时)获取一个处理好的结果
        数据爬取尽量保证成功, 实在失败时 爬取数据为None, 所以不期望的异常
        可以认为只存在于 数据解析 部分, 需要防止一个任务失败导致全部失败
        """
        return NotImplemented

    @abstractmethod
    def shutdown(self):
        """
        请求已经全部传递完了
        """
        return NotImplemented
