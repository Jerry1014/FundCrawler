from abc import abstractmethod, ABC
from typing import NoReturn

from module.downloader.download_by_requests_v2 import ResponseV2
from module.fund_context import FundContext


class DataCleaningStrategy(ABC):
    """
    数据清洗策略
    爬取的页面与解析策略一一对应
    """

    @abstractmethod
    def build_url(self, context: FundContext) -> str:
        return NotImplemented

    @abstractmethod
    def fill_result(self, response: ResponseV2, context: FundContext) -> NoReturn:
        return NotImplemented
