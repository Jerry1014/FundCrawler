from abc import abstractmethod, ABC
from typing import Optional

from module.downloader.download_by_requests import FundResponse
from module.fund_context import FundContext


class NoNeedException(Exception):
    """
    不满足爬取前提，啥也不用爬
    """
    pass


class DataCleaningStrategy(ABC):
    """
    数据清洗策略
    爬取的页面与解析策略一一对应
    """

    @abstractmethod
    def build_url(self, context: FundContext) -> Optional[str]:
        """
        @return: 需要爬取的url，特殊地，返回None代表需要等待其他策略的爬取结果，暂时无法给出爬取url
        """
        pass

    @abstractmethod
    def fill_result(self, fund_response: FundResponse, context: FundContext) -> None:
        pass
