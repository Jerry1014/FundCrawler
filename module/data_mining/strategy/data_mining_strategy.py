from abc import abstractmethod, ABC

from module.downloader.download_by_requests import FundResponse
from module.fund_context import FundContext


class DataCleaningStrategy(ABC):
    """
    数据清洗策略
    爬取的页面与解析策略一一对应
    """

    @abstractmethod
    def build_url(self, context: FundContext) -> str:
        pass

    @abstractmethod
    def fill_result(self, fund_response: FundResponse, context: FundContext) -> None:
        pass
