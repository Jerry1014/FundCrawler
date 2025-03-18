from abc import abstractmethod, ABC
from typing import NoReturn, List

from module.downloader.download_by_requests import FundRequest
from module.fund_context import FundContext


class DataMiningModule(ABC):
    """
    爬取到的原始结果 解析
    """

    @abstractmethod
    def summit_context(self, context: FundContext) -> List[FundRequest] | NoReturn:
        """
        提交基金信息的上下文
        当返回request列表时，代表还需要爬取对应的网站(并将res添加到context中) / 没有返回时代表没有需要爬取的数据了(结果是最终结果)
        """
        return NotImplemented
