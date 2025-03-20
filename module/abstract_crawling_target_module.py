from abc import abstractmethod, ABC
from typing import List

from module.fund_context import FundContext


class CrawlingTargetModule(ABC):
    """
    基金爬取任务模块(基类)
    通过生成器逐个给出 需要爬取的基金
    """

    @abstractmethod
    def get_fund_list(self) -> List[FundContext]:
        """
        获取需要爬取的基金列表
        """
        pass
