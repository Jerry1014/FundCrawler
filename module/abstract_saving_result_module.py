from abc import abstractmethod, ABC
from typing import NoReturn

from module.fund_context import FundContext


class SavingResultModule(ABC):
    """
    基金数据的保存模块
    """

    @abstractmethod
    def save_result(self, result: FundContext) -> NoReturn:
        """
        爬取结果的保存
        """
        return NotImplemented

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
