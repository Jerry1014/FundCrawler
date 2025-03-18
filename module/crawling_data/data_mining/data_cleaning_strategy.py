from abc import ABC, abstractmethod
from typing import NoReturn

from module.fund_context import FundContext


class DataCleaningStrategy(ABC):
    """
    数据清洗策略
    """

    @abstractmethod
    def build_url(self, fund_code: str) -> str:
        return NotImplemented

    @abstractmethod
    def fill_result(self, response, result: FundContext) -> NoReturn:
        return NotImplemented
