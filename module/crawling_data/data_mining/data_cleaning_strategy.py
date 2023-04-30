from abc import ABC, abstractmethod
from typing import NoReturn

from process_manager import FundCrawlingResult


class DataCleaningStrategy(ABC):
    """
    数据清洗策略
    """

    @abstractmethod
    def build_url(self, fund_code: str) -> str:
        return NotImplemented

    @abstractmethod
    def fill_result(self, response, result: FundCrawlingResult) -> NoReturn:
        return NotImplemented
