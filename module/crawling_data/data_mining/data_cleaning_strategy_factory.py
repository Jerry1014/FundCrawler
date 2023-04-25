"""
数据清洗策略
"""
from abc import ABC, abstractmethod
from typing import NoReturn

from module.crawling_data.data_mining.data_mining_type import PageType
from module.crawling_data.data_mining.impl.overview import OverviewDataCleaningStrategy


class DataCleaningStrategy(ABC):
    """
    数据清洗策略
    """

    @abstractmethod
    def fill_result(self, response, result) -> NoReturn:
        return NotImplemented


class DataCleaningStrategyFactory:
    """
    数据清洗策略工厂
    """

    _strategy_dict: dict[PageType, DataCleaningStrategy] = {
        PageType.OVERVIEW, OverviewDataCleaningStrategy()
    }

    @classmethod
    def get_strategy(cls, page_type: PageType) -> DataCleaningStrategy:
        """
        获取对应页面数据的清洗策略
        """
        return cls._strategy_dict.get(page_type)
