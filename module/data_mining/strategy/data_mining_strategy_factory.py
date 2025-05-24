"""
数据清洗策略
"""

from module.data_mining.strategy.data_mining_strategy import DataCleaningStrategy
from module.data_mining.strategy.fund_return import ReturnStrategy
from module.data_mining.strategy.manager import ManagerStrategy
from module.data_mining.strategy.morningstar import MorningstarStrategy
from module.data_mining.strategy.overview import OverviewStrategy
from module.data_mining.strategy.risk import RiskStrategy
from utils.constants import PageType


class DataCleaningStrategyFactory:
    """
    数据清洗策略工厂
    """

    _strategy_dict: dict[PageType, DataCleaningStrategy] = {
        PageType.OVERVIEW: OverviewStrategy(),
        PageType.MANAGER: ManagerStrategy(),
        PageType.MORNINGSTAR: MorningstarStrategy(),
        PageType.RETURN: ReturnStrategy(),
        PageType.RISK: RiskStrategy(),
    }

    @classmethod
    def get_strategy(cls, page_type: PageType) -> DataCleaningStrategy:
        """
        获取对应页面数据的清洗策略
        """
        return cls._strategy_dict[page_type]
