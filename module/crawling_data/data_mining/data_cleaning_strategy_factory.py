"""
数据清洗策略
"""

from module.crawling_data.data_mining.data_cleaning_strategy import DataCleaningStrategy
from module.crawling_data.data_mining.data_mining_type import PageType
from module.crawling_data.data_mining.impl.increase import RiseStrategy
from module.crawling_data.data_mining.impl.manager import ManagerStrategy
from module.crawling_data.data_mining.impl.overview import OverviewStrategy
from module.crawling_data.data_mining.impl.sharpe import MetricsStrategy


class DataCleaningStrategyFactory:
    """
    数据清洗策略工厂
    """

    _strategy_dict: dict[PageType, DataCleaningStrategy] = {
        PageType.OVERVIEW: OverviewStrategy(),
        PageType.MANAGER: ManagerStrategy(),
        PageType.METRICS: MetricsStrategy(),
        PageType.INCREASE: RiseStrategy()
    }

    @classmethod
    def get_strategy(cls, page_type: PageType) -> DataCleaningStrategy:
        """
        获取对应页面数据的清洗策略
        """
        return cls._strategy_dict.get(page_type)
