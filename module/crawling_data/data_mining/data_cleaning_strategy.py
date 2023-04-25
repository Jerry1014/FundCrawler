from abc import ABC, abstractmethod
from typing import NoReturn


class DataCleaningStrategy(ABC):
    """
    数据清洗策略
    """

    @abstractmethod
    def fill_result(self, response, result) -> NoReturn:
        return NotImplemented
