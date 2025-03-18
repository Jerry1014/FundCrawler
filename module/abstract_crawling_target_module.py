from abc import abstractmethod, ABC
from collections.abc import Generator
from typing import NoReturn, Optional

from module.fund_info_bo import NeedCrawledOnceFund


class CrawlingTargetModule(ABC):
    """
    基金爬取任务模块(基类)
    通过生成器逐个给出 需要爬取的基金
    """

    def __init__(self):
        self.total = None
        self.task_generator: Optional[Generator[NeedCrawledOnceFund]] = None

        self.init_generator()

    @abstractmethod
    def init_generator(self) -> NoReturn:
        """
        初始化 生成器
        """
        return NotImplemented
