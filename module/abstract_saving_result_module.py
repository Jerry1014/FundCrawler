from abc import abstractmethod, ABC

from module.fund_context import FundContext


class SavingResultModule(ABC):
    """
    基金数据的保存模块
    """

    @abstractmethod
    def save_result(self, result: FundContext) -> None:
        """
        爬取结果的保存
        """
        pass

    def exit(self) -> None:
        """
        对于运行中止时需要后处理的场景
        """
        pass
