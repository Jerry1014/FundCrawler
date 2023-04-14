"""
爬取核心
爬取过程的管理 模板方法
"""
from typing import Generator, NoReturn


class NeedCrawledFund:
    """
    通过生成器逐个给出 需要爬取的基金
    """

    def __init__(self, total: int, generator: Generator):
        self.total = total
        self.generator = generator

    class NeedCrawledOnceFund:
        """
        需要爬取的 单个基金信息
        """

        def __init__(self, code: str, name: str):
            self.code = code
            self.name = name


class FundCrawlingResult:
    """
    基金的最终爬取结果
    """

    def __init__(self):
        pass


class TaskManager:
    def get_need_crawled_fund(self) -> NeedCrawledFund:
        """
        获取需要 爬取的基金 的生成器
        :return: 生成器
        """
        return NotImplemented

    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        """
        爬取结果的保存
        """
        return NotImplemented

    def run(self):
        # todo 爬取主流程
        pass
