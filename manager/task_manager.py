"""
爬取核心
爬取过程的管理
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


class Pair:
    def __init__(self, key, value):
        self._key = key
        self._value = value


class FundCrawlingResult:
    """
    基金的最终爬取结果
    """

    def __init__(self, fund_info_list: list[Pair]):
        self._fund_info_list = fund_info_list


class CrawlingData:
    pass


class SaveResult:
    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        """
        爬取结果的保存
        """
        return NotImplemented


class TaskManager:
    def __init__(self, need_crawled_fund_generator: NeedCrawledFund, crawling_data_generator: CrawlingData,
                 save_result: SaveResult):
        """
        :param need_crawled_fund_generator: 获取需要 爬取的基金 的生成器
        :param crawling_data_generator: 数据爬取、清洗的生成器
        :param save_result: 数据保存类
        """
        self._need_crawled_fund_generator = need_crawled_fund_generator
        self._crawling_data_generator = crawling_data_generator
        self._save_result = save_result

    def run(self):
        # todo 爬取主流程
        pass
