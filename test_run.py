from unittest import TestCase

from module.crawling_data.async_crawling_data import AsyncCrawlingData
from module.crawling_target.get_fund_by_web import GetSmallBatchNeedCrawledFund4Test
from module.process_manager import TaskManager
from module.saving_result.save_result_2_file import SaveResult2File


class SmokeTestTaskManager(TestCase):
    """
    冒烟测试, 小批量爬取基金信息, 主要用于验证数据的爬取和清洗逻辑
    """

    def test_run(self):
        manager = TaskManager(GetSmallBatchNeedCrawledFund4Test()
                              , AsyncCrawlingData()
                              , SaveResult2File())
        manager.run()
