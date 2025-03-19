import logging
from unittest import TestCase

from module.crawling_target.get_small_batch_need_crawled_fund_4_test import GetSmallBatchNeedCrawledFund4Test
from module.data_mining.data_mining import DataMining
from module.process_manager import TaskManager
from module.saving_result.save_result_2_file import SaveResult2File


class SmokeTestTaskManager(TestCase):
    """
    冒烟测试, 小批量爬取基金信息, 主要用于验证数据的爬取和清洗逻辑
    """

    def test_run(self):
        # 日志级别
        logging.basicConfig(level=logging.INFO)

        # 测试批次大小
        GetSmallBatchNeedCrawledFund4Test.TEST_CASE_NUM = 1000

        TaskManager(GetSmallBatchNeedCrawledFund4Test()
                    , DataMining()
                    , SaveResult2File()).run()
