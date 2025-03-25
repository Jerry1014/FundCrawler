import logging

from module.crawling_target.get_small_batch_4_test import GetSmallBatch4Test
from module.data_mining.data_mining import DataMining
from module.process_manager import TaskManager
from module.saving_result.save_result_2_file import SaveResult2CSV
from utils.constants import log_format

"""
冒烟测试, 小批量爬取基金信息, 主要用于验证数据的爬取和清洗逻辑
"""
if __name__ == '__main__':
    # 日志级别
    logging.basicConfig(level=logging.INFO, format=log_format)

    # 测试批次大小
    GetSmallBatch4Test.TEST_CASE_NUM = 100

    TaskManager(GetSmallBatch4Test()
                , DataMining()
                , SaveResult2CSV()).run()
