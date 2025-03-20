"""
标准的爬取流程
爬取所有的基金信息，并将结果保证到文件中
"""
import logging

from module.crawling_target.get_fund_by_web import GetFundByWeb
from module.data_mining.data_mining import DataMining
from module.process_manager import TaskManager
from module.saving_result.save_result_2_file import SaveResult2CSV
from utils.constants import log_format

if __name__ == '__main__':
    # 日志级别
    logging.basicConfig(level=logging.WARN, format=log_format)

    TaskManager(GetFundByWeb()
                , DataMining()
                , SaveResult2CSV()).run()
