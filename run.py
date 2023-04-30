"""
标准的爬取流程
爬取所有的基金信息，并将结果保证到文件中
"""

from module.crawling_data.async_crawling_data import AsyncCrawlingData
from module.need_crawling_fund.get_fund_by_web import GetNeedCrawledFundByWeb
from module.save_result.save_result_2_file import SaveResult2File
from process_manager import TaskManager

if __name__ == '__main__':
    manager = TaskManager(GetNeedCrawledFundByWeb()
                          , AsyncCrawlingData()
                          , SaveResult2File())
    manager.run()
