"""
标准的爬取流程
爬取所有的基金信息，并将结果保证到文件中
"""
from module.crawling_target.get_fund_by_web import GetFundByWeb
from module.data_mining.data_mining import DataMining
from module.process_manager import TaskManager
from module.saving_result.save_result_2_file import SaveResult2File

if __name__ == '__main__':
    TaskManager(GetFundByWeb()
                , DataMining()
                , SaveResult2File()).run()
