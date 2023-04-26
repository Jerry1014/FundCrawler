"""
通过基金网站的全部基金列表，获取到 初始的，需要爬取的基金任务
"""
import re
from typing import NoReturn

import requests

from process_manager import NeedCrawledFundModule


class GetNeedCrawledFundByWeb(NeedCrawledFundModule):

    def init_generator(self) -> NoReturn:
        # 全部（不一定可购） 的开放式基金
        url = 'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?page=1,&onlySale=0'
        page = requests.get(url, headers={
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/78.0.3904.108 Safari/537.36'})

        # 基金目录
        fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
        self.total = len(fund_list)

        self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(i[1:7], i[10:-1]) for i in fund_list)


class GetNeedCrawledFundByWeb4Test(NeedCrawledFundModule):
    """
    测试用的 基金任务 提供者
    """
    test_case_num = 2

    def init_generator(self) -> NoReturn:
        # 全部（不一定可购） 的开放式基金
        url = f'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?page=1,{self.test_case_num}&onlySale=0'
        page = requests.get(url, headers={
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/78.0.3904.108 Safari/537.36'})

        # 基金目录
        fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
        self.total = len(fund_list)

        self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(i[1:7], i[10:-1]) for i in fund_list)
