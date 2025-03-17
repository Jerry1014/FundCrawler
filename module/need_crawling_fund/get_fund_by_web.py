"""
通过基金网站的全部基金列表，获取到 初始的，需要爬取的基金任务
"""
import re
from typing import NoReturn

import requests

from process_manager import NeedCrawledFundModule
from utils.fake_ua_getter import singleton_fake_ua


class GetNeedCrawledFundByWeb(NeedCrawledFundModule):

    def init_generator(self) -> NoReturn:
        # 全部（不一定可购） 的开放式基金
        url = 'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?page=1,&onlySale=0'
        page = requests.get(url, headers={"User-Agent": singleton_fake_ua.get_random_ua()})

        # 基金目录
        fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
        self.total = len(fund_list)

        self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(i[1:7], i[10:-1]) for i in fund_list)


class GetSmallBatchNeedCrawledFund4Test(NeedCrawledFundModule):
    """
    测试用的 基金任务 提供者
    指定case数量，小批量进行爬取
    """
    test_case_num = 10

    def init_generator(self) -> NoReturn:
        # 全部（不一定可购） 的开放式基金
        url = f'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?page=1,{self.test_case_num}&onlySale=0'
        page = requests.get(url, headers={"User-Agent": singleton_fake_ua.get_random_ua()})

        # 基金目录
        fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
        self.total = len(fund_list)

        self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(i[1:7], i[10:-1]) for i in fund_list)


class GetSpecialNeedCrawledFund(NeedCrawledFundModule):
    """
    测试用的 基金任务 提供者
    """

    def init_generator(self) -> NoReturn:
        # 基金目录
        fund_list = ({'code': '007746', 'name': '华安现金润利'}, {'code': '020282', 'name': '益民优势安享混合C'})
        self.total = len(fund_list)

        self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(
            code=t['code'], name=t['name']) for t in fund_list)
