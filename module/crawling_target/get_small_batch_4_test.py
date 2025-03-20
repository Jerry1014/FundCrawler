import re
from typing import List

import requests

from module.fund_context import FundContext
from module.process_manager import CrawlingTargetModule
from utils.fake_ua_getter import singleton_fake_ua


class GetSmallBatch4Test(CrawlingTargetModule):
    """
    测试用的 基金任务 提供者
    指定case数量，小批量进行爬取
    """
    TEST_CASE_NUM: int = 10

    def get_fund_list(self) -> List[FundContext]:
        # 全部（不一定可购） 的开放式基金
        url = f'http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?page=1,{self.TEST_CASE_NUM}&onlySale=0'
        page = requests.get(url, headers={"User-Agent": singleton_fake_ua.get_random_ua()})

        # 基金目录
        fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)

        return [FundContext(i[1:7], i[10:-1]) for i in fund_list]
