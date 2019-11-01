"""
向爬虫提供要爬取的基金列表
通过调用实现类的get_fund_list方法获得基金列表的迭代器
"""
import re

import requests

from FakeUA import fake_ua


class GetFundList:
    """
    调用get_fund_list来获得基金列表
    """
    def get_fund_list(self):
        raise NotImplementedError


class GetFundListByWeb(GetFundList):
    """
    调用get_fund_list()来获得基金列表迭代器
    sum_of_fund 为基金总数
    """
    def __init__(self):
        self.sum_of_fund = None

    def get_fund_list(self):
        """
        爬取简单的基金代码名称目录
        :return: iterator str 基金编号，基金名称
        """
        print('获取基金列表中。。。')

        header = {"User-Agent": fake_ua.random}
        page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                            'desc&page=1,9999&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)

        # 基金目录
        fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
        self.sum_of_fund = len(fund_list)
        print('共发现' + str(self.sum_of_fund) + '个基金')

        for i in fund_list:
            yield f'%s,%s' % (i[1:7], i[10:-1])


class GetFundListByWebForTest(GetFundListByWeb):
    """
    测试用
    """
    def __init__(self):
        super().__init__()

    def get_fund_list(self):
        """
        爬取简单的基金代码名称目录
        :return: iterator str 基金编号，基金名称
        """
        my_test_iter = super().get_fund_list()
        for i in range(5):
            yield next(my_test_iter)
        yield StopIteration()
