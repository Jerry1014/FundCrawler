# -*- coding:UTF-8 -*-
"""
向爬虫提供要爬取的基金列表
通过调用实现类的get_fund_list方法获得基金列表的迭代器
"""
import re

import requests


class GetFundList:
    """
    调用get_fund_list来获得基金列表，需实现_set_fund_list_generator方法来设置_fund_list_generator的值
    """

    def __init__(self):
        self.sum_of_fund = None
        self._fund_list_generator = None
        self._set_fund_list_generator()

    def get_fund_list(self):
        return self._fund_list_generator

    def _set_fund_list_generator(self):
        raise NotImplementedError()


class GetFundListByWeb(GetFundList):
    """
    调用get_fund_list()来获得基金列表迭代器
    sum_of_fund 为基金总数
    """

    def _set_fund_list_generator(self):
        """
        爬取简单的基金代码名称目录
        :return: iterator str 基金编号，基金名称
        """
        print('获取基金列表中。。。')

        header = {"User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/78.0.3904.108 Safari/537.36'}
        page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                            'desc&page=1,9999&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)

        # 基金目录
        fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
        self.sum_of_fund = len(fund_list)
        print('共发现' + str(self.sum_of_fund) + '个基金')

        self._fund_list_generator = (f'{i[1:7]},{i[10:-1]}' for i in fund_list)


class GetFundListByWebForTest(GetFundListByWeb):
    """
    测试用
    """

    def _set_fund_list_generator(self):
        """
        爬取简单的基金代码名称目录
        :return: iterator str 基金编号，基金名称
        """
        super()._set_fund_list_generator()
        self.sum_of_fund = 5
        self._fund_list_generator = (i for i in list(self._fund_list_generator)[:self.sum_of_fund])


class GetFundListTest(GetFundList):
    """
    提供测试列表
    """

    def _set_fund_list_generator(self):
        test_list = ['000452,test-multi-manager', '180002,test-guaranteed']
        self.sum_of_fund = len(test_list)
        self._fund_list_generator = (i for i in test_list)


class GetFundListFromList(GetFundList):
    """
    传入一个List，并构造为迭代器
    """

    def __init__(self, provide_list):
        super().__init__()
        self.sum_of_fund = len(provide_list)
        self._fund_list_generator = (i for i in provide_list)

    def _set_fund_list_generator(self):
        pass
