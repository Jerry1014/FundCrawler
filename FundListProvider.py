# -*- coding:UTF-8 -*-
"""
向爬虫提供要爬取的基金列表
"""
import random
import re
import traceback

import requests


class GetFundList:
    """
    获取需要爬取的基金列表
    需要实现 _set_fund_list_generator(self, *args, **kwargs) 方法
    """

    def __init__(self, **kwargs):
        # 基金数量
        self._sum_of_fund = None
        # 基金迭代器 (基金code, 基金name)
        self._fund_list_generator = None

        try:
            print('获取基金列表中。。。')
            self._set_fund_list_generator()
            assert self._fund_list_generator is not None, '_fund_list_generator是None，咋回事啊小老弟'
            print('共发现' + str(self._sum_of_fund) + '个基金')
        except:
            print('需要爬取的基金列表获取失败')
            traceback.print_exc()

    def get_fund_list(self):
        return self._fund_list_generator

    def get_sum_of_fund(self):
        return self._sum_of_fund

    def _set_fund_list_generator(self, **kwargs):
        raise NotImplementedError()


class GetFundListFromWeb(GetFundList):
    """
    获取当前网络上最新的基金列表
    """

    def _set_fund_list_generator(self, **kwargs):
        header = {"User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/78.0.3904.108 Safari/537.36'}
        page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                            'desc&page=1,&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)

        # 基金目录
        fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
        self._sum_of_fund = len(fund_list)

        self._fund_list_generator = (f'{i[1:7]},{i[10:-1]}' for i in fund_list)


class GetFundListFromWebForTest(GetFundListFromWeb):
    """
    测试用 选择通过网络爬取的 随机范围的少量基金结果
    """
    TEST_NUM = 5

    def _set_fund_list_generator(self, **kwargs):
        """
        爬取简单的基金代码名称目录
        :return: iterator str 基金编号，基金名称
        """
        super()._set_fund_list_generator()
        from_index = random.randint(0, self._sum_of_fund - self.TEST_NUM)
        fund_list = list(self._fund_list_generator)[from_index:from_index + self.TEST_NUM]
        self._fund_list_generator = (i for i in fund_list)
        self._sum_of_fund = self.TEST_NUM


class GetFundListTest(GetFundList):
    """
    自定义测试列表
    """

    def _set_fund_list_generator(self, **kwargs):
        test_list = ['000452,test-multi-manager', '180002,test-guaranteed']
        self.sum_of_fund = len(test_list)
        self._fund_list_generator = (i for i in test_list)


class GetFundListFromList(GetFundList):
    """
    传入一个List获其他可迭代的类，作为爬取列表
    """

    def _set_fund_list_generator(self, **kwargs):
        provide_list = kwargs['list']
        self._fund_list_generator = provide_list.__iter__()


class GetFundListFromFile(GetFundList):
    """
    传入一个文件，作为爬取列表，未实现
    """

    def _set_fund_list_generator(self, **kwargs):
        filename = kwargs.get('filename')
        raise NotImplementedError()
