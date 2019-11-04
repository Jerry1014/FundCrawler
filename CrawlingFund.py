# -*- coding:UTF-8 -*-
import re
import time
from collections import OrderedDict
from multiprocessing import Queue, Event

from eprogress import LineProgress

from CrawlingWebpage import GetPageByWebWithAnotherProcessAndMultiThreading
from ProvideTheListOfFund import GetFundListByWebForTest


class FundInfo:
    """
    基金信息
    """

    def __init__(self):
        self._fund_info = OrderedDict()

    def get_header(self):
        return ','.join(self._fund_info.keys())

    def get_info(self):
        return ','.join(self._fund_info.values())

    def get_fund_kind(self):
        try:
            return self._fund_info['fund_kind']
        except KeyError:
            return 'Unknown'

    def set_fund_info(self, key, value):
        self._fund_info[key] = value

    def __repr__(self):
        return ' | '.join(str(key) + ',' + str(value) for key, value in self._fund_info.items())


# 这个是根据网页的html解析的顺序，若需要指定在爬取结果中的顺序，请修改index_of_header
index_header = ['近1月', '近1年', '近3月', '近3年', '近6月', '成立来']
guaranteed_header = ['保本期收益', '近6月', '近1月', '近1年', '近3月', '近3年']
index_of_header = [0, 2, 4, 1, 3, 5]


def parse_fund_info():
    """
    对基金信息界面进行解析 通过send(page_context)来获得解析
    :return: 迭代器 FundInfo
    """
    page_context, fund_info = yield

    while True:
        # 清洗基金收益率 此为指数/股票型的基金
        achievement_re = re.search(r'：.*?((?:-?\d+\.\d{2}%)|--).*?'.join(index_header + ['基金类型']), page_context)
        if not achievement_re:
            # 保本型基金
            achievement_re = re.search(r'：.*?((-?\d+\.\d{2}%)|--).*?'.join(guaranteed_header + ['基金类型']), page_context)
            if re.search('封闭期', page_context) or re.search('本基金已终止', page_context):
                # 基金为有封闭期的固定收益基金或已终止的基金
                fund_info.set_fund_info('fund_kind', 'close')
            if achievement_re:
                fund_info.set_fund_info('fund_kind', 'guaranteed')
                for header, value in zip(guaranteed_header, achievement_re.groups()):
                    fund_info.set_fund_info(header, value)
            else:
                fund_info.set_fund_info('fund_kind', 'Unknown')
        else:
            fund_info.set_fund_info('fund_kind', 'index')
            for header, value in zip(index_header, achievement_re.groups()):
                fund_info.set_fund_info(header, value)

        page_context, fund_info = yield fund_info


def write_to_file():
    """
    将爬取到的信息逐行保存到文件 保存内容通过send()发送 (一行内容，文件名)
    当文件名为None时，保存文件过程结束，释放所有句柄，并抛出StopIteration
    """
    # todo 将文件的初始化移到此函数中处理
    filename_handle = dict()
    line_context_and_filename = yield
    while line_context_and_filename[1] is not None:
        if line_context_and_filename[1] not in filename_handle.keys():
            f = open(line_context_and_filename[1], 'a')
            filename_handle[line_context_and_filename[1]] = f
        else:
            f = filename_handle[line_context_and_filename[1]]

        f.write(line_context_and_filename[0])
        f.write('\n')
        line_context_and_filename = yield

    for i in filename_handle.values():
        i.close()


def get_past_performance(all_fund_generator_or_list, first_crawling=True):
    """
    在简单基金目录的基础上，爬取所有基金的信息
    :param all_fund_generator_or_list: 要爬取的基金目录(generator) 也可以直接是列表('基金代码,基金名称')(list)
    :param first_crawling: 是否是第一次爬取，这决定了是否会重新写保存文件（清空并写入列索引）
    :return 爬取失败的('基金代码,基金名称')(list)
    """
    header_index_fund = ','.join([index_header[i] for i in index_of_header]) + '\n'
    header_guaranteed_fund = ','.join([guaranteed_header[i] for i in index_of_header]) + '\n'
    # 测试文件是否被占用，并写入列索引
    try:
        if first_crawling:
            with open(all_index_fund_with_msg_filename, 'w') as f:
                f.write(header_index_fund)
            with open(all_guaranteed_fund_with_msg_filename, 'w') as f:
                f.write(header_guaranteed_fund)
    except IOError:
        print('文件' + all_fund_filename + '无法打开')
        return

    # 对于输入为list的情况，将其构造成迭代器
    if type(all_fund_generator_or_list) == list:
        all_fund_generator_or_list = (i for i in all_fund_generator_or_list)
    elif str(type(all_fund_generator_or_list)) != "<class 'generator'>":
        raise AttributeError

    # 进度条
    line_progress = LineProgress(title='爬取进度')
    # 爬取结果队列
    input_queue = Queue()
    result_queue = Queue()
    finish_sign = Event()
    GetPageByWebWithAnotherProcessAndMultiThreading(input_queue, result_queue, finish_sign).start()

    # todo 爬取出错时，结束爬虫进程
    while True:
        # todo 进度条
        try:
            tem = next(all_fund_generator_or_list).split(',')
            code, name = tem
        except StopIteration:
            break

        tem_fund_info = FundInfo()
        tem_fund_info.set_fund_info('name', name)
        tem_fund_info.set_fund_info('code', code)
        input_queue.put(('http://fund.eastmoney.com/' + code + '.html', tem_fund_info))

    # todo 暂时不做基金经理这部分
    finish_sign.set()
    while finish_sign.is_set(): pass

    web_page_parse = parse_fund_info()
    next(web_page_parse)
    while result_queue.qsize() > 0:
        print(web_page_parse.send(result_queue.get()[1:]))

    line_progress.update(99)
    # todo 保存文件
    line_progress.update(100)
    # todo 对第一次爬取失败的基金的处理


if __name__ == '__main__':
    start_time = time.time()
    # 文件名设置
    all_fund_filename = 'fund_simple.csv'  # 基金目录
    all_index_fund_with_msg_filename = 'index_fund_with_achievement.csv'  # 指数/股票型基金完整信息
    all_guaranteed_fund_with_msg_filename = 'guaranteed_fund_with_achievement.csv'  # 保本型基金完整信息
    fund_need_handle_filename = 'fund_need_handle.csv'  # 保存需要重新爬取的基金

    # todo 对网络环境的判断与测试

    # just for test
    get_past_performance(GetFundListByWebForTest().get_fund_list())

    print("\n爬取总用时", time.time() - start_time)
