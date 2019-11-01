# -*- coding:UTF-8 -*-
import random
import re
import threading
import time
from queue import Queue

import requests
from eprogress import LineProgress

from FakeUA import fake_ua


class FundManager:
    def __init__(self, position_time=0, term_return=0, this_fund_return=0):
        self.position_time = position_time
        self.term_return = term_return
        self.this_fund_return = this_fund_return

    def get_info(self):
        return str(self.position_time) + ',' + str(self.term_return) + ',' + str(self.this_fund_return)


class FundInfo:
    """
    基金信息
    """

    def __init__(self):
        self._fund_info = dict()
        self._manager_info = FundManager()

    def get_header(self):
        return ','.join(self._fund_info.keys())

    def get_info(self):
        return ','.join(self._fund_info.values()) + ',' + self._manager_info.get_info()

    def get_fund_kind(self):
        try:
            return self._fund_info['fund_kind']
        except KeyError:
            return 'Unknown'

    def set_fund_info(self, key, value):
        self._fund_info[key] = value

    def __repr__(self):
        return ' | '.join(str(key) + ',' + str(value) for key, value in self._fund_info.items())


def get_page_context(url, queue: Queue):
    """
    用于爬取页面 爬取特定的网页
    :param queue: 用于保存结果的队列 ('success', page.text)
    :param url:要爬取的url
    """
    # todo 线程数量自动调节
    header = {"User-Agent": fake_ua.random}
    try:
        page = requests.get(url, headers=header, timeout=(30, 70))
        page.encoding = 'utf-8'
        result = ('success', page.text)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError):
        result = ('error', None)
    queue.put(result)


def parse_fund_info():
    """
    对基金信息界面进行解析 通过send(page_context)来获得解析
    :return: 迭代器 FundInfo
    """
    index_header = ['近1月', '近1年', '近3月', '近3年', '近6月', '成立来']
    guaranteed_header = ['保本期收益', '近6月', '近1月', '近1年', '近3月', '近3年']
    page_context = yield

    while True:
        result = FundInfo()
        # 清洗基金收益率 此为指数/股票型的基金
        achievement_re = re.search(r'：.*?((?:-?\d+\.\d{2}%)|--).*?'.join(index_header + ['基金类型']), page_context)
        if not achievement_re:
            # 保本型基金
            achievement_re = re.search(r'：.*?((-?\d+\.\d{2}%)|--).*?'.join(guaranteed_header + ['基金类型']), page_context)
            if re.search('封闭期', page_context) or re.search('本基金已终止', page_context):
                # 基金为有封闭期的固定收益基金或已终止的基金
                result.set_fund_info('fund_kind', 'close')
            if achievement_re:
                result.set_fund_info('fund_kind', 'guaranteed')
                for header, value in zip(guaranteed_header, achievement_re.groups()):
                    result.set_fund_info(header, value)
            else:
                result.set_fund_info('fund_kind', 'Unknown')
        else:
            result.set_fund_info('fund_kind', 'index')
            for header, value in zip(guaranteed_header, achievement_re.groups()):
                result.set_fund_info(header, value)

        # todo 爬取基金经理的个人页面，获得总任职时间
        page_context = yield result


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
    maximum_of_thread = 1
    header_index_fund = '基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,成立来收益,基金经理,本基金任职时间,本基金任职收益,累计任职时间,\n'
    header_guaranteed_fund = '基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,保本期收益,基金经理,本基金任职时间,本基金任职收益,累计任职时间,\n'
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

    # 线程集合 爬取结果队列
    thread = list()
    result_queue = Queue()

    need_to_save_file_event = threading.Event()

    while True:
        i = next(all_fund_generator_or_list)
        try:
            code, name = i.split(',')
            name = name[:-1]
        except ValueError:
            continue

        # 多线程爬取
        t = threading.Thread(target=get_page_context,
                             args=('http://fund.eastmoney.com/' + code + '.html', result_queue))
        thread.append(t)
        t.setName(code + ',' + name)
        t.start()
        for t in thread:
            if not t.is_alive():
                thread.remove(t)

        if len(thread) > maximum_of_thread:
            time.sleep(random.random())
            while len(thread) > maximum_of_thread // 2:
                for t in thread:
                    if not t.is_alive():
                        thread.remove(t)

        line_progress.update(result_queue.qsize() * 100 // sum_of_fund)

    # 等待所有线程执行完毕
    while len(thread) > 0:
        line_progress.update((sum_of_fund - len(thread)) * 100 // sum_of_fund)
        time.sleep(random.random())
        for t in thread:
            if not t.is_alive():
                thread.remove(t)

    line_progress.update(99)
    need_to_save_file_event.set()
    line_progress.update(100)
    print('\n基金信息爬取完成，其中处于封闭期或已终止的基金有' + str(queue_other_fund.qsize()) + '个，爬取失败的有' + str(queue_give_up.qsize()) + '个')
    return list(queue_give_up.get() for i in range(queue_give_up.qsize()))


if __name__ == '__main__':
    start_time = time.time()
    # 文件名设置
    all_fund_filename = 'fund_simple.csv'  # 基金目录
    all_index_fund_with_msg_filename = 'index_fund_with_achievement.csv'  # 指数/股票型基金完整信息
    all_guaranteed_fund_with_msg_filename = 'guaranteed_fund_with_achievement.csv'  # 保本型基金完整信息
    fund_need_handle_filename = 'fund_need_handle.csv'  # 保存需要重新爬取的基金

    # 基金总数 线程数
    sum_of_fund = 0

    # 获取基金过往数据 重新获取第一次失败的数据
    fail_fund_list = get_past_performance(get_fund_list())
    print('\n对第一次爬取失败的基金进行重新爬取\n')
    fail_fund_list = get_past_performance(fail_fund_list, False)
    if fail_fund_list:
        print('仍然还有爬取失败的基金如下')
        print(fail_fund_list)

    print("\n爬取总用时", time.time() - start_time)
