# -*- coding:UTF-8 -*-
import random
import re
import threading
import time
from queue import Queue

import requests
from eprogress import LineProgress

# 载入随机UA模块，若无，则使用默认的chrome ua
print('正在载入随机UA模块')
try:
    from FakeUA import FakeUA

    ua = FakeUA()
    print('载入完成')
except ModuleNotFoundError:
    print('未能导入随机UA模块FakeUA，使用默认的唯一的chrome UA（可能会影响爬取效果）')


    class TemporaryUA:
        def __init__(self):
            self.random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                          "Chrome/76.0.3809.100 Safari/537.36"


    ua = TemporaryUA()


class FundCrawlerException(Exception):
    pass


class FundInfo:
    """
    基金信息，缺少基金经理部分
    """

    def __init__(self):
        self.info = dict()

    def get_header(self):
        return ','.join(self.info.keys())

    def get_info(self):
        return ','.join(self.info.values())

    def get_fund_kind(self):
        try:
            return self.info['fund_kind']
        except KeyError:
            return 'Unknown'


class FundManagerInfo:
    """
    基金经理信息
    """

    def __init__(self):
        self.info = dict()

    def get_header(self):
        return ','.join(self.info.keys())

    def get_info(self):
        return ','.join(self.info.values())


class FundWithAllInfo:
    """
    完整的基金信息
    """

    def __init__(self, fund_info: FundInfo, fund_manager: FundManagerInfo):
        self.fund_info = fund_info
        self.fund_manager_info = fund_manager

    def get_header(self):
        """
        获取基金信息第一行（索引）
        :return: str 基金信息索引 逗号分割
        """
        return self.fund_info.get_header() + ',' + self.fund_manager_info.get_header()

    def get_info(self):
        """
        获取该基金信息
        :return: str 基金信息 逗号分割
        """
        return self.fund_info.get_info() + ',' + self.fund_manager_info.get_info()


def get_fund_list():
    """
    爬取简单的基金代码名称目录
    :return: iterator str 基金编号，基金名称
    """
    global sum_of_fund
    print('开始爬取。。。')

    header = {"User-Agent": ua.random}
    page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                        'desc&page=1,9999&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)

    # 基金目录
    fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
    sum_of_fund = len(fund_list)
    print('共发现' + str(sum_of_fund) + '个基金')

    for i in fund_list:
        yield f'%s,%s' % (i[1:7], i[10:-1])


def get_page_context():
    """
    用于爬取页面 通过send url(str)爬取特定的网页，发送'kill'时结束
    :return: 迭代器 页面内容(str)
    """
    result = ('init', None)
    url = yield result
    while url != 'kill':
        header = {"User-Agent": ua.random}

        try:
            page = requests.get(url, headers=header, timeout=(30, 70))
            page.encoding = 'utf-8'
            result = ('success', page.text)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError):
            result = ('error', None)
        url = yield result


def get_achievement(re_text, need_to_save_file_event):
    """
    用于爬取基金的收益率和基金经理的信息
    :param need_to_save_file_event: 网络出错事件
    :param re_text: 要清洗网页文本
    :return 二元组（基金信息，基金种类），其中基金信息为list，近1月。3。6。近1年。3。（成立来收益率|保本期收益）（指数股票|保本），
    基金经理/基金经理2.，任职时间，任职收益率，基金经理总任职时间/基金经理2总任职时间.
    基金种类 1:指数/股票基金 2:保本型基金 3:封闭期又或者已终止的 4:未识别的
    """
    fund_kind = None
    achievement = list()

    # 基金的收益率
    achievement_after_clean = re.search(
        '近1月：.*?((-?\d+\.\d{2}%)|--).*?近1年：.*?((-?\d+\.\d{2}%)|--).*?近3月：.*?((-?\d+\.\d{2}%)|--).*?'
        '近3年：.*?((-?\d+\.\d{2}%)|--).*?近6月：.*?((-?\d+\.\d{2}%)|--).*?成立来：.*?((-?\d+\.\d{2}%)|--).*?基金类型', re_text)
    if not achievement_after_clean:
        # 基金为保本型基金
        achievement_after_clean = re.search('保本期收益.*?((-?\d+\.\d{2}%)|--).*?近6月：.*?((-?\d+\.\d{2}%)|--).*?'
                                            '近1月：.*?((-?\d+\.\d{2}%)|--).*?近1年：.*?((-?\d+\.\d{2}%)|--).*?'
                                            '近3月：.*?((-?\d+\.\d{2}%)|--).*?近3年：.*?((-?\d+\.\d{2}%)|--).*?基金类型', re_text)
        if re.search('封闭期', re_text) or re.search('本基金已终止', re_text):
            # 基金为有封闭期的固定收益基金或已终止的基金
            fund_kind = 3
            return list(), fund_kind
        if achievement_after_clean:
            fund_kind = 2
        else:
            fund_kind = 4
            return list(), fund_kind
    else:
        fund_kind = 1

    # 基金经理和个人信息链接
    fund_manager_ifo = re.search('<td class="td02">(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)(?:(?:<a href="(.*?)">(.+?)'
                                 '</a>&nbsp;&nbsp;)|)', re_text)
    # 基金经理在本基金的任职时间和收益率
    fund_manager_detail = re.search(
        '</td>  <td class="td03">(.+?)</td>  <td class="td04 bold (?:ui-color-(?:red|green)|)">'
        '(-?\d+\.\d{2}%)</td></tr>', re_text)

    # 对可能的多个基金经理分别记录以便后续爬取
    manager_link_list = list()
    manager_list = list()
    i = 1
    for j in fund_manager_ifo.groups():
        if j:
            if i % 2 == 0:
                manager_list.append(j)
            else:
                manager_link_list.append(j)
        i += 1
    # 保存基金经理名字
    manager = None
    for i in manager_list:
        if i != manager_list[0]:
            manager += '/' + i
        else:
            manager = i

    try:
        if fund_kind == 1:
            # 保存基金收益率
            achievement.append(achievement_after_clean.group(1))
            achievement.append(achievement_after_clean.group(5))
            achievement.append(achievement_after_clean.group(9))
            achievement.append(achievement_after_clean.group(3))
            achievement.append(achievement_after_clean.group(7))
            achievement.append(achievement_after_clean.group(11))
        else:
            # 保本型基金
            achievement.append(achievement_after_clean.group(5))
            achievement.append(achievement_after_clean.group(9))
            achievement.append(achievement_after_clean.group(3))
            achievement.append(achievement_after_clean.group(7))
            achievement.append(achievement_after_clean.group(11))
            achievement.append(achievement_after_clean.group(1))

        achievement.append(manager)
        achievement.append(fund_manager_detail.group(1))
        achievement.append(fund_manager_detail.group(2))
    except AttributeError:
        # 理论上应该是能有这个多个结果的，如果出错，1.正则表达式 2.特殊网页结构
        fund_kind = 4

    # 分别打开基金经理的个人信息页，保存他们的总任职时间
    manager_link = None
    for i in manager_link_list:
        re_text = get_page_context(i, need_to_save_file_event)
        if re_text:
            fund_manager_appointment_time = re.search('<span>累计任职时间：</span>(.*?)<br />', re_text)
            if i != manager_link_list[0]:
                manager_link += '/' + fund_manager_appointment_time.group(1)
            else:
                manager_link = fund_manager_appointment_time.group(1)
        else:
            fund_kind = 4
            break
    achievement.append(manager_link)

    return achievement, fund_kind


def thread_get_past_performance(code, name, queue_index_fund, queue_guaranteed_fund, queue_other_fund, queue_give_up,
                                need_to_save_file_event):
    """
    爬取单个基金信息的线程
    :param code: 基金代号
    :param name: 基金名称
    :param queue_index_fund: 保存爬取的指数/股票基金
    :param queue_guaranteed_fund: 保存爬取的保本基金
    :param queue_other_fund: 保存封闭期或者已终止的基金代码
    :param queue_give_up: 保存放弃爬取的基金代码
    :param need_to_save_file_event: 爬取时，发生网路错误事件
    """
    # 临时接受爬取函数返回的数据
    tem = list()

    re_text = get_page_context('http://fund.eastmoney.com/' + code + '.html', need_to_save_file_event)

    if re_text is None:
        # 重复出错3次后，放弃。
        fund_kind = 4
    else:
        tem, fund_kind = get_achievement(re_text, need_to_save_file_event)
    fund_all_msg = [code, name] + list(tem)

    if fund_kind == 1:
        # 指数型/股票型等基金
        queue_index_fund.put(fund_all_msg)
    elif fund_kind == 2:
        # 保本型基金
        queue_guaranteed_fund.put(fund_all_msg)
    elif fund_kind == 3:
        # 有封闭期的固定收益基金或已终止的基金
        queue_other_fund.put(code + ',' + name + '\n')
    else:
        # 爬取失败
        queue_give_up.put(code + ',' + name + '\n')


def get_past_performance(all_fund_generator_or_list, first_crawling=True):
    """
    在简单基金目录的基础上，爬取所有基金的信息
    :param all_fund_generator_or_list: 要爬取的基金目录(generator) 也可以直接是列表('基金代码,基金名称')(list)
    :param first_crawling: 是否是第一次爬取，这决定了是否会重新写保存文件（清空并写入列索引）
    :return 爬取失败的('基金代码,基金名称')(list)
    """
    maximum_of_thread = 1
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

    # 对于输入为list的情况，构造成迭代器
    if type(all_fund_generator_or_list) == list:
        all_fund_generator_or_list = (i for i in all_fund_generator_or_list)
    elif str(type(all_fund_generator_or_list)) != "<class 'generator'>":
        raise AttributeError

    # 进度条
    line_progress = LineProgress(title='爬取进度')

    # 线程集合
    thread = list()
    # 接受线程爬取的信息
    queue_index_fund = Queue()
    queue_guaranteed_fund = Queue()
    queue_other_fund = Queue()
    queue_give_up = Queue()

    num_of_previous_completed = 0
    num_of_last_addition_of_completed_fund_this_time = 0
    num_of_last_addition_give_up_fund = 0
    num_of_last_addition_other_fund = 0
    need_to_save_file_event = threading.Event()

    def save_file():
        nonlocal maximum_of_thread, num_of_last_addition_of_completed_fund_this_time, num_of_previous_completed, \
            num_of_last_addition_give_up_fund, num_of_last_addition_other_fund
        # 写入文件和最大线程数减半
        while True:
            need_to_save_file_event.wait()
            maximum_of_thread = (maximum_of_thread // 2) + 1
            num_of_last_addition_of_completed_fund_this_time = 0
            num_of_previous_completed += (queue_index_fund.qsize() + queue_guaranteed_fund.qsize() +
                                          queue_other_fund.qsize() + queue_give_up.qsize() -
                                          num_of_last_addition_give_up_fund - num_of_last_addition_other_fund)
            num_of_last_addition_give_up_fund = queue_give_up.qsize()
            with open(all_index_fund_with_msg_filename, 'a') as f:
                while not queue_index_fund.empty():
                    i = queue_index_fund.get()
                    for j in i:
                        f.write(j + ',')
                    f.write('\n')

            with open(all_guaranteed_fund_with_msg_filename, 'a') as f:
                while not queue_guaranteed_fund.empty():
                    i = queue_guaranteed_fund.get()
                    for j in i:
                        f.write(j + ',')
                    f.write('\n')
            need_to_save_file_event.clear()

    t = threading.Thread(target=save_file)
    t.setDaemon(True)
    t.start()

    try:
        while True:
            i = next(all_fund_generator_or_list)
            try:
                code, name = i.split(',')
                name = name[:-1]
            except ValueError:
                continue

            num_of_completed_this_time = (queue_index_fund.qsize() + queue_guaranteed_fund.qsize() +
                                          queue_other_fund.qsize() + queue_give_up.qsize() -
                                          num_of_last_addition_give_up_fund - num_of_last_addition_other_fund)

            # 多线程爬取
            t = threading.Thread(target=thread_get_past_performance, args=(
                code, name, queue_index_fund, queue_guaranteed_fund, queue_other_fund,
                queue_give_up, need_to_save_file_event))
            thread.append(t)
            t.setName(code + ',' + name)
            t.start()
            for t in thread:
                if not t.is_alive():
                    thread.remove(t)

            if len(thread) > maximum_of_thread:
                time.sleep(random.random())
                if need_to_save_file_event.is_set():
                    while need_to_save_file_event.is_set():
                        pass
                else:
                    maximum_of_thread += num_of_completed_this_time - num_of_last_addition_of_completed_fund_this_time
                    num_of_last_addition_of_completed_fund_this_time = num_of_completed_this_time

                while len(thread) > maximum_of_thread // 2:
                    for t in thread:
                        if not t.is_alive():
                            thread.remove(t)

            line_progress.update((num_of_previous_completed + num_of_completed_this_time) * 100 // sum_of_fund)

    except StopIteration:
        pass

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

    header_index_fund = '基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,成立来收益,基金经理,本基金任职时间,本基金任职收益,累计任职时间,\n'
    header_guaranteed_fund = '基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,保本期收益,基金经理,本基金任职时间,本基金任职收益,累计任职时间,\n'

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
