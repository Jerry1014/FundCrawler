# -*- coding:UTF-8 -*-
import re
import time
from collections import OrderedDict
from multiprocessing import Queue, Event
from os import makedirs
from os.path import exists

from eprogress import LineProgress
from requests.exceptions import RequestException

from CrawlingWebpage import GetPageByWebWithAnotherProcessAndMultiThreading
from ProvideTheListOfFund import GetFundList, GetFundListByWeb, GetFundListTest

# 测试标记
if_test = False


class FundInfo:
    """
    基金信息
    """

    def __init__(self):
        self.fund_kind = 'Unknown'
        self._fund_info = OrderedDict()
        self._manager_info = dict()
        self.next_step = 'parsing_fund'
        self.manager_need_process_list = list()

    def get_header(self):
        return ','.join(self._fund_info.keys())

    def get_info(self, index=None, missing='??'):
        if index is None:
            return ','.join(list(self._fund_info.values()) + ['/'.join(self._manager_info.keys()),
                                                              '/'.join(self._manager_info.values())])
        else:
            return ','.join(self._get_info(i, missing) for i in index)

    def _get_info(self, index, missing):
        if index in self._fund_info.keys():
            return self._fund_info[index]
        elif index == '基金经理' or index == '总任职时间':
            return '/'.join(self._manager_info.keys()) if index == '基金经理' else '/'.join(self._manager_info.values())
        else:
            return str(missing)

    def set_fund_info(self, key, value):
        self._fund_info[key] = str(value)

    def set_manager_info(self, key, value):
        self._manager_info[key] = value

    def __repr__(self):
        return self.get_info()


index_header = ['近1月', '近1年', '近3月', '近3年', '近6月', '成立来']
guaranteed_header = ['保本期收益', '近6月', '近1月', '近1年', '近3月', '近3年']
capital_preservation_header = ['最近约定年化收益率']
index_kind = ['股票型', '混合型', '债券型', '定开债券', '股票指数', '联接基金', 'QDII-指数', 'QDII', '混合-FOF', '货币型',
              '理财型', '分级杠杆', 'ETF-场内', '债券指数']
guaranteed_kind = ['保本型']
closed_period_kind = ['固定收益']
result_dir = './results/'


def parse_fund_info():
    """
    对基金信息界面进行解析 通过send(page_context,fund_info)来获得解析
    未来将整合进fund_info类中
    :return: 迭代器 FundInfo
    """
    page_context, fund_info = yield

    while True:
        # 基金分类
        fund_info.fund_kind = re.search(r'基金类型：(?:<a.*?>|)(.*?)[<&]', page_context)
        fund_info.fund_kind = fund_info.fund_kind.group(1) if fund_info.fund_kind is not None else "解析基金类型失败"
        fund_info.set_fund_info('基金规模', re.search(r'基金规模</a>：((?:\d+(?:\.\d{2}|)|--)亿元.*?)<', page_context).group(1))

        if fund_info.fund_kind in index_kind:
            achievement_re = re.search(r'：.*?((?:-?\d+\.\d{2}%)|--).*?'.join(index_header + ['基金类型']), page_context)
        elif fund_info.fund_kind in guaranteed_kind:
            achievement_re = re.search(r'(?:：|).*?((?:-?\d+\.\d{2}%)|--).*?'.join(guaranteed_header + ['基金类型']),
                                       page_context)
        elif fund_info.fund_kind in closed_period_kind:
            achievement_re = re.search(r'最近约定年化收益率(?:<.*?>)(-?\d+\.\d{2}%)<', page_context)
        else:
            print(f'出现无解析方法的基金种类 {fund_info}')
            achievement_re = None

        if achievement_re is not None:
            # 清洗基金收益率
            if fund_info.fund_kind in index_kind:
                tem_header = index_header
            elif fund_info.fund_kind in guaranteed_kind:
                tem_header = guaranteed_header
            else:
                tem_header = capital_preservation_header
            for header, value in zip(tem_header, achievement_re.groups()):
                fund_info.set_fund_info(header, value)
            fund_info.next_step = 'parsing_manager'
            # 清洗 基金经理在本基金的任职时间和收益率 和基金经理信息及其主页链接
            fund_manager_detail = re.search(r'</td> {2}<td class="td03">(.+?|-)</td> {2}<td class="td04 bold (?:ui-colo'
                                            r'r-(?:red|green)|)">(-?\d+\.\d{2}%|--)</td></tr>', page_context)
            if fund_manager_detail is not None:
                fund_info.set_fund_info('任职时间', fund_manager_detail.group(1))
                fund_info.set_fund_info('任期收益', fund_manager_detail.group(2))
                fund_managers = re.findall(r'(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)',
                                           re.search(r'<td class="td02">(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)+',
                                                     page_context).group(0))
                fund_info.manager_need_process_list = fund_managers
            else:
                print(f'出现无法解析基金经理的基金 {fund_info}')
                fund_info.next_step = 'writing_file'
        else:
            print(f'出现无法解析收益的基金 {fund_info}')
            fund_info.next_step = 'writing_file'

        page_context, fund_info = yield fund_info


def parse_manager_info():
    """
    对基金经理的信息进行解析 通过send(page_context,fund_info)来获得解析
    """
    # 挖坑 下次重构获取基金经理名称，与爬取部分做解耦
    page_context, fund_info = yield
    fund_info: FundInfo
    while True:
        manager_info = re.search('<span>累计任职时间：</span>(.*?)<br />', page_context)
        fund_info.set_manager_info(fund_info.manager_need_process_list.pop()[1], manager_info.group(1))
        if len(fund_info.manager_need_process_list) == 0:
            fund_info.next_step = 'writing_file'
        page_context, fund_info = yield fund_info


def write_to_file(first_crawling):
    """
    将爬取到的信息逐行保存到文件 保存内容通过send()发送 (FundInfo)
    当基金类型为None时，保存文件过程结束，释放所有句柄，并抛出StopIteration
    :param first_crawling: 是否是第一次爬取，这决定了是否会重新写保存文件（清空并写入列索引）
    """
    open_mode = 'w' if first_crawling else 'a'
    filename_handle = dict()
    if not exists(result_dir):
        makedirs(result_dir)
    # 保存文件的第一行（列索引）
    write_format_of_index = ['基金名称', '基金代码', '基金规模', '近1月', '近3月', '近6月', '近1年', '近3年', '成立来', '基金经理', '任职时间', '任期收益',
                             '总任职时间']
    write_format_of_guaranteed = ['基金名称', '基金代码', '基金规模', '保本期收益', '近1月', '近3月', '近6月', '近1年', '近3年', '基金经理', '任职时间',
                                  '任期收益', '总任职时间']
    write_format_of_capital_preservation = ['基金名称', '基金代码', '基金规模', '最近约定年化收益率', '基金经理', '任职时间', '任期收益', '总任职时间']

    fund_info = yield
    while fund_info is not None:
        if fund_info.fund_kind not in filename_handle.keys():
            # 此基金类型的文件尚未打开过
            f = open(result_dir + fund_info.fund_kind + '.csv', open_mode)
            filename_handle[fund_info.fund_kind] = f
            if fund_info.fund_kind in index_kind:
                header = ','.join(write_format_of_index) + '\n'
            elif fund_info.fund_kind in guaranteed_kind:
                header = ','.join(write_format_of_guaranteed) + '\n'
            else:
                header = ','.join(write_format_of_capital_preservation) + '\n'
            f.write(header)
        else:
            f = filename_handle[fund_info.fund_kind]

        # 按照列索引，取出基金数据并写入文件
        if fund_info.fund_kind in index_kind:
            index = write_format_of_index
        elif fund_info.fund_kind in guaranteed_kind:
            index = write_format_of_guaranteed
        else:
            index = write_format_of_capital_preservation
        f.write(fund_info.get_info(index))
        f.write('\n')
        fund_info = yield

    for i in filename_handle.values():
        i.close()


def crawling_fund(fund_list_class: GetFundList, first_crawling=True):
    """
    在简单基金目录的基础上，爬取所有基金的信息
    :param fund_list_class: 提供要爬取的基金目录的类
    :param first_crawling: 是否是第一次爬取，这决定了是否会重新写保存文件（清空并写入列索引）
    :return 爬取失败的('基金代码,基金名称')(list)
    """
    # 进度条 基金总数 爬取进度
    line_progress = LineProgress(title='爬取进度')
    cur_process = 0
    # 爬取输入、输出队列，输入结束事件，爬取核心
    input_queue = Queue()
    result_queue = Queue()
    finish_sign = Event()
    network_health = Event()
    crawling_core = GetPageByWebWithAnotherProcessAndMultiThreading(input_queue, result_queue, finish_sign,
                                                                    network_health)
    crawling_core.start()

    # 爬取出错时，不会自动结束爬虫进程
    fund_list = fund_list_class.get_fund_list()
    num_of_fund = fund_list_class.sum_of_fund
    having_fund_need_to_crawl = True

    # 未来有计划将解析部分分离
    fund_web_page_parse = parse_fund_info()
    manager_web_page_parse = parse_manager_info()
    write_file = write_to_file(first_crawling)
    next(fund_web_page_parse)
    next(manager_web_page_parse)
    next(write_file)
    if_first_show_network_problem = True
    while True:
        if network_health.is_set():
            if if_first_show_network_problem:
                print('如果此条提示持续出现，请检查当前的网络状态')
                if_first_show_network_problem = False
        elif not if_first_show_network_problem:
            if_first_show_network_problem = True

        # 根据短路原则，首先是是否还有要爬取的基金，然后是判断需要解析的数据量（控制内存），最后才是查看输入队列的情况
        while having_fund_need_to_crawl and result_queue.qsize() < 100 and input_queue.qsize() < 10:
            try:
                code, name = next(fund_list).split(',')
            except StopIteration:
                having_fund_need_to_crawl = False
                break
            tem_fund_info = FundInfo()
            tem_fund_info.set_fund_info('基金名称', name)
            tem_fund_info.set_fund_info('基金代码', code)
            input_queue.put(('http://fund.eastmoney.com/' + code + '.html', tem_fund_info))

        # 优先补充输入队列，保证爬取的速度，再处理需要解析的数据
        while (input_queue.qsize() > 5 or not having_fund_need_to_crawl) and result_queue.qsize():
            a_result = result_queue.get()
            # 若上次的爬取失败了，则重试，未对一直失败的进行排除
            if a_result[0] == 'error':
                input_queue.put(a_result[1:])
            else:
                if a_result[2].next_step == 'parsing_fund':
                    new_fund_info: FundInfo = fund_web_page_parse.send(a_result[1:])
                    if new_fund_info.next_step == 'parsing_manager':
                        input_queue.put((new_fund_info.manager_need_process_list[-1][0], new_fund_info))
                    else:
                        result_queue.put((None, None, new_fund_info))
                elif a_result[2].next_step == 'parsing_manager':
                    new_fund_info: FundInfo = manager_web_page_parse.send(a_result[1:])
                    if new_fund_info.next_step == 'parsing_manager':
                        input_queue.put((new_fund_info.manager_need_process_list[-1][0], new_fund_info))
                    else:
                        result_queue.put((None, None, new_fund_info))

                elif a_result[2].next_step == 'writing_file':
                    write_file.send(a_result[2])
                    cur_process += 1
                    line_progress.update(100 * cur_process / num_of_fund)
                else:
                    print(f'请检查FundInfo的next_step(此处为{a_result[2].next_step})设置，出现了未知的参数')

        # 完成所有任务判断
        if not having_fund_need_to_crawl and input_queue.qsize() == 0 and result_queue.qsize() == 0:
            # 下次要将这里的等待时间设置为全局变量TIME_OUT，这样才能保证所有的数据都处理完毕
            time.sleep(1)
            if not having_fund_need_to_crawl and input_queue.qsize() == 0 and result_queue.qsize() == 0:
                break

    finish_sign.set()
    # 挖坑 对第一次爬取失败的基金的处理


if __name__ == '__main__':
    start_time = time.time()
    try:
        # 在这里更换提供基金列表的类，以实现从文件或者其他方式来指定要爬取的基金
        if if_test:
            crawling_fund(GetFundListTest())
        else:
            crawling_fund(GetFundListByWeb())
    except RequestException:
        print('网络错误')
    print(f'\n爬取总用时{time.time() - start_time} s')
