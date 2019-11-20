# -*- coding:UTF-8 -*-
import re
import time
from collections import OrderedDict
from multiprocessing import Queue, Event
from os import makedirs
from os.path import exists

from eprogress import LineProgress

from CrawlingWebpage import GetPageByWebWithAnotherProcessAndMultiThreading
from ProvideTheListOfFund import GetFundList, GetFundListByWeb, GetFundListTest


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
        elif index == '基金经理' or index == '任职时间':
            return '/'.join(self._manager_info.keys()) if index == '基金经理' else '/'.join(self._manager_info.values())
        else:
            return str(missing)

    def set_fund_info(self, key, value):
        self._fund_info[key] = str(value)

    def set_manager_info(self, key, value):
        self._manager_info[key] = value

    def __repr__(self):
        return self.get_info()


class MyPriorityQueue:
    """
    自定义的多级优先队列结构
    """

    def __init__(self, num_of_queue, default_size=None):
        super().__init__()
        self._queues = [Queue(default_size) for _ in range(num_of_queue if num_of_queue > 1 else 1)]

    def put(self, obj, priority=None):
        """
        按照指定的优先级，将对象放入对应队列中
        :param obj: 放入队列的对象
        :param priority: 优先级 0为最高 为None或超出范围时，选择最低优先级
        """
        priority = -1 if not priority or priority > len(self._queues) else priority
        self._queues[priority].put(obj)

    def get(self):
        """
        获取当前优先级别最高，且最先进入该级别队列的对象
        """
        # 当队列为空时的阻塞
        for i in self._queues:
            if i.qsize() > 0:
                return i.get()
        raise Exception('队列中')


# 这个是根据网页的html解析的顺序，若需要指定在爬取结果中的顺序，请修改index_of_header
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
            achievement_re = re.search(r'(：|).*?((-?\d+\.\d{2}%)|--).*?'.join(guaranteed_header + ['基金类型']),
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
            elif fund_info.fund_kind in guaranteed_header:
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
                fund_info.set_fund_info('working time', fund_manager_detail.group(1))
                fund_info.set_fund_info('rate of return', fund_manager_detail.group(2))
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
    将爬取到的信息逐行保存到文件 保存内容通过send()发送 (一行内容，文件名)
    当基金类型为None时，保存文件过程结束，释放所有句柄，并抛出StopIteration
    :param first_crawling: 是否是第一次爬取，这决定了是否会重新写保存文件（清空并写入列索引）
    """
    # 挖坑 可以加入最终结果计数的功能
    open_mode = 'w' if first_crawling else 'a'
    filename_handle = dict()
    index_of_header = [0, 2, 4, 1, 3, 5]
    if not exists(result_dir):
        makedirs(result_dir)
    line_context_and_fund_kind = yield
    while line_context_and_fund_kind[1] is not None:
        if line_context_and_fund_kind[1] not in filename_handle.keys():
            f = open(result_dir + line_context_and_fund_kind[1] + '.csv', open_mode)
            filename_handle[line_context_and_fund_kind[1]] = f
            if line_context_and_fund_kind[1] in index_kind:
                header = ','.join(
                    ['基金名称', '基金代码', '基金规模'] + index_header
                    + ['基金经理任职时间', '基金经理任职收益', '基金经理总任职时间']) + '\n'
            elif line_context_and_fund_kind[1] in guaranteed_kind:
                header = ','.join(
                    ['基金名称', '基金代码', '基金规模'] + guaranteed_header
                    + ['基金经理任职时间', '基金经理任职收益', '基金经理总任职时间']) + '\n'
            else:
                header = ','.join(['基金名称', '基金代码', '基金规模'] + capital_preservation_header
                                  + ['基金经理任职时间', '基金经理任职收益', '基金经理总任职时间']) + '\n'
            f.write(header)
        else:
            f = filename_handle[line_context_and_fund_kind[1]]

        f.write(line_context_and_fund_kind[0])
        f.write('\n')
        line_context_and_fund_kind = yield

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
    GetPageByWebWithAnotherProcessAndMultiThreading(input_queue, result_queue, finish_sign).start()

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
    while True:
        # todo 任务分配
        while having_fund_need_to_crawl and input_queue.qsize() < 10 and result_queue.qsize() < 100:
            try:
                code, name = next(fund_list).split(',')
            except StopIteration:
                having_fund_need_to_crawl = False
                break
            tem_fund_info = FundInfo()
            tem_fund_info.set_fund_info('name', name)
            tem_fund_info.set_fund_info('code', code)
            input_queue.put(('http://fund.eastmoney.com/' + code + '.html', tem_fund_info))

        while result_queue.qsize():
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
                    write_file.send((str(a_result[2]), a_result[2].fund_kind))
                    cur_process += 1
                    line_progress.update(100 * cur_process / num_of_fund)
                else:
                    print(f'请检查FundInfo的next_step(此处为{a_result[2].next_step})设置，出现了未知的参数')

        # 完成所有任务判断
        if not having_fund_need_to_crawl and input_queue.qsize() == 0 and result_queue.qsize() == 0:
            time.sleep(1)
            if not having_fund_need_to_crawl and input_queue.qsize() == 0 and result_queue.qsize() == 0:
                break

    finish_sign.set()
    # 挖坑 对第一次爬取失败的基金的处理


if __name__ == '__main__':
    start_time = time.time()
    # 挖坑 对网络环境的判断与测试
    crawling_fund(GetFundListTest())
    print(f'\n爬取总用时{time.time() - start_time} s', )
