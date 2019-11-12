# -*- coding:UTF-8 -*-
import re
import time
from collections import OrderedDict
from multiprocessing import Queue, Event

from eprogress import LineProgress

from CrawlingWebpage import GetPageByWebWithAnotherProcessAndMultiThreading
from ProvideTheListOfFund import GetFundListByWebForTest, GetFundList


class FundInfo:
    """
    基金信息
    """

    def __init__(self):
        self._fund_info = OrderedDict()
        self._manager_info = dict()
        self.next_step = 'parsing_fund'
        self.manager_need_process_list = list()

    def get_header(self):
        return ','.join(self._fund_info.keys())

    def get_info(self):
        # 未来升级为可定制顺序的
        return ','.join(list(self._fund_info.values()) + ['/'.join(self._manager_info.keys()),
                                                          '/'.join(self._manager_info.values())])

    def get_fund_kind(self):
        try:
            return self._fund_info['fund_kind']
        except KeyError:
            return 'Unknown'

    def set_fund_info(self, key, value):
        self._fund_info[key] = value

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
index_of_header = [0, 2, 4, 1, 3, 5]


def parse_fund_info():
    """
    对基金信息界面进行解析 通过send(page_context,fund_info)来获得解析
    未来将整合进fund_info类中
    :return: 迭代器 FundInfo
    """
    page_context, fund_info = yield

    while True:
        # 基金分类
        achievement_re = re.search(r'：.*?((?:-?\d+\.\d{2}%)|--).*?'.join(index_header + ['基金类型']), page_context)
        if not achievement_re:
            # 保本型基金
            achievement_re = re.search(
                r'：.*?((-?\d+\.\d{2}%)|--).*?'.join(guaranteed_header + ['基金类型']), page_context)
            if achievement_re:
                fund_kind = 'guaranteed'
            elif re.search('封闭期', page_context) or re.search('本基金已终止', page_context):
                # 基金为有封闭期的固定收益基金或已终止的基金
                fund_kind = 'close'
            else:
                # 未知
                fund_kind = 'Unknown'
        else:
            # 指数型、股票型
            fund_kind = 'index'
        fund_info.set_fund_info('fund_kind', fund_kind)

        if fund_kind == 'index' or fund_kind == 'guaranteed':
            # 清洗基金收益率 此为指数/股票型的基金
            tem_header = index_header if fund_kind == 'index' else guaranteed_header
            for header, value in zip(tem_header, achievement_re.groups()):
                fund_info.set_fund_info(header, value)
            fund_info.next_step = 'parsing_manager'
            # 清洗 基金经理在本基金的任职时间和收益率 和基金经理信息及其主页链接
            fund_manager_detail = re.search(r'</td> {2}<td class="td03">(.+?)</td> {2}<td class="td04 bold (?:ui-color-'
                                            r'(?:red|green)|)">(-?\d+\.\d{2}%)</td></tr>', page_context)
            fund_info.set_fund_info('working time', fund_manager_detail.group(1))
            fund_info.set_fund_info('rate of return', fund_manager_detail.group(2))
            fund_managers = re.findall(r'<td class="td02">(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)+', page_context)[0]
            fund_info.manager_need_process_list = [i for i in zip(fund_managers[1::2], fund_managers[0::2])]
        else:
            fund_info.next_step = 'writing_file'

        page_context, fund_info = yield fund_info


def parse_manager_info():
    """
    对基金经理的信息进行解析 通过send(page_context,fund_info)来获得解析
    """
    # 下次重构获取基金经理名称，与爬取部分做解耦
    page_context, fund_info = yield
    fund_info: FundInfo
    while True:
        manager_info = re.search('<span>累计任职时间：</span>(.*?)<br />', page_context)
        fund_info.set_manager_info(fund_info.manager_need_process_list.pop()[0], manager_info.group(1))
        if len(fund_info.manager_need_process_list) == 0:
            fund_info.next_step = 'writing_file'
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


def crawling_fund(fund_list: GetFundList, first_crawling=True):
    """
    在简单基金目录的基础上，爬取所有基金的信息
    :param fund_list: 要爬取的基金目录
    :param first_crawling: 是否是第一次爬取，这决定了是否会重新写保存文件（清空并写入列索引）
    :return 爬取失败的('基金代码,基金名称')(list)
    """
    # 基金信息是按爬取时的清洗顺序决定的，未来可以升级为指定顺序
    header_index_fund = ','.join(['基金名称', '基金代码'] + [index_header[i] for i in index_of_header]) + '\n'
    header_guaranteed_fund = ','.join(['基金名称', '基金代码'] + [guaranteed_header[i] for i in index_of_header]) + '\n'
    # 测试文件是否被占用，并写入列索引
    try:
        if first_crawling:
            with open(index_fund_filename, 'w') as f:
                f.write(header_index_fund)
            with open(guaranteed_fund_filename, 'w') as f:
                f.write(header_guaranteed_fund)
    except IOError:
        print('爬取结果保存文件无法打开')
        return

    # 进度条 基金总数 爬取进度
    line_progress = LineProgress(title='爬取进度')
    # todo 基金总数返回错误
    num_of_fund = 5
    cur_process = 0
    # 爬取输入、输出队列，输入结束事件，爬取核心
    input_queue = Queue()
    result_queue = Queue()
    finish_sign = Event()
    GetPageByWebWithAnotherProcessAndMultiThreading(input_queue, result_queue, finish_sign).start()

    # 爬取出错时，不会自动结束爬虫进程
    fund_list = fund_list.get_fund_list()
    having_fund_need_to_crawl = True
    fund_web_page_parse = parse_fund_info()
    manager_web_page_parse = parse_manager_info()
    next(fund_web_page_parse)
    next(manager_web_page_parse)
    while True:
        # 下列while的两个数字需要微调以达到比较好的效果
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
                        input_queue.put((new_fund_info.manager_need_process_list[-1][1], new_fund_info))
                    else:
                        result_queue.put((None, None, new_fund_info))

                    # 测试用
                    print(f'fund  {new_fund_info}')
                elif a_result[2].next_step == 'parsing_manager':
                    new_fund_info: FundInfo = manager_web_page_parse.send(a_result[1:])
                    if new_fund_info.next_step == 'parsing_manager':
                        input_queue.put((new_fund_info.manager_need_process_list[-1][1], new_fund_info))
                    else:
                        result_queue.put((None, None, new_fund_info))

                    # 测试用
                    print(f'manager  {new_fund_info}')
                elif a_result[2].next_step == 'writing_file':
                    # todo 保存文件，更新进度条

                    # 测试用
                    print(f'write  {a_result[2]}')
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
    # todo 对第一次爬取失败的基金的处理


if __name__ == '__main__':
    start_time = time.time()
    # 文件名设置
    index_fund_filename = 'index_fund_with_achievement.csv'  # 指数/股票型基金完整信息
    guaranteed_fund_filename = 'guaranteed_fund_with_achievement.csv'  # 保本型基金完整信息

    # todo 对网络环境的判断与测试

    # just for test
    crawling_fund(GetFundListByWebForTest())

    print("\n爬取总用时", time.time() - start_time)
