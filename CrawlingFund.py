# -*- coding:UTF-8 -*-
"""
爬取基金信息的主文件
"""
import time
from multiprocessing import Queue, Event

from requests.exceptions import RequestException

from CrawlingWebpage import GetPageByWebWithAnotherProcessAndMultiThreading
from ParsingHtml import ParseDefault
from ProvideTheListOfFund import GetFundList, GetFundListByWeb, GetFundListTest

# 尝试引入进度条所需库文件
try:
    from eprogress import LineProgress
except ImportError:
    print('未安装进度条依赖库，将以极简形式显示当前进度')
    LineProgress = None

# 测试标记 连接timeout
if_test = False
TIMEOUT = 3


class FundInfo:
    """
    基金信息
    """

    def __init__(self):
        # 基金类型 基金信息字典 基金经理信息字典 当前基金信息类状态（下一步） 需要解析的基金经理列表
        self.fund_kind = 'Unknown'
        self._fund_info = dict()
        self._manager_info = dict()
        self.next_step = 'parsing_fund'
        self.manager_need_process_list = list()

    def get_info(self, index: list = None, missing: str = '??'):
        """
        获取基金信息
        :param index: 基金信息的列索引，若无，则按照保存信息的字典给出的哈希顺序
        :param missing: 列索引无对应值的填充
        :return: str 按照给定的列索引返回基金信息，信息之间以 , 分割
        """
        if index is None:
            return ','.join(list(self._fund_info.values()) + ['/'.join(self._manager_info.keys()),
                                                              '/'.join(self._manager_info.values())])
        else:
            return ','.join(self._get_info(i, missing) for i in index)

    def _get_info(self, index: str, missing: str):
        """
        内部的获取基金信息的方法
        :param index: 要获取的基金信息索引（key）
        :param missing: 列索引无对应值的填充
        :return: str 对应的基金信息
        """
        if index in self._fund_info.keys():
            return self._fund_info[index]
        elif index == '基金经理' or index == '总任职时间':
            return '/'.join(self._manager_info.keys()) if index == '基金经理' else '/'.join(self._manager_info.values())
        else:
            return str(missing)

    def set_fund_info(self, key: str, value: str):
        """
        设置基金信息
        :param key: 基金信息索引
        :param value: 基金信息
        """
        self._fund_info[key] = str(value)

    def set_manager_info(self, key, value):
        """
        设置基金经理信息
        :param key: 基金经理姓名
        :param value: 基金经理信息（目前为str 基金经理的总任职时长）
        """
        self._manager_info[key] = value

    def __repr__(self):
        return self.get_info()


def crawling_fund(fund_list_class: GetFundList, first_crawling=True):
    """
    在简单基金目录的基础上，爬取所有基金的信息
    :param fund_list_class: 提供要爬取的基金目录的类
    :param first_crawling: 是否是第一次爬取，这决定了是否会重新写保存文件（清空并写入列索引）
    :return 爬取失败的('基金代码,基金名称')(list)
    """
    # 进度条 基金总数 爬取进度
    line_progress = None if LineProgress is None else LineProgress(title='爬取进度')
    cur_process = 0
    # 爬取输入、输出队列，输入结束事件，网络状态事件，爬取核心
    input_queue = Queue()
    result_queue = Queue()
    finish_sign = Event()
    network_health = Event()
    crawling_core = GetPageByWebWithAnotherProcessAndMultiThreading(input_queue, result_queue, finish_sign,
                                                                    network_health, TIMEOUT)
    crawling_core.start()

    fund_list = fund_list_class.get_fund_list()
    num_of_fund = fund_list_class.sum_of_fund
    having_fund_need_to_crawl = True

    parse_html_core = ParseDefault()
    fund_web_page_parse = parse_html_core.get_parse_fund_info()
    manager_web_page_parse = parse_html_core.get_parse_fund_manger()
    write_file = parse_html_core.get_after_parsing_fund_info(first_crawling)
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
                    if line_progress is not None:
                        line_progress.update(100 * cur_process / num_of_fund)
                    else:
                        print(f'已完成{cur_process}/全部{num_of_fund}')
                else:
                    print(f'请检查FundInfo的next_step(此处为{a_result[2].next_step})设置，出现了未知的参数')

        # 完成所有任务判断
        if not having_fund_need_to_crawl and input_queue.qsize() == 0 and result_queue.qsize() == 0:
            time.sleep(TIMEOUT)
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
