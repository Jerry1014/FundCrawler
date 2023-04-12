# -*- coding:UTF-8 -*-
"""
爬取基金信息的主文件
"""
import os
import time
import traceback
import sys
from multiprocessing import Event
if 'darwin' in sys.platform:
    from methods import Queue
else:
    from multiprocessing import Queue

from requests.exceptions import RequestException

from CrawlingCore import GetPageByWebWithAnotherProcessAndMultiThreading
from Parser import ParseDefault
from FundListProvider import GetFundList, GetFundListFromWeb, GetFundListTest
from DataStructure import FundInfo

# 尝试引入进度条所需库文件
try:
    from eprogress import LineProgress
except ImportError:
    print('未安装进度条依赖库，将以极简形式显示当前进度')
    LineProgress = None


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
    crawling_core = GetPageByWebWithAnotherProcessAndMultiThreading(input_queue, result_queue, finish_sign, network_health)
    crawling_core.start()

    fund_list = fund_list_class.get_fund_list()
    num_of_fund = fund_list_class._sum_of_fund
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
                    if a_result[1] == None:
                        print('stop')
                    # # fixme 临时措施，重新爬取返回空网页的基金
                    # if a_result[1] == '':
                    #     tem_fund_info = a_result[3]
                    #     input_queue.put(('http://fund.eastmoney.com/' + tem_fund_info._fund_info('code') + '.html', tem_fund_info))
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
            time.sleep(1)
            if not having_fund_need_to_crawl and input_queue.qsize() == 0 and result_queue.qsize() == 0:
                break

    finish_sign.set()
    # 挖坑 对第一次爬取失败的基金的处理


if __name__ == '__main__':
    # 获取用于区分测试环境和正式环境的 标记
    try:
        if_test_env = os.environ["ifTest"]
    except KeyError:
        if_test_env = False

    # 记录开始时间
    start_time = time.time()

    # 干活
    try:
        # 需要爬取的基金列表
        if if_test_env:
            # 仅供测试
            fund_list = GetFundListTest()
        else:
            # 获取当前网络上的基金列表
            fund_list = GetFundListFromWeb()

        # 在这里更换提供基金列表的类，以实现从文件或者其他方式来指定要爬取的基金
        crawling_fund(fund_list)
    except Exception:
        if not if_test_env:
            print('不知道为了什么，程序死掉了。')
        traceback.print_exc()

    # 输出总时间
    print(f'\n爬取总用时{time.time() - start_time} s')
