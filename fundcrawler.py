# -*- coding:UTF-8 -*-
import os
import random
import re
import threading
import time

import requests
from eprogress import LineProgress
from fake_useragent import UserAgent


class MyUserAgent():
    """
    用于提供ua，单例模式，fake_ua能用则用，否则用自带的数据库，通过class.random获取随机ua
    """

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(MyUserAgent, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.fake_ua = None
        try:
            self.fake_ua = UserAgent()
        except:
            self.some = [
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60',
                'Opera/8.0 (Windows NT 5.1; U; en)',
                'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
                'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 9.50',
                'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
                'Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10',
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16',
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36',
                'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11',
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
                'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)',
                'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)"',
                'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)',
                'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',
                'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0',
                'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)',
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.3.4000 Chrome/30.0.1599.101 Safari/537.36',
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36']

    def __getattr__(self, item):
        try:
            if item == 'random':
                if self.fake_ua is not None:
                    return self.fake_ua.random
                else:
                    return random.choice(self.some)
        except KeyError:
            raise AttributeError(r"Object does'n has attribute '%s'" % item)


def get_fund_list():
    """爬取简单的基金代码名称目录"""
    print('开始爬取。。。')
    ua = MyUserAgent()
    header = {"User-Agent": ua.random}
    page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                        'desc&page=1,9999&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)

    # 基金目录
    fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)

    # 保存到文件
    fund_save = dict()
    count = 0
    for i in fund_list:
        count += 1
        fund_save[i[1:7]] = i[10:-1]

    with open(all_fund_filename, 'w') as f:
        for key, value in fund_save.items():
            f.write(key + ',' + value + ',\n')

    print('共发现' + str(len(fund_list)) + '个基金')


def get_achievement(code, sign):
    """
    用于爬取基金的收益率和基金经理的信息
    :param code: 基金代码
    :param sign: 爬取失败次数，初始3，为0时放弃这个基金的爬取
    :return 二元组（基金信息，是否保本型标志），其中基金信息为list，近1月。3。6。近1年。3。（成立来收益率|保本期收益）（指数股票|保本），
    基金经理/基金经理2.，任职时间，任职收益率，基金经理总任职时间/基金经理2总任职时间.
    """
    global thread_pool
    try:
        proxy = random.choice(proxies_http_list)  # 代理ip
        proxy_all = {'http': proxy['ip']}  # 代理ip
    except IndexError:
        proxy_all = None

    achievement = []
    if sign > 0:
        ua = MyUserAgent()
        header = {"User-Agent": ua.random}
        try:
            try:
                page = requests.get('http://fund.eastmoney.com/' + code + '.html', headers=header, proxies=proxy_all)
            except:
                try:
                    if proxy_all:
                        if proxy['err_count'] == 5:
                            proxies_http_list.remove(proxy)
                        else:
                            proxy['err_count'] += 1
                except ValueError:
                    pass
                page = requests.get('http://fund.eastmoney.com/' + code + '.html', headers=header)
            page.encoding = 'utf-8'
            re_text = page.text
        except Exception as e:
            print(e)
            print(' ' + code)
            time.sleep(2)
            re_text = ''

        sign2 = 1
        # 基金的收益率
        tem = re.search('近1月：.*?((-?\d+\.\d{2}%)|--).*?近1年：.*?((-?\d+\.\d{2}%)|--).*?近3月：.*?((-?'
                        '\d+\.\d{2}%)|--).*?近3年：.*?((-?\d+\.\d{2}%)|--).*?近6月：.*?((-?\d+\.\d{2}%)|--).*?成立来：'
                        '.*?((-?\d+\.\d{2}%)|--).*?基金类型', re_text)
        if not tem:
            # 基金为保本型基金
            sign2 = 0
            tem = re.search('保本期收益.*?((-?\d+\.\d{2}%)|--).*?近6月：.*?((-?\d+\.\d{2}%)|--).*?近1月：.*?((-?\d+\.\d'
                            '{2}%)|--).*?近1年：.*?((-?\d+\.\d{2}%)|--).*?近3月：.*?((-?\d+\.\d{2}%)|--).*?近3年：.*?(('
                            '-?\d+\.\d{2}%)|--).*?基金类型', re_text)
            if re.search('封闭期', re_text) or re.search('本基金已终止', re_text):
                # 基金为有封闭期的固定收益基金或已终止的基金
                sign2 = 2
                return list(), sign2
        # 基金经理和个人信息链接
        tem4 = re.search('<td class="td02">(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)(?:(?:<a href="(.*?)">(.+?)'
                         '</a>&nbsp;&nbsp;)|)', re_text)
        # 基金经理在本基金的任职时间和收益率
        tem2 = re.search('</td>  <td class="td03">(.+?)</td>  <td class="td04 bold (?:ui-color-(?:red|green)|)">'
                         '(-?\d+\.\d{2}%)</td></tr>', re_text)
        try:
            # 保存基金收益率
            if sign2 == 1:
                achievement.append(tem.group(1))
                achievement.append(tem.group(5))
                achievement.append(tem.group(9))
                achievement.append(tem.group(3))
                achievement.append(tem.group(7))
                achievement.append(tem.group(11))
            else:
                # 保本型基金
                achievement.append(tem.group(5))
                achievement.append(tem.group(9))
                achievement.append(tem.group(3))
                achievement.append(tem.group(7))
                achievement.append(tem.group(11))
                achievement.append(tem.group(1))

            # 对可能的多个基金经理分别记录以便后续爬取
            manager_link_list = list()
            manager_list = list()
            i = 1
            for j in tem4.groups():
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
            achievement.append(manager)
            achievement.append(tem2.group(1))
            achievement.append(tem2.group(2))

            # 分别打开基金经理的个人信息页，保存他们的总任职时间
            manager_link = None
            for i in manager_link_list:
                try:
                    page2 = requests.get(i)
                    page2.encoding = 'utf-8'
                    re_text = page2.text
                except:
                    time.sleep(2)
                    re_text = ''
                tem3 = re.search('<span>累计任职时间：</span>(.*?)<br />', re_text)
                if i != manager_link_list[0]:
                    manager_link += '/' + tem3.group(1)
                else:
                    manager_link = tem3.group(1)
            achievement.append(manager_link)
            thread_pool += 1
        except:
            # 出错后的重试
            time.sleep(random.randint(1, 3))
            thread_pool = thread_pool // 2 + 1
            achievement = get_achievement(code, sign - 1)
    else:
        # 重复出错3次后，放弃。相应信息为未知(??)
        for i in range(10):
            achievement.append('??')

    if sign == 3:
        # 最后返回的两元组，收益集合以及是否为保本型基金的标志
        return achievement, sign2
    else:
        # 递归只返回收益集合
        return achievement


def thread_get_past_performance(code, name, list_index_fund, list_guaranteed_fund):
    """
    爬取单个基金信息的线程
    :param code: 基金代号
    :param name: 基金名称
    :param list_index_fund: 保存爬取的指数/股票基金
    :param list_guaranteed_fund: 保存爬取的保本基金
    """
    # 爬取收益、基金经理信息
    global thread_pool
    tem, sign = get_achievement(code, 3)
    fund_all_msg = [code, name] + tem

    # 保存文件
    if sign == 1:
        # 指数型/股票型等基金
        list_index_fund.append(fund_all_msg)
    # f = open('index_fund_with_achievement.csv', 'a')
    # thread_index_fund_file_lock.acquire()
    # for i in fund_all_msg:
    #     f.write(i + ',')
    # f.write('\n')
    # thread_index_fund_file_lock.release()

    elif sign == 0:
        # 保本型基金
        list_guaranteed_fund.append(fund_all_msg)
        # f = open('guaranteed_fund_with_achievement.csv', 'a')
        # thread_guaranteed_fund_file_lock.acquire()
        # for i in fund_all_msg:
        #     f.write(i + ',')
        # f.write('\n')
        # thread_guaranteed_fund_file_lock.release()
    else:
        # 有封闭期的固定收益基金或已终止的基金
        pass

    # f.close()


def get_past_performance(source_file_name):
    """
    在简单基金目录的基础上，爬取所有基金的信息
    :param source_file_name:基金目录
    """
    # 测试文件是否被占用，并写入列索引
    try:
        if source_file_name == all_fund_filename:
            with open(all_index_fund_with_msg_filename, 'w') as f:
                f.write('基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,成立来收益,基金经理,'
                        '本基金任职时间,本基金任职收益,累计任职时间,\n')
            with open(all_guaranteed_fund_with_msg_filename, 'w') as f:
                f.write('基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,保本期收益,基金经理,'
                        '本基金任职时间,本基金任职收益,累计任职时间,\n')
    except:
        print('文件' + all_fund_filename + '无法打开')
        return

    # 进度条
    line_progress = LineProgress(title='爬取进度')
    count = 0

    with open(source_file_name, 'r') as f:
        # 线程集合和保存信息文件的线程安全锁
        thread = []
        # 废弃
        # thread_index_fund_file_lock = threading.Lock()
        # thread_guaranteed_fund_file_lock = threading.Lock()

        # 接受线程爬取的信息，到一定数量后一次写入
        list_index_fund = list()
        list_guaranteed_fund = list()

        # 逐个爬取所有基金的信息
        fund_list = f.readlines()
        fund_list_length = len(fund_list) // 100
        for i in fund_list:
            count += 1
            try:
                code, name, _ = i.split(',')
            except ValueError:
                continue
            # 多线程爬取
            t = threading.Thread(target=thread_get_past_performance,
                                 args=(code, name, list_index_fund, list_guaranteed_fund))
            thread.append(t)
            t.start()
            time.sleep(0.1)
            for t in thread:
                if not t.is_alive():
                    thread.remove(t)

            # 判断线程集合是否过大
            while len(thread) > thread_pool:
                time.sleep(1)
                for t in thread:
                    if not t.is_alive():
                        thread.remove(t)

            # 判断信息流大小，是否需要写入文件
            if len(list_index_fund) > write_file_num:
                f = open(all_index_fund_with_msg_filename, 'a')
                for i in list_index_fund:
                    for j in i:
                        f.write(j + ',')
                    f.write('\n')
                list_index_fund.clear()

            if len(list_guaranteed_fund) > write_file_num:
                f = open(all_guaranteed_fund_with_msg_filename, 'a')
                for i in list_guaranteed_fund:
                    for j in i:
                        f.write(j + ',')
                    f.write('\n')
                list_guaranteed_fund.clear()

            line_progress.update(count // fund_list_length)

    # 等待所有线程执行完毕
    while len(thread) > 0:
        time.sleep(2)
        for t in thread:
            if not t.is_alive():
                thread.remove(t)
    line_progress.update(100)

    # 写完所有的文件
    for i in list_index_fund:
        f = open(all_index_fund_with_msg_filename, 'a')
        for j in i:
            f.write(j + ',')
        f.write('\n')
    for i in list_guaranteed_fund:
        f = open(all_guaranteed_fund_with_msg_filename, 'a')
        for j in i:
            f.write(j + ',')
        f.write('\n')


def no_data_handle(fund_with_achievement):
    """
    对第一次爬取失败的基金信息的重新爬取
    :param fund_with_achievement:重新爬取的基金目录
    """
    # 文件以a方式写入，先进行可能的文件清理
    try:
        os.remove(fund_need_handle_filename)
    except FileNotFoundError:
        pass
    try:
        os.remove('tem.csv')
    except FileNotFoundError:
        pass

    sign = 1
    with open(fund_with_achievement, 'r') as f1:
        print('重新爬取第一次失败的基金')
        for i in f1.readlines():
            # 逐条检查基金信息
            try:
                code, name, one_month, three_month, six_month, one_year, three_year, from_st, _, this_tenure_time, \
                this_return, all_tenure_time, _ = i.split(',')
            except ValueError:
                code, name, *_ = i.split(',')
                one_month = '??'

            # 当基金信息为未知时(??)
            if one_month == '??':
                sign = 0
                with open(fund_need_handle_filename, 'a') as f2:
                    f2.write(code + ',' + name + ',' + '\n')
                continue
            else:
                with open('tem.csv', 'a') as f3:
                    f3.write(i)

    # 用筛选过的文件替换原来的基金信息文件，并调用函数对信息未知的基金进行重爬
    os.remove(fund_with_achievement)
    os.renames('tem.csv', fund_with_achievement)
    if sign == 0:
        get_past_performance(source_file_name=fund_need_handle_filename)
        os.remove(fund_need_handle_filename)


def get_time_from_str(time_str):
    """
    将文字描述的任职时间转为列表
    :param time_str: 描述时间的str
    :return [int(多少年),int(多少天)]
    """
    tem = re.search('(?:(\d)年又|)(\d{0,3})天', time_str).groups()
    tem_return = list()
    for i in tem:
        if i:
            tem_return.append(int(i))
        else:
            tem_return.append(0)
    return tem_return


def data_analysis(fund_with_achievement, choice_cretertion_return, choice_cretertion_time):
    """
    按传入的训责策略，筛选出符合要求的基金
    :param fund_with_achievement: 全部的基金信息文件名
    :param choice_cretertion_return: 要求的基金收益率
    :param choice_cretertion_time: 要求的任职时间
    """
    # 文件以a方式写入，先进行可能的文件清理
    try:
        os.remove(fund_choice_filename)
    except FileNotFoundError:
        pass

    try:
        with open(fund_choice_filename, 'w') as f:
            f.write('基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,成立来收益/保本期收益,基金经理,'
                    '本基金任职时间,本基金任职收益,累计任职时间,\n')

        with open(fund_with_achievement, 'r') as f:
            count = 0
            for i in f.readlines()[1:]:
                # 逐条检查
                count += 1
                sign = 1

                # 取基金信息，并按收益率和任职时间分类
                _, _, one_month, three_month, six_month, one_year, three_year, from_st, _, this_tenure_time, \
                this_return, all_tenure_time, _ = i.split(',')
                return_all = [one_month, three_month, six_month, one_year, three_year, from_st, this_return]
                time_all = [this_tenure_time, all_tenure_time]

                # 信息未知或一月数据不存在（成立时间过短）的淘汰
                if one_month == '??' or one_month == '--':
                    continue

                # 收益率部分的筛选
                for j, k in zip(choice_cretertion_return.values(), return_all):
                    if k == '--':
                        continue
                    if float(k[:-1]) < j:
                        sign = 0
                        break

                # 任职时间部分的筛选
                if sign == 1:
                    for j, k in zip(choice_cretertion_time.values(), time_all):
                        for l, m in zip(j, get_time_from_str(k)):
                            if m > l:
                                break
                            elif m == l:
                                continue
                            else:
                                sign = 0
                                break

                # 符合要求的保存进文件
                if sign == 1:
                    with open(fund_choice_filename, 'a') as f2:
                        f2.write(i)
                print(count)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    start_time = time.time()

    # 写入文件缓冲阈值 线程池大小
    write_file_num = 1000
    thread_pool = 10

    # 文件名设置
    all_fund_filename = 'fund_simple.csv'  # 基金目录
    all_index_fund_with_msg_filename = 'index_fund_with_achievement.csv'  # 指数/股票型基金完整信息
    all_guaranteed_fund_with_msg_filename = 'guaranteed_fund_with_achievement.csv'  # 保本型基金完整信息
    fund_need_handle_filename = 'fund_need_handle.csv'  # 保存需要重新爬取的基金
    fund_choice_filename = 'fund_choice.csv'  # 保存筛选出的基金

    # 打开保存在proxies_http.txt的http代理ip
    proxies_http_list = list()
    # with open('proxies_http.txt', 'r') as f:
    #     for i in f.readlines()[1:]:
    #         tem = {'ip': i[:-1], 'err_count': 0}
    #         proxies_http_list.append(tem)

    # 获取基金列表 获取基金过往数据 重新获取第一次失败的数据
    get_fund_list()
    get_past_performance(all_fund_filename)
    no_data_handle(all_index_fund_with_msg_filename)
    # no_data_handle(all_guaranteed_fund_with_msg_filename)

    # 对基金的筛选设置
    choice_cretertion_return = {'近1月收益': 4.63, '近3月收益': 11.67, '近6月收益': 12.07, '近1年收益': 6.97,
                                '近3年收益': 22.39, '成立来收益/保本期收益': 0, '本基金任职收益': 0}
    choice_cretertion_time = {'本基金任职时间': [1, 0], '累计任职时间': [3, 0]}

    # 筛选后的文件为fund_choice_filename的值，若还需要对保本型基金进来筛选，需要先备份
    data_analysis(all_index_fund_with_msg_filename, choice_cretertion_return, choice_cretertion_time)

    print("爬取总用时", time.time() - start_time)
