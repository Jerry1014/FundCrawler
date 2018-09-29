# -*- coding:UTF-8 -*-
import requests
import time
from fake_useragent import UserAgent
import re
import threading
import random
import os


def get_fund_list():
    """爬取简单的基金代码名称目录"""
    ua = UserAgent()
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
        print("No."+str(count)+"  "+i[1:7]+"  "+i[10:-1])

    with open('fund_simple.csv', 'w') as f:
        for key, value in fund_save.items():
            f.write(key + ',' + value + ',\n')


def get_achievement(code, sign):
    """用于爬取基金的收益率和基金经理的信息"""
    achievement = []
    if sign > 0:
        ua = UserAgent()
        header = {"User-Agent": ua.random}
        page = requests.get('http://fund.eastmoney.com/' + code + '.html', headers=header)
        page.encoding = 'utf-8'

        sign2 = 1
        # 基金的收益率
        tem = re.search('近1月：.*?((-?\d+\.\d{2}%)|--).*?近1年：.*?((-?\d+\.\d{2}%)|--).*?近3月：.*?((-?'
                        '\d+\.\d{2}%)|--).*?近3年：.*?((-?\d+\.\d{2}%)|--).*?近6月：.*?((-?\d+\.\d{2}%)|--).*?成立来：'
                        '.*?((-?\d+\.\d{2}%)|--).*?基金类型', page.text)
        if not tem:
            # 基金为保本型基金
            sign2 = 0
            tem = re.search('保本期收益.*?((-?\d+\.\d{2}%)|--).*?近6月：.*?((-?\d+\.\d{2}%)|--).*?近1月：.*?((-?\d+\.\d'
                            '{2}%)|--).*?近1年：.*?((-?\d+\.\d{2}%)|--).*?近3月：.*?((-?\d+\.\d{2}%)|--).*?近3年：.*?(('
                            '-?\d+\.\d{2}%)|--).*?基金类型', page.text)
        # 基金经理和个人信息链接
        tem4 = re.search('<td class="td02">(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)(?:(?:<a href="(.*?)">(.+?)'
                         '</a>&nbsp;&nbsp;)|)', page.text)
        # 基金经理在本基金的任职时间和收益率
        tem2 = re.search('</td>  <td class="td03">(.+?)</td>  <td class="td04 bold (?:ui-color-(?:red|green)|)">'
                         '(-?\d+\.\d{2}%)</td></tr>', page.text)
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
                page2 = requests.get(i)
                page2.encoding = 'utf-8'
                tem3 = re.search('<span>累计任职时间：</span>(.*?)<br />', page2.text)
                if i != manager_link_list[0]:
                    manager_link += '/' + tem3.group(1)
                else:
                    manager_link = tem3.group(1)
            achievement.append(manager_link)
        except Exception as e:
            print(e)
            # 出错后的重试
            time.sleep(random.randint(1, 5))
            achievement = get_achievement(code, sign-1)
    else:
        # 重复出错3次后，放弃。相应信息为未知(??)
        for i in range(10):
            achievement.append('??')
    return achievement


def thread_get_past_performance(code, name, thread_file_lock):
    """爬取单个基金信息的线程"""
    # 爬取收益、基金经理信息
    tem = get_achievement(code, 3)
    fund_all_msg = [code, name] + tem

    # 保存文件
    thread_file_lock.acquire()
    with open('fund_with_achievement.csv', 'a') as f:
        for i in fund_all_msg:
            f.write(i + ',')
        f.write('\n')
    thread_file_lock.release()


def get_past_performance(source_file_name='fund_simple.csv'):
    """在简单基金目录的基础上，爬取所有基金的信息"""
    # 测试文件是否被占用，并写入列索引
    try:
        if source_file_name == 'fund_simple.csv':
            with open('fund_with_achievement.csv', 'w') as f:
                f.write('基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,成立来收益/保本期收益,基金经理,'
                        '本基金任职时间,本基金任职收益,累计任职时间,\n')
    except:
        print('文件fund_with_achievement.csv无法打开')
        return

    with open(source_file_name, 'r') as f:
        # 线程集合和保存信息文件的线程安全锁
        thread = []
        thread_file_lock = threading.Lock()

        # 逐个爬取所有基金的信息
        count = 0
        for i in f.readlines():
            count += 1
            try:
                code, name, _ = i.split(',')
            except ValueError:
                break
            # 多线程爬取
            t = threading.Thread(target=thread_get_past_performance, args=(code, name, thread_file_lock))
            thread.append(t)
            t.start()
            time.sleep(0.1)

            # 判断线程集合是否过大
            while len(thread) > 50:
                time.sleep(random.randint(2, 10))
                for t in thread:
                    if not t.is_alive():
                        thread.remove(t)
                print("the len of thread " + str(len(thread)))
            print(count)

    # 等待所有线程执行完毕
    while len(thread) > 0:
        time.sleep(2)
        for t in thread:
            if not t.is_alive():
                thread.remove(t)
    return


def no_data_handle():
    """对第一次爬取失败的基金信息的重新爬取"""
    # 文件以a方式写入，先进行可能的文件清理
    try:
        os.remove('fund_need_handle.csv')
    except FileNotFoundError:
        pass
    try:
        os.remove('fund_with_achievement2.csv')
    except FileNotFoundError:
        pass

    sign = 1
    with open('fund_with_achievement.csv', 'r') as f1:
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
                with open('fund_need_handle.csv', 'a') as f2:
                    f2.write(code+','+name+','+'\n')
                continue
            with open('fund_with_achievement2.csv', 'a') as f3:
                f3.write(i)

    # 用筛选过的文件替换原来的基金信息文件，并调用函数对信息未知的基金进行重爬
    os.remove('fund_with_achievement.csv')
    os.renames('fund_with_achievement2.csv', 'fund_with_achievement.csv')
    if sign == 0:
        get_past_performance(source_file_name='fund_need_handle.csv')
        os.remove('fund_need_handle.csv')


def get_time_from_str(time_str):
    """将文字描述的任职时间转为列表"""
    tem = re.search('(?:(\d)年又|)(\d{0,3})天', time_str).groups()
    tem_return = list()
    for i in tem:
        if i:
            tem_return.append(int(i))
        else:
            tem_return.append(0)
    # [int(多少年),int(多少天)]
    return tem_return


def data_analysis():
    """按预先设置，筛选出符合要求的基金"""
    # 文件以a方式写入，先进行可能的文件清理
    try:
        os.remove('fund_choice.csv')
    except FileNotFoundError:
        pass
    
    # 对基金的筛选设置
    choice_cretertion_return = {'近1月收益': 1.14, '近3月收益': 0.45, '近6月收益': -10.51, '近1年收益': -10.04,
                                '近3年收益': 1.34, '成立来收益//保本期收益': 0, '本基金任职收益': 0}
    choice_cretertion_time = {'本基金任职时间': [1, 0], '累计任职时间': [3, 0]}
    
    try:
        with open('fund_choice.csv', 'w') as f:
            f.write('基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,成立来收益/保本期收益,基金经理,'
                    '本基金任职时间,本基金任职收益,累计任职时间,\n')

        with open('fund_with_achievement.csv', 'r') as f:
            count = 0
            for i in f.readlines()[1:]:
                # 逐条检查
                count += 1
                sign = 1

                # 取基金信息，并按收益率和任职时间分类
                _, _, one_month, three_month, six_month, one_year, three_year, from_st, _, this_tenure_time,\
                this_return, all_tenure_time,_ = i.split(',')
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
                    with open('fund_choice.csv', 'a') as f2:
                        f2.write(i)
                print(count)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    get_fund_list()
    get_past_performance()
    no_data_handle()
    data_analysis()
