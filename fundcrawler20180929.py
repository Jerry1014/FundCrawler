# -*- coding:UTF-8 -*-
import requests
import time
from fake_useragent import UserAgent
import re
import threading
import random
import os


def get_fund_list():
    ua = UserAgent()
    header = {"User-Agent": ua.random}
    page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                        'desc&page=1,9999&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)
    fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
    count = 0

    fund_save = dict()
    for i in fund_list:
        count += 1
        fund_save[i[1:7]] = i[10:-1]
        print("No."+str(count)+"  "+i[1:7]+"  "+i[10:-1])

    with open('fund_simple.csv', 'w') as f:
        for key, value in fund_save.items():
            f.write(key + ',' + value + ',\n')


def get_achievement(code, sign):
    achievement = []
    if sign > 0:
        ua = UserAgent()
        header = {"User-Agent": ua.random}
        page = requests.get('http://fund.eastmoney.com/' + code + '.html', headers=header)
        page.encoding = 'utf-8'

        sign2 = 1
        tem = re.search('近1月：.*?((-?\d+\.\d{2}%)|--).*?近1年：.*?((-?\d+\.\d{2}%)|--).*?近3月：.*?((-?'
                        '\d+\.\d{2}%)|--).*?近3年：.*?((-?\d+\.\d{2}%)|--).*?近6月：.*?((-?\d+\.\d{2}%)|--).*?成立来：'
                        '.*?((-?\d+\.\d{2}%)|--).*?基金类型', page.text)
        if not tem:
            sign2 = 0
            tem = re.search('保本期收益.*?((-?\d+\.\d{2}%)|--).*?近6月：.*?((-?\d+\.\d{2}%)|--).*?近1月：.*?((-?\d+\.\d'
                            '{2}%)|--).*?近1年：.*?((-?\d+\.\d{2}%)|--).*?近3月：.*?((-?\d+\.\d{2}%)|--).*?近3年：.*?(('
                            '-?\d+\.\d{2}%)|--).*?基金类型', page.text)

        tem4 = re.search('<td class="td02">(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)(?:(?:<a href="(.*?)">(.+?)'
                         '</a>&nbsp;&nbsp;)|)', page.text)
        tem2 = re.search('</td>  <td class="td03">(.+?)</td>  <td class="td04 bold (?:ui-color-(?:red|green)|)">'
                         '(-?\d+\.\d{2}%)</td></tr>', page.text)
        try:
            if sign2 == 1:
                achievement.append(tem.group(1))
                achievement.append(tem.group(5))
                achievement.append(tem.group(9))
                achievement.append(tem.group(3))
                achievement.append(tem.group(7))
                achievement.append(tem.group(11))
            else:
                achievement.append(tem.group(5))
                achievement.append(tem.group(9))
                achievement.append(tem.group(3))
                achievement.append(tem.group(7))
                achievement.append(tem.group(11))
                achievement.append(tem.group(1))

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
            manager = None
            for i in manager_list:
                if i != manager_list[0]:
                    manager += '/' + i
                else:
                    manager = i
            achievement.append(manager)
            achievement.append(tem2.group(1))
            achievement.append(tem2.group(2))

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
            time.sleep(random.randint(1, 5))
            achievement = get_achievement(code, sign-1)
    else:
        for i in range(10):
            achievement.append('??')
    return achievement


def thread_get_past_performance(code, name, thread_file_lock):
    tem = get_achievement(code, 3)
    fund_all_msg = [code, name] + tem
    thread_file_lock.acquire()
    with open('fund_with_achievement.csv', 'a') as f:
        for i in fund_all_msg:
            f.write(i + ',')
        f.write('\n')
    thread_file_lock.release()


def get_past_performance(source_file_name='fund_simple.csv'):
    try:
        if source_file_name == 'fund_simple.csv':
            with open('fund_with_achievement.csv', 'w') as f:
                f.write('基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,成立来收益/保本期收益,基金经理,'
                        '本基金任职时间,本基金任职收益,累计任职时间,\n')
    except:
        print('文件fund_with_achievement.csv无法打开')
        return
    with open(source_file_name, 'r') as f:
        thread = []
        thread_file_lock = threading.Lock()

        count = 0
        for i in f.readlines():
            count += 1
            try:
                code, name, _ = i.split(',')
            except ValueError:
                break
            t = threading.Thread(target=thread_get_past_performance, args=(code, name, thread_file_lock))
            thread.append(t)
            t.start()
            time.sleep(0.1)

            while len(thread) > 50:
                time.sleep(random.randint(2, 10))
                for t in thread:
                    if not t.is_alive():
                        thread.remove(t)
                print("the len of thread " + str(len(thread)))
            print(count)

    while len(thread) > 0:
        time.sleep(2)
        for t in thread:
            if not t.is_alive():
                thread.remove(t)
    return


def no_data_handle():
    try:
        os.remove('fund_need_handle.csv')
        os.remove('fund_with_achievement2.csv')
    except FileNotFoundError:
        pass

    sign = 1
    with open('fund_with_achievement.csv', 'r') as f1:
        for i in f1.readlines():
            try:
                code, name, one_month, three_month, six_month, one_year, three_year, from_st, _, this_tenure_time, \
                this_return, all_tenure_time, _ = i.split(',')
            except ValueError:
                code, name, *_ = i.split(',')
                one_month = '??'

            if one_month == '??':
                sign = 0
                with open('fund_need_handle.csv', 'a') as f2:
                    f2.write(code+','+name+','+'\n')
                continue
            with open('fund_with_achievement2.csv', 'a') as f3:
                f3.write(i)

    os.remove('fund_with_achievement.csv')
    os.renames('fund_with_achievement2.csv', 'fund_with_achievement.csv')
    if sign == 0:
        get_past_performance(source_file_name='fund_need_handle.csv')
        os.remove('fund_need_handle.csv')


def get_time_from_str(time_str):
    tem = re.search('(?:(\d)年又|)(\d{0,3})天', time_str).groups()
    tem_return = list()
    for i in tem:
        if i:
            tem_return.append(int(i))
        else:
            tem_return.append(0)
    return tem_return


def data_analysis():
    try:
        os.remove('fund_choice.csv')
    except FileNotFoundError:
        pass
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
                count += 1
                sign = 1
                _, _, one_month, three_month, six_month, one_year, three_year, from_st, _, this_tenure_time,\
                this_return, all_tenure_time,_ = i.split(',')
                return_all = [one_month, three_month, six_month, one_year, three_year, from_st, this_return]
                time_all = [this_tenure_time, all_tenure_time]

                if one_month == '??' or one_month == '--':
                    continue

                for j, k in zip(choice_cretertion_return.values(), return_all):
                    if k == '--':
                        continue
                    if float(k[:-1]) < j:
                        sign = 0
                        break

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

                if sign == 1:
                    with open('fund_choice.csv', 'a') as f2:
                        f2.write(i)
                print(count)

    except Exception as e:
        print(e)


# get_fund_list()
# get_past_performance()
no_data_handle()
data_analysis()
