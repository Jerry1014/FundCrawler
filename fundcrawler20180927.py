# -*- coding:UTF-8 -*-
import requests
import time
from fake_useragent import UserAgent
import re
import threading
import os


def get_fund_list():
    ua = UserAgent()
    header = {"User-Agent": ua.random}
    page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                        'desc&page=1,9999&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)
    fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
    count = 0

    # fund_save = {'基金代号': [], '基金名称': []}
    # for i in fund_list:
    #     count += 1
    #     fund_save['基金代号'].append(i[1:7])
    #     fund_save['基金名称'].append(i[10:-1])
    #     print("No."+str(count)+"  "+i[1:7]+"  "+i[10:-1])
    # pandas.DataFrame(fund_save, index=range(count)).to_csv('fund.csv')

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
        tem = re.search('(?:近1月|保本期收益)：.*?((-?\d+\.\d{2}%)|--).*?近1年：.*?((-?\d+\.\d{2}%)|--).*?近3月：.*?((-?'
                        '\d+\.\d{2}%)|--).*?近3年：.*?((-?\d+\.\d{2}%)|--).*?近6月：.*?((-?\d+\.\d{2}%)|--).*?成立来：'
                        '.*?((-?\d+\.\d{2}%)|--).*?基金类型', page.text)
        try:
            achievement.append(tem.group(1))
            achievement.append(tem.group(5))
            achievement.append(tem.group(9))
            achievement.append(tem.group(3))
            achievement.append(tem.group(7))
            achievement.append(tem.group(11))
        except:
            time.sleep(10)
            achievement = get_achievement(code, sign-1)
    else:
        for i in range(6):
            achievement.append('??')
    return achievement


def thread_get_past_performance(code, name, thread_file_lock):
    tem = get_achievement(code, 3)
    sign = 1
    for i in tem:
        if i[0] == '-':
            if i[1] == '-':
                if tem[5] == i:
                    sign = 0
                    break
                else:
                    continue
            else:
                sign = 0
                break
    tem.append(str(sign))
    fund_all_msg = [code, name] + tem
    thread_file_lock.acquire()
    with open('fund_with_achievement.csv', 'a') as f:
        for i in fund_all_msg:
            f.write(i + ',')
        f.write('\n')
    thread_file_lock.release()


def get_past_performance():
    try:
        os.remove('fund_with_achievement.csv')
    except FileNotFoundError:
        pass
    with open('fund_simple.csv', 'r') as f:
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

            sleep_time = 1
            while len(thread) > 50:
                time.sleep(sleep_time)
                for t in thread:
                    if not t.is_alive():
                        thread.remove(t)
                sleep_time += 1
                print("the len of thread " + str(len(thread)))
            print(count)


#
# get_fund_list()
get_past_performance()
