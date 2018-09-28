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
        #tem4 = re.search('<td class="td02">(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)+', page.text)
        tem4 = re.search('<td class="td02">(?:<a href="(.*?)">(.+?)</a>&nbsp;&nbsp;)(?:(?:<a href="(.*?)">(.+?)'
                         '</a>&nbsp;&nbsp;)|)', page.text)
        tem2 = re.search('</td>  <td class="td03">(.+?)</td>  <td class="td04 bold ui-color-(?:red|green)">'
                         '(-?\d+\.\d{2}%)</td></tr>', page.text)
        try:
            achievement.append(tem.group(1))
            achievement.append(tem.group(5))
            achievement.append(tem.group(9))
            achievement.append(tem.group(3))
            achievement.append(tem.group(7))
            achievement.append(tem.group(11))

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
            time.sleep(10)
            achievement = get_achievement(code, sign-1)
    else:
        for i in range(len(achievement)):
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


def get_past_performance():
    try:
        with open('fund_with_achievement.csv', 'w') as f:
            f.write('基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,成立来收益,基金经理,本基金任职时间,'
                    '本基金任职收益,累计任职时间,\n')
    except:
        print('文件fund_with_achievement.csv无法打开')
        return
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


get_fund_list()
get_past_performance()
