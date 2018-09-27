# -*- coding:UTF-8 -*-
import requests
import time
from fake_useragent import UserAgent
import re


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


def get_past_performance():
    # fund_list = pandas.read_csv('fund.csv', index_col=0, dtype={'基金代号': str, '基金名称': str})\
    # .to_dict(orient='list')

    fund_list = {'基金代号': [], '基金名称': []}
    with open('fund_simple.csv', 'r') as f:
        for i in f.readlines():
            code, name, _ = i.split(',')
            fund_list['基金代号'].append(code)
            fund_list['基金名称'].append(name)

    achievement = {'近1月': [], '近3月': [], '近6月': [], '近1年': [], '近3年': [], '成立来': []}
    count = 0
    error_count = 0
    ua = UserAgent()
    for i in fund_list['基金代号']:
        count += 1
        header = {"User-Agent": ua.random}
        page = requests.get('http://fund.eastmoney.com/'+i+'.html', headers=header)
        page.encoding = 'utf-8'
        tem = re.search('(?:近1月|保本期收益)：.*?((\d+\.\d{2}%)|--).*?近1年：.*?((\d+\.\d{2}%)|--).*?近3月：.*?((\d+\.'
                        '\d{2}%)|--).*?近3年：.*?((\d+\.\d{2}%)|--).*?近6月：.*?((\d+\.\d{2}%)|--).*?成立来：.*?((\d+\.'
                        '\d{2}%)|--).*?基金类型', page.text)
        try:
            achievement['近1月'].append(tem.group(1))
            achievement['近1年'].append(tem.group(3))
            achievement['近3月'].append(tem.group(5))
            achievement['近3年'].append(tem.group(7))
            achievement['近6月'].append(tem.group(9))
            achievement['成立来'].append(tem.group(11))
        except:
            try:
                page = requests.get('http://fund.eastmoney.com/' + i + '.html', headers=header)
                page.encoding = 'utf-8'
                tem = re.search(
                    '(?:近1月|保本期收益)：.*?((\d+\.\d{2}%)|--).*?近1年：.*?((\d+\.\d{2}%)|--).*?近3月：.*?((\d+\.\d{2}'
                    '%)|--).*?近3年：.*?((\d+\.\d{2}%)|--).*?近6月：.*?((\d+\.\d{2}%)|--).*?成立来：.*?((\d+\.\d{2}%)|-'
                    '-).*?基金类型', page.text)
                achievement['近1月'].append(tem.group(1))
                achievement['近1年'].append(tem.group(3))
                achievement['近3月'].append(tem.group(5))
                achievement['近3年'].append(tem.group(7))
                achievement['近6月'].append(tem.group(9))
                achievement['成立来'].append(tem.group(11))
            except:
                error_count += 1
                time.sleep(10)
                print('------------------------')
                achievement['近1月'].append('??')
                achievement['近1年'].append('??')
                achievement['近3月'].append('??')
                achievement['近3年'].append('??')
                achievement['近6月'].append('??')
                achievement['成立来'].append('??')
        print(count)

    # pandas.DataFrame(fund_list, index=range(count)).to_csv('fund2.csv')

    with open('fund_with_achievement.csv', 'w') as f:
        for i in range(count):
            f.write(fund_list['基金代号'][i] + ',' + fund_list['基金名称'][i] + ',' + achievement['近1月'][i] + ',' +
                    achievement['近3月'][i] + ',' + achievement['近6月'][i] + ',' + achievement['近1年'][i] + ',' +
                    achievement['近3年'][i] + ',' + achievement['成立来'][i] + ',\n')

    print(error_count)


# get_fund_list()
get_past_performance()
