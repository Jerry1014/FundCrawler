# -*- coding:UTF-8 -*-
import requests
import pandas
from fake_useragent import UserAgent
import re


def get_fund():
    fund_save = {}
    ua = UserAgent()
    header = {"User-Agent": ua.random}
    page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                        'desc&page=1,9999&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)
    print(page.text)
    fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
    count = 0
    for i in fund_list:
        count += 1
        print(i)
        fund_save[i[1:7]] = i[10:-1]
        print("No."+str(count)+"  "+i[1:7]+"  "+i[10:-1])
    pandas.Series(fund_save).to_csv('fund.csv')


get_fund()
