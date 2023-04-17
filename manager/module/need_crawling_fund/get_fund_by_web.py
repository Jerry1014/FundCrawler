import re
from typing import NoReturn

import requests

from manager.task_manager import NeedCrawledFundModule


class GetNeedCrawledFundByWeb(NeedCrawledFundModule):

    def init(self) -> NoReturn:
        header = {"User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/78.0.3904.108 Safari/537.36'}
        page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                            'desc&page=1,&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)

        # 基金目录
        fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
        self.total = len(fund_list)

        self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(i[1:7], i[10:-1]) for i in fund_list)
