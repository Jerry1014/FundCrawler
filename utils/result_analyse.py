from csv import DictReader
from datetime import date
from heapq import heappushpop, heappush
from typing import NoReturn

from process_manager import FundCrawlingResult


def analyse():
    holder = FundFolder(retain_num=10)
    with open('../result/result.csv', 'r', newline='', encoding='utf-8') as csvfile:
        # 读取数据
        reader: DictReader = DictReader(csvfile)

        today = date.today()
        for row in reader:
            # 基金经理至少管理了这个基金n年以上
            # date_of_appointment: date = date.fromisoformat(row[FundCrawlingResult.Header.DATE_OF_APPOINTMENT])
            # delta: timedelta = today - date_of_appointment
            # if delta.days <= 365 * 4:
            #     continue

            # 不考虑债基
            fund_type: str = row[FundCrawlingResult.Header.FUND_TYPE]
            if '债' in fund_type:
                continue

            sharpe: str = row[FundCrawlingResult.Header.SHARPE_THREE_YEARS]
            if sharpe != 'None':
                holder.put_fund(float(sharpe), row)

    holder.show_result()


class FundFolder:
    def __init__(self, retain_num: int):
        """
        :param retain_num: 需要保留的基金数量
        """
        self._retain_num = retain_num
        self._sharpe_heap: list[float] = []
        self._sharpe_fund_dict: FundFolder.SpecialDict[float, list[str]] = FundFolder.SpecialDict()

    def put_fund(self, sharpe: float, fund_info: dict) -> NoReturn:
        if len(self._sharpe_heap) < self._retain_num:
            heappush(self._sharpe_heap, sharpe)
            self._sharpe_fund_dict[sharpe].append(fund_info)
            return

        min_sharp = heappushpop(self._sharpe_heap, sharpe)
        if min_sharp == sharpe:
            return
        self._sharpe_fund_dict[min_sharp].pop()
        self._sharpe_fund_dict[sharpe].append(fund_info)

    def show_result(self):
        result = []
        for value in self._sharpe_fund_dict.values():
            if value:
                result += value
        print(result)

    class SpecialDict(dict):
        def __missing__(self, key):
            new_list = []
            self[key] = new_list
            return new_list


if __name__ == '__main__':
    analyse()
