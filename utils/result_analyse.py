from csv import DictReader
from heapq import heappushpop
from typing import NoReturn

from process_manager import FundCrawlingResult


def analyse():
    holder = FundFolder(retain_num=10)
    with open('../result/result.csv', 'r', newline='', encoding='utf-8') as csvfile:
        # 读取数据
        reader: DictReader = DictReader(csvfile)

        for row in reader:
            sharpe = row[FundCrawlingResult.Header.SHARPE_THREE_YEARS]
            if sharpe != 'None':
                holder.put_fund(float(sharpe), row[FundCrawlingResult.Header.FUND_CODE])

    holder.show_result()


class FundFolder:
    def __init__(self, retain_num: int):
        """
        :param retain_num: 需要保留的基金数量
        """
        self._retain_num = retain_num
        self._sharpe_heap: list[float] = []
        self._sharpe_fund_dict: FundFolder.SpecialDict[float, list[str]] = FundFolder.SpecialDict()

    def put_fund(self, sharpe: float, fund_code: str) -> NoReturn:
        if len(self._sharpe_heap) < self._retain_num:
            self._sharpe_heap.append(sharpe)
            self._sharpe_fund_dict[sharpe].append(fund_code)
            return

        min_sharp = heappushpop(self._sharpe_heap, sharpe)
        if min_sharp == sharpe:
            return
        self._sharpe_fund_dict[min_sharp].pop()
        self._sharpe_fund_dict[sharpe].append(fund_code)

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
