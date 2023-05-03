"""
选择要买的基金
整体思路是
1 风险和收益是正相关的，因此夏普是一个很好的衡量基金性价比的指标
因此，取近三年夏普排名靠前的基金
2.1 低风险池子
在夏普前十的债基里，取收益率靠前的两三个
2.2 高风险池子
在夏普前十的除债基外的基金里，取收益率靠前的两三个
"""
from csv import DictReader
from datetime import date, timedelta
from heapq import heappushpop, heappush
from typing import NoReturn

from process_manager import FundCrawlingResult


def analyse():
    # 债基的夏普太高了，单独放一个池子里
    debt_holder = FundFolder(retain_num=10)
    other_holder = FundFolder(retain_num=10)

    with open('../result/result.csv', 'r', newline='', encoding='utf-8') as csvfile:
        # 读取数据
        reader: DictReader = DictReader(csvfile)

        today = date.today()
        for row in reader:
            try:
                # 基金经理至少管理了这个基金n年以上
                date_of_appointment: date = date.fromisoformat(row[FundCrawlingResult.Header.DATE_OF_APPOINTMENT])
                delta: timedelta = today - date_of_appointment
                if delta.days <= 365 * 4:
                    continue

                # 不考虑没有三年夏普的基金
                sharpe: str = row[FundCrawlingResult.Header.SHARPE_THREE_YEARS]
                if sharpe == 'None':
                    continue

                # 债基单独放一个篮子里
                fund_type: str = row[FundCrawlingResult.Header.FUND_TYPE]
                if '债' in fund_type:
                    debt_holder.put_fund(float(sharpe), row)
                else:
                    other_holder.put_fund(float(sharpe), row)
            except:
                print(f'基金{row[FundCrawlingResult.Header.FUND_CODE]}分析失败')

    debt_increase_holder = FundFolder(retain_num=3)
    for fund in debt_holder.get_result():
        increase = fund[FundCrawlingResult.Header.THREE_YEARS_INCREASE]
        if increase != 'None':
            debt_increase_holder.put_fund(float(increase[:-1]), fund)
    print(f'债基收益前三{debt_increase_holder.get_result()}')

    other_increase_holder = FundFolder(retain_num=3)
    for fund in other_holder.get_result():
        increase = fund[FundCrawlingResult.Header.THREE_YEARS_INCREASE]
        if increase != 'None':
            other_increase_holder.put_fund(float(increase[:-1]), fund)
    print(f'其他基收益前三{other_increase_holder.get_result()}')


class FundFolder:
    def __init__(self, retain_num: int):
        """
        :param retain_num: 需要保留的基金数量
        """
        self._retain_num = retain_num
        self._heap: list[float] = []
        self._fund_dict: FundFolder.SpecialDict[float, list[str]] = FundFolder.SpecialDict()

    def put_fund(self, value: float, fund_info: dict) -> NoReturn:
        if len(self._heap) < self._retain_num:
            heappush(self._heap, value)
            self._fund_dict[value].append(fund_info)
            return

        min_value = heappushpop(self._heap, value)
        if min_value == value:
            return
        self._fund_dict[min_value].pop()
        self._fund_dict[value].append(fund_info)

    def get_result(self):
        result = []
        for value in self._fund_dict.values():
            result.extend(value)
        return result

    class SpecialDict(dict):
        def __missing__(self, key):
            new_list = []
            self[key] = new_list
            return new_list


if __name__ == '__main__':
    analyse()
