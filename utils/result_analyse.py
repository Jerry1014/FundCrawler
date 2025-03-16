"""
自动挑选基金
整体思路是 风险和收益是正相关的，我们追求的就是收益/风险比最大化，因此夏普作为最基金的指标
将基金分为三类
1 债
夏普很高，但是风险很低，导致收益也低
先按夏普排序，拿到n个候选基金，再根据回报排序，最终选择其中的m个基金
2 长牛
对于管理超过n年的基金，就不再看夏普，纯纯看长期的回报
直接根据回报排序，选择n个基金
3 其他
首先看夏普，其他看回报，和债互补
先按夏普排序，拿到n个候选基金，再根据回报排序，最终选择其中的m个基金
"""
import json
from csv import DictReader
from datetime import date, timedelta
from heapq import heappushpop, heappush
from typing import NoReturn

from process_manager import FundCrawlingResult

# 债型、其他的基金，根据夏普挑选时，所保留的基金数（参与后续回报率排序）
debt_shape_remain = 200
other_shape_remain = 200
# 长牛基金，保留的基金数
manager_long_remain = 10
# 长牛基金，需要基金经理管理超过多长时间（单位：年）
manager_4_n_years = 10
# 债型、其他的基金，根据回报率进行排序后，最终保留的基金数
debt_increase_remain = 5
other_increase_remain = 10


def analyse():
    # 债基的夏普太高了，单独放一个池子里

    debt_holder = FundFolder(retain_num=debt_shape_remain)
    other_holder = FundFolder(retain_num=other_shape_remain)
    manager_long_years_holder = FundFolder(retain_num=manager_long_remain)

    with open('../result/result.csv', 'r', newline='', encoding='utf-8') as csvfile:
        # 读取数据
        reader: DictReader = DictReader(csvfile)

        today = date.today()
        for row in reader:
            try:
                date_of_appointment: date = date.fromisoformat(row[FundCrawlingResult.Header.DATE_OF_APPOINTMENT])
                delta: timedelta = today - date_of_appointment
                manager_4_more_3_yeas = delta.days > 365 * 3
                manager_4_long_times = delta.days > 365 * manager_4_n_years
                three_years_shape: str = row[FundCrawlingResult.Header.SHARPE_THREE_YEARS]
                three_years_increase = row[FundCrawlingResult.Header.THREE_YEARS_INCREASE]

                # 底线，不考虑基金经理管理低于3年的基金，历史数据没有参考意义
                if manager_4_more_3_yeas is False or three_years_shape == 'None':
                    continue

                fund_type: str = row[FundCrawlingResult.Header.FUND_TYPE]
                # 债基 1 夏普 2 收益
                if '债' in fund_type:
                    debt_holder.put_fund(float(three_years_shape), row)
                # 管理了非常长时间的基金 只看收益
                elif manager_4_long_times and three_years_increase != 'None':
                    manager_long_years_holder.put_fund(float(three_years_increase[:-1]), row)
                # 其他 1 夏普 2 收益
                else:
                    other_holder.put_fund(float(three_years_shape), row)
            except Exception as e:
                print(f'基金{row[FundCrawlingResult.Header.FUND_CODE]}分析失败', e)

    # 债基 夏普前十里再找收益前x的
    debt_increase_holder = FundFolder(retain_num=debt_increase_remain)
    for fund in debt_holder.get_result():
        increase = fund[FundCrawlingResult.Header.THREE_YEARS_INCREASE]
        if increase != 'None':
            debt_increase_holder.put_fund(float(increase[:-1]), fund)
    print(f'债基收益前三\n{json.dumps(debt_increase_holder.get_result(), ensure_ascii=False)}')

    # 管理了非常长时间的基金 只看收益
    print(f'长牛基收益排名\n{json.dumps(manager_long_years_holder.get_result(), ensure_ascii=False)}')

    # 其他基金前x
    other_increase_holder = FundFolder(retain_num=other_increase_remain)
    for fund in other_holder.get_result():
        increase = fund[FundCrawlingResult.Header.THREE_YEARS_INCREASE]
        if increase != 'None':
            other_increase_holder.put_fund(float(increase[:-1]), fund)
    print(f'其他基收益前三\n{json.dumps(other_increase_holder.get_result(), ensure_ascii=False)}')


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
