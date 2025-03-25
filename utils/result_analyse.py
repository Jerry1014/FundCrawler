import json
from csv import DictReader
from datetime import date, timedelta
from heapq import heappushpop, heappush
from typing import List

from constants import FundAttrKey, NO_DATA

"""
挑选基金
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

# 债型、其他的基金，根据夏普排序后，保留的候选基金数（对应策略中的第一步）
debt_shape_remain = 50
other_shape_remain = 20
# 长牛基金，保留的基金数
manager_long_remain = 10
# 长牛基金，需要基金经理管理超过多长时间（单位：年）
manager_4_n_years = 10
# 债型、其他的基金，根据回报率进行排序后，最终保留的基金数
debt_increase_remain = 5
other_increase_remain = 10


class FundFolder:
    """
    基金数据计算容器
    本质上是一个堆，增加了top k的限制，增加了基金信息的映射
    """

    def __init__(self, retain_num: int):
        """
        :param retain_num: 需要保留的基金数量
        """
        self._retain_num = retain_num
        self._heap: list[float] = []
        self._fund_dict: dict[float, list[dict]] = dict()

    def put_fund(self, value: float, fund_info: dict) -> None:
        if len(self._heap) < self._retain_num:
            # 容器未满直接push
            heappush(self._heap, value)
            if value in self._fund_dict:
                self._fund_dict[value].append(fund_info)
            else:
                self._fund_dict[value] = [fund_info]
            return

        # 容器满了，push and pop一下
        min_value = heappushpop(self._heap, value)
        if min_value == value:
            return
        self._fund_dict[min_value].pop()
        if value in self._fund_dict:
            self._fund_dict[value].append(fund_info)
        else:
            self._fund_dict[value] = [fund_info]

    def get_result(self) -> List[dict]:
        result = []
        for value in self._fund_dict.values():
            if value:
                result.extend(value)
        return result


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
                date_of_appointment: date = date.fromisoformat(row[FundAttrKey.DATE_OF_APPOINTMENT])
                delta: timedelta = today - date_of_appointment
                # 基金经理在本基金的上任时间超过三年
                manager_4_more_3_yeas = delta.days > 365 * 3
                # 基金经理在本基金 达到长期的标准
                manager_4_long_times = delta.days > 365 * manager_4_n_years
                # 三年夏普
                three_years_shape: str = row[FundAttrKey.SHARPE_THREE_YEARS]
                # 三年涨幅
                five_years_increase = row[FundAttrKey.FIVE_YEARS_INCREASE]
                # 三年涨幅
                fund_size = row[FundAttrKey.FUND_SIZE]

                # 基金经理上任不到3年，历史数据没有参考意义，直接pass
                if manager_4_more_3_yeas is False or three_years_shape == NO_DATA:
                    continue
                # 基金规模小于5亿，同样不考虑
                if float(fund_size) < 5:
                    continue

                # 这里只是做排序的第一步
                fund_type: str = row[FundAttrKey.FUND_TYPE]
                if '债' in fund_type:
                    # 债基排序策略 1 夏普 2 收益
                    debt_holder.put_fund(float(three_years_shape), row)
                elif manager_4_long_times and five_years_increase != NO_DATA:
                    # 长牛基金排序策略 1 收益
                    manager_long_years_holder.put_fund(float(five_years_increase[:-1]), row)
                elif not ('固收' in fund_type or '指数' in fund_type):
                    # 其他基金排序策略 1 夏普 2 收益
                    other_holder.put_fund(float(three_years_shape), row)
            except Exception as e:
                print(f'基金{row[FundAttrKey.FUND_CODE]}分析失败', e)

    # 从这开始，是第二步的排序
    # 债基排序策略 1 夏普 2 收益
    debt_increase_holder = FundFolder(retain_num=debt_increase_remain)
    for fund in debt_holder.get_result():
        increase = fund[FundAttrKey.THREE_YEARS_INCREASE]
        if increase != NO_DATA:
            debt_increase_holder.put_fund(float(increase[:-1]), fund)

    # 其他基金排序策略 1 夏普 2 收益
    other_increase_holder = FundFolder(retain_num=other_increase_remain)
    for fund in other_holder.get_result():
        increase = fund[FundAttrKey.THREE_YEARS_INCREASE]
        if increase != NO_DATA:
            other_increase_holder.put_fund(float(increase[:-1]), fund)

    print(f'债基\n{json.dumps(debt_increase_holder.get_result(), ensure_ascii=False)}')
    print(f'长牛\n{json.dumps(manager_long_years_holder.get_result(), ensure_ascii=False)}')
    print(f'其他\n{json.dumps(other_increase_holder.get_result(), ensure_ascii=False)}')


if __name__ == '__main__':
    analyse()
