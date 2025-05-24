import json
from csv import DictReader
from datetime import date, timedelta
from heapq import heappushpop, heappush
from typing import List

from constants import FundAttrKey, NO_DATA
from utils.constants import DATA_IGNORE

"""
挑选基金
"""


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
    with open('../result/result.csv', 'r', newline='', encoding='utf-8') as csvfile:
        reader: DictReader = DictReader(csvfile)

        # 债基/股票 选择夏普排名前1%的 且经理任职超过十年
        debt_sharp = FundFolder(retain_num=100)
        stock_sharp = FundFolder(retain_num=100)
        other_sharp = FundFolder(retain_num=100)
        for row in reader:
            try:
                # 基金经理上任时间
                date_of_appointment: timedelta = date.today() - date.fromisoformat(row[FundAttrKey.DATE_OF_APPOINTMENT])

                fund_type: str = row[FundAttrKey.FUND_TYPE]
                fund_name: str = row[FundAttrKey.FUND_SIMPLE_NAME]
                if 'C' in fund_name or '美元现汇' in fund_name:
                    pass
                elif '纯债' in fund_name:
                    if date_of_appointment.days < 365 * 10:
                        continue
                    if (sharp_str := row[FundAttrKey.SHARP_RATE_TEN_YEARS]) != NO_DATA:
                        debt_sharp.put_fund(float(sharp_str), row)
                elif '固收' in fund_type or '黄金' in fund_name or '海外' in fund_type or 'QDII' in fund_type or '资源' in fund_name:
                    if date_of_appointment.days < 365 * 10:
                        continue
                    if (sharp_str := row[FundAttrKey.SHARP_RATE_TEN_YEARS]) != NO_DATA:
                        other_sharp.put_fund(float(sharp_str), row)
                else:
                    if date_of_appointment.days < 365 * 10 or '指数' in fund_type:
                        continue
                    if (sharp_str := row[FundAttrKey.SHARP_RATE_TEN_YEARS]) != NO_DATA:
                        stock_sharp.put_fund(float(sharp_str), row)
            except Exception as e:
                print(f'基金{row[FundAttrKey.FUND_CODE]}分析失败', e)

    # 债基/股票 在夏普靠前的部分再选收益高的
    debt_return: FundFolder = sort_by_return(debt_sharp)
    stock_return: FundFolder = sort_by_return(stock_sharp)
    other_return: FundFolder = sort_by_return(other_sharp)

    print(f'债基\n{json.dumps(debt_return.get_result(), ensure_ascii=False)}')
    print(f'股票\n{json.dumps(stock_return.get_result(), ensure_ascii=False)}')
    print(f'其他\n{json.dumps(other_return.get_result(), ensure_ascii=False)}')


def sort_by_return(sharp_holder) -> FundFolder:
    return_holder = FundFolder(retain_num=3)

    for fund in sharp_holder.get_result():
        if ((fund_return_str := fund[FundAttrKey.ANNUALIZED_RETURN_TEN_YEAR]) != NO_DATA
                and fund[FundAttrKey.MANAGEMENT_FEE_RATE] != DATA_IGNORE):
            fund_return = float(fund_return_str)
            management_fee = float(fund[FundAttrKey.MANAGEMENT_FEE_RATE][:1]) \
                if fund[FundAttrKey.MANAGEMENT_FEE_RATE] != NO_DATA else 0
            custody_fee = float(fund[FundAttrKey.CUSTODY_FEE_RATE][:1]) \
                if fund[FundAttrKey.CUSTODY_FEE_RATE] != NO_DATA else 0
            sales_service_fee = float(fund[FundAttrKey.SALES_SERVICE_FEE_RATE][:1]) \
                if fund[FundAttrKey.SALES_SERVICE_FEE_RATE] != NO_DATA else 0
            return_holder.put_fund(fund_return - (management_fee + custody_fee + sales_service_fee), fund)
    return return_holder


if __name__ == '__main__':
    analyse()
