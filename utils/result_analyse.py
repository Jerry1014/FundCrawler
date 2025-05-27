import json
from csv import DictReader
from datetime import date

from constants import FundAttrKey, NO_DATA, DATA_IGNORE
from utils.TopKHolder import TopKHolder


def analyse(fund_filter, tenure_day_filter):
    with open('../result/result.csv', 'r', newline='', encoding='utf-8') as csvfile:
        reader: DictReader = DictReader(csvfile)

        # 筛选基金类型
        meet_type_fund_list = list()
        for row in reader:
            fund_name: str = row[FundAttrKey.FUND_SIMPLE_NAME]
            fund_type: str = row[FundAttrKey.FUND_TYPE]
            fund_size: float = float(row[FundAttrKey.FUND_SIZE]) if row[FundAttrKey.FUND_SIZE] != NO_DATA else None

            if not fund_filter(fund_name, fund_type, fund_size):
                continue
            meet_type_fund_list.append(row)
        print(f'符合类型要求的基金数量为{len(meet_type_fund_list)}')

        # 满足时间要求的基金
        meet_tenure_fund_list = list()
        for row in meet_type_fund_list:
            # 基金经理上任天数
            tenure_days: int = (date.today() - date.fromisoformat(row[FundAttrKey.DATE_OF_APPOINTMENT])).days

            if not tenure_day_filter(tenure_days):
                continue
            meet_tenure_fund_list.append(row)
        print(f'符合时间要求的基金数量为{len(meet_tenure_fund_list)}')

        # 选择夏普排名前10%的基金
        fund_top_holder: TopKHolder = TopKHolder(lambda cur_row: float(cur_row[FundAttrKey.SHARP_RATE_TEN_YEARS]),
                                                 len(meet_tenure_fund_list) // 10)
        for row in meet_tenure_fund_list:
            if row[FundAttrKey.SHARP_RATE_TEN_YEARS] != NO_DATA \
                    and row[FundAttrKey.R_SQUARED_TO_IND] != NO_DATA \
                    and float(row[FundAttrKey.R_SQUARED_TO_IND]) > 60.0:
                fund_top_holder.put(row)
        top_sharp_fund_list = fund_top_holder.cur_k()

        # 根据阿尔法系数选择排名前三的基金
        alpha_holder = TopKHolder(
            lambda cur_row: float(row[FundAttrKey.ALPHA_TO_IND]) - get_annual_fee(cur_row), 3)
        for row in top_sharp_fund_list:
            if (row[FundAttrKey.ALPHA_TO_IND] != NO_DATA
                    and row[FundAttrKey.MANAGEMENT_FEE_RATE] != DATA_IGNORE):
                alpha_holder.put(row)
        alpha_fund_list = alpha_holder.cur_k()
        print(f'根据阿尔法选择的基金是\n{json.dumps(alpha_fund_list, ensure_ascii=False)}')

        # 根据最终的年化回报选择排名前三的基金
        return_holder = TopKHolder(
            lambda cur_row: float(row[FundAttrKey.ANNUALIZED_RETURN_TEN_YEAR]) - get_annual_fee(cur_row), 3)
        for row in top_sharp_fund_list:
            if (row[FundAttrKey.ANNUALIZED_RETURN_TEN_YEAR] != NO_DATA
                    and row[FundAttrKey.MANAGEMENT_FEE_RATE] != DATA_IGNORE):
                return_holder.put(row)
        return_fund_list = return_holder.cur_k()

        # 回报很好 但是阿尔法不高
        # 1 基金类型不纯 本次分析掺和了不同的类型
        # 2 也可以借此看下 挣钱的赛道是什么 可能是个不错的长坡
        return_without_alpha_fund_list = list()
        alpha_fund_set = {row[FundAttrKey.FUND_SIMPLE_NAME] for row in alpha_fund_list}
        for row in return_fund_list:
            if row[FundAttrKey.FUND_SIMPLE_NAME] not in alpha_fund_set:
                return_without_alpha_fund_list.append(row)
        print(f'年化回报优秀但阿尔法落后的基金是\n{json.dumps(return_without_alpha_fund_list, ensure_ascii=False)}')


def get_annual_fee(row):
    management_fee = float(row[FundAttrKey.MANAGEMENT_FEE_RATE][:1]) \
        if row[FundAttrKey.MANAGEMENT_FEE_RATE] != NO_DATA else 0
    custody_fee = float(row[FundAttrKey.CUSTODY_FEE_RATE][:1]) \
        if row[FundAttrKey.CUSTODY_FEE_RATE] != NO_DATA else 0
    sales_service_fee = float(row[FundAttrKey.SALES_SERVICE_FEE_RATE][:1]) \
        if row[FundAttrKey.SALES_SERVICE_FEE_RATE] != NO_DATA else 0
    return management_fee + custody_fee + sales_service_fee


if __name__ == '__main__':
    print('⬇️ 纯债基金分析 ⬇️')
    analyse(lambda fund_name, fund_type, fund_size: '债券型' in fund_type and '纯债' in fund_name
                                                    and fund_size is not None and fund_size > 10,
            lambda tenure_days: tenure_days > 7 * 365)
    print('⬆️ 纯债基金分析 ⬆️')
    print('⬇️ 国内货币基金分析 ⬇️')
    analyse(lambda fund_name, fund_type, fund_size: (('指数型' in fund_type and '海外股票' not in fund_type)
                                                     or ('混合型' in fund_type and '偏债' not in fund_type))
                                                    and fund_size is not None and fund_size > 10,
            lambda tenure_days: tenure_days > 10 * 365)
    print('⬆️ 国内货币基金分析 ⬆️')
    print('⬇️ 全部基金比较 ⬇️')
    analyse(lambda fund_name, fund_type, fund_size: fund_size is not None and fund_size > 10,
            lambda tenure_days: tenure_days > 5 * 365)
    print('⬆️ 全部基金比较 ⬆️')
