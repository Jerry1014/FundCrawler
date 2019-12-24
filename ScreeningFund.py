# -*- coding:UTF-8 -*-
import os
import re

from eprogress import LineProgress

header_index_fund = '基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,成立来收益,基金经理,本基金任职时间,本基金任职收益,累计任职时间,\n'
header_guaranteed_fund = '基金代码,基金名称,近1月收益,近3月收益,近6月收益,近1年收益,近3年收益,保本期收益,基金经理,本基金任职时间,本基金任职收益,累计任职时间,\n'


def get_time_from_str(time_str):
    """
    将文字描述的任职时间转为列表
    :param time_str: 描述时间的str
    :return [int(多少年),int(多少天)]
    """
    tem = re.search(r'(?:(\d)年又|)(\d{0,3})天', time_str).groups()
    tem_return = list()
    for i in tem:
        if i:
            tem_return.append(int(i))
        else:
            tem_return.append(0)
    return tem_return


def data_analysis(fund_with_achievement, choice_return_this, choice_time_this):
    """
    按传入的筛选策略，筛选出符合要求的基金
    :param fund_with_achievement: 全部的基金信息文件名
    :param choice_return_this: 要求的基金收益率
    :param choice_time_this: 要求的任职时间
    """
    # 文件以a方式写入，先进行可能的文件清理
    try:
        os.remove(fund_choice_filename)
    except FileNotFoundError:
        pass

    try:
        with open(fund_choice_filename, 'w') as f:
            if fund_with_achievement == all_index_fund_with_msg_filename:
                f.write(header_index_fund)
            else:
                f.write(header_guaranteed_fund)

        print('开始筛选基金。。。')
        with open(fund_with_achievement, 'r') as f:
            count = 0
            all_lines = f.readlines()[1:]
            len_of_lines = len(all_lines)
            line_progress = LineProgress(title='基金筛选进度')

            for i in all_lines:
                # 逐条检查
                count += 1
                sign = 1

                # 取基金信息，并按收益率和任职时间分类
                _, _, one_month, three_month, six_month, one_year, three_year, from_st, _, this_tenure_time, \
                this_return, all_tenure_time, _ = i.split(',')
                return_all = [one_month, three_month, six_month, one_year, three_year, from_st, this_return]
                time_all = [this_tenure_time, all_tenure_time]

                # 信息未知或一月数据不存在（成立时间过短）的淘汰
                if one_month == '??' or one_month == '--':
                    continue

                # 收益率部分的筛选
                for j, k in zip(choice_return_this.values(), return_all):
                    if k == '--':
                        continue
                    if float(k[:-1]) < j:
                        sign = 0
                        break

                # 任职时间部分的筛选
                if sign == 1:
                    for j, k in zip(choice_time_this.values(), time_all):
                        for l, m in zip(j, get_time_from_str(k)):
                            if m > l:
                                break
                            elif m == l:
                                continue
                            else:
                                sign = 0
                                break

                # 符合要求的保存进文件
                if sign == 1:
                    with open(fund_choice_filename, 'a') as f2:
                        f2.write(i)
                line_progress.update(count * 100 // len_of_lines)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    fund_choice_filename = 'fund_choice.csv'  # 保存筛选出的基金
    all_index_fund_with_msg_filename = 'index_fund_with_achievement.csv'  # 指数/股票型基金完整信息

    # 对基金的筛选设置
    choice_return = {'近1月收益': 9.34, '近3月收益': 25.31, '近6月收益': 9.46, '近1年收益': 21.21,
                     '近3年收益': 25.00, '成立来收益/保本期收益': 0, '本基金任职收益': 20}
    choice_time = {'本基金任职时间': [3, 0], '累计任职时间': [5, 0]}

    # 筛选后的文件为fund_choice_filename的值，若还需要对保本型基金进来筛选，需要先备份
    data_analysis(all_index_fund_with_msg_filename, choice_return, choice_time)
