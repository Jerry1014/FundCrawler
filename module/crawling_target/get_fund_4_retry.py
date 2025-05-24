from csv import DictReader
from typing import List

from module.crawling_target.get_fund_by_web import GetFundByWeb
from module.fund_context import FundContext
from module.saving_result.save_result_2_file import SaveResult2CSV
from utils.constants import FundAttrKey


class GetFund4Retry(GetFundByWeb):
    """
    和GetFundByWeb不同点在于，会跳过当前结果文件中已经存在的记录
    """

    def get_fund_list(self) -> List[FundContext]:
        all_fund_context_list = super().get_fund_list()
        fund_dict = {fund.fund_code: fund for fund in all_fund_context_list}

        with open(SaveResult2CSV.RESULT_FILE_PATH + SaveResult2CSV.RESULT_FILE_NAME, 'r', newline='',
                  encoding='utf-8') as csvfile:
            # 读取数据
            reader: DictReader = DictReader(csvfile)
            for row in reader:
                fund_code = row[FundAttrKey.FUND_CODE]
                if fund_code in fund_dict:
                    fund_dict.pop(fund_code)

        return list(fund_dict.values())
