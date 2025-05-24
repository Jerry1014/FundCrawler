from typing import List

from module.fund_context import FundContext
from module.process_manager import CrawlingTargetModule


class GetSpecialFund(CrawlingTargetModule):
    """
    测试用的 基金任务 提供者
    """

    def get_fund_list(self) -> List[FundContext]:
        # 基金目录
        fund_list = ({'code': '000970', 'name': '东方红睿元混合'}, {'code': '001298', 'name': '金鹰民族新兴混合A'})

        return [FundContext(t['code'], t['name']) for t in fund_list]
