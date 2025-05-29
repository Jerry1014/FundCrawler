from typing import List

from module.fund_context import FundContext
from module.process_manager import CrawlingTargetModule


class GetSpecialFund(CrawlingTargetModule):
    """
    测试用的 基金任务 提供者
    """

    def get_fund_list(self) -> List[FundContext]:
        # 基金目录
        fund_list = ({'code': '003244', 'name': '摩根中国世纪灵活配置混合美元现钞（QDII）'},)

        return [FundContext(t['code'], t['name']) for t in fund_list]
