from typing import List

from module.fund_context import FundContext
from module.process_manager import CrawlingTargetModule


class GetSpecialFund(CrawlingTargetModule):
    """
    测试用的 基金任务 提供者
    """

    def get_fund_list(self) -> List[FundContext]:
        # 基金目录
        fund_list = ({'code': '023713', 'name': '国联安主题驱动混合C'}, {'code': '023178', 'name': '安信价值精选股票C'})

        return [FundContext(t['code'], t['name']) for t in fund_list]
