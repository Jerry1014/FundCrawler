from typing import List

from module.fund_info_bo import FundCrawlingResult
from module.process_manager import CrawlingTargetModule


class GetSpecialNeedCrawledFund(CrawlingTargetModule):
    """
    测试用的 基金任务 提供者
    """

    def get_fund_list(self) -> List[FundCrawlingResult]:
        # 基金目录
        fund_list = ({'code': '007746', 'name': '华安现金润利'}, {'code': '020282', 'name': '益民优势安享混合C'})

        return [FundCrawlingResult(t['code'], t['name']) for t in fund_list]
