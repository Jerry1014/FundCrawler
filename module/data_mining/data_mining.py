import logging
from typing import List, Optional

from module.abstract_data_mining_module import DataMiningModule
from module.data_mining.strategy.data_mining_strategy_factory import PageType, DataCleaningStrategyFactory
from module.fund_context import FundContext


class DataMining(DataMiningModule):
    def __init__(self, page_type_list: Optional[list[PageType]] = None):
        self._page_type_list = page_type_list if page_type_list else [i for i in PageType]

    def summit_context(self, context: FundContext) -> Optional[List[tuple[PageType, str]]]:
        url_list = list()
        for page_type in self._page_type_list:
            # 有返回就解析，没有就构造请求
            if page_type in context.http_response_dict:
                DataCleaningStrategyFactory.get_strategy(page_type) \
                    .fill_result(context.http_response_dict[page_type], context)
            else:
                try:
                    url = DataCleaningStrategyFactory.get_strategy(page_type).build_url(context)
                    url_list.append((page_type, url))
                except:
                    logging.error(f'基金{context.fund_code}类型{page_type}爬取失败')

        return url_list if url_list else None
