from typing import List, NoReturn

from module.abstract_data_mining_module import DataMiningModule
from module.data_mining.strategy.data_mining_strategy_factory import PageType, DataCleaningStrategyFactory
from module.downloader.download_by_requests import Request
from module.downloader.download_by_requests_v2 import RequestV2
from module.fund_context import FundContext


class DataMining(DataMiningModule):
    def __init__(self, page_type_list: list[PageType] = None):
        self._page_type_list = page_type_list if page_type_list else [i for i in PageType]

    def summit_context(self, context: FundContext) -> List[Request] | NoReturn:
        request_list = list()
        for page_type in self._page_type_list:
            # 有返回就解析，没有就构造请求
            if page_type in context.http_response_dict:
                DataCleaningStrategyFactory.get_strategy(page_type) \
                    .fill_result(context.http_response_dict[page_type], context)
            else:
                url = DataCleaningStrategyFactory.get_strategy(page_type).build_url(context)
                request_list.append(RequestV2(context.fund_code, page_type, url))

        return request_list if len(request_list) > 0 else None
