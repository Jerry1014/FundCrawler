from typing import NoReturn

from module.crawling_data.data_mining.data_cleaning_strategy_factory import DataCleaningStrategy


class OverviewDataCleaningStrategy(DataCleaningStrategy):
    def fill_result(self, response, result) -> NoReturn:
        print(f'爬取结果 url:{response.url}')
