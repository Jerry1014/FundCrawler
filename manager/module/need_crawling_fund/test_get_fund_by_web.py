from unittest import TestCase

from manager.module.need_crawling_fund.get_fund_by_web import GetNeedCrawledFundByWeb


class TestGetNeedCrawledFundByWeb(TestCase):
    def test_init(self):
        web = GetNeedCrawledFundByWeb()
        print(f'total {web.total}')
        for fund in web.task_generator:
            print(f'{fund.code} {fund.name}')
