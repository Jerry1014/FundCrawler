from unittest import TestCase

from module.crawling_target.get_fund_by_web import GetFundByWeb


class TestGetFundByWeb(TestCase):
    def test_init(self):
        web = GetFundByWeb()
        print(f'total {web.total}')
        for fund in web.task_generator:
            print(f'{fund.code} {fund.name}')
