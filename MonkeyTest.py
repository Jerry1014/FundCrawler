import unittest
import CrawlingFund
import time


class MyTestCaseForFakeUA(unittest.TestCase):
    def test(self):
        from FakeUA import fake_ua
        for i in range(10):
            print(fake_ua.random)


class MyTestCaseForGetFundList(unittest.TestCase):
    def test(self):
        from ProvideTheListOfFund import GetFundListByWebForTest
        tem = GetFundListByWebForTest()
        my_iter = tem.get_fund_list()
        try:
            while True:
                print(next(my_iter))
        except StopIteration:
            print(f'end of iter {tem.sum_of_fund}')


class MyTestCaseForCrawling(unittest.TestCase):
    def test_get_page_context(self):
        url = ['http://baidu.com']

        for i in url:
            print(CrawlingFund.get_page_context(i))

    def test_parse_fund_info(self):
        url = ['http://fund.eastmoney.com/002939.html']

        my_iter = CrawlingFund.parse_fund_info()
        my_iter.send(None)
        for i in url:
            print(my_iter.send(CrawlingFund.get_page_context(i)[1]))

    def test_write_to_file(self):
        context_filename = [('11', '11.txt'), ('22', '22.txt')]
        my_iter = CrawlingFund.write_to_file()
        my_iter.send(None)
        for i in context_filename:
            my_iter.send(i)


if __name__ == '__main__':
    unittest.main()
