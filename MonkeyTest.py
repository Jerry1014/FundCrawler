import unittest
import CrawlingFund
import time


class MyTestCase(unittest.TestCase):
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

if __name__ == '__main__':
    unittest.main()
