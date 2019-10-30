import unittest
import CrawlingFund


class MyTestCase(unittest.TestCase):
    def test_get_page_context(self):
        url = ['http://baidu.com']

        my_test_iter = CrawlingFund.get_page_context()
        my_test_iter.send(None)
        for i in url:
            print(my_test_iter.send(i))


if __name__ == '__main__':
    unittest.main()
