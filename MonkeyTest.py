# -*- coding:UTF-8 -*-
"""
猴子测试嘛，就是瞎测两下的意思
"""
import time
import unittest


class MyTestCaseForGetFundList(unittest.TestCase):
    def test(self):
        from FundListProvider import GetFundListFromWebForTest
        tem = GetFundListFromWebForTest()
        my_iter = tem.get_fund_list()
        try:
            while True:
                a_fund = next(my_iter)
                print(a_fund)
        except StopIteration:
            pass


class MyTestCaseForCrawlingWebpage(unittest.TestCase):
    def test_for_get_page_context(self):
        from multiprocessing import Queue, Event
        from CrawlingCore import GetPageByWebWithAnotherProcessAndMultiThreading

        input_queue = Queue()
        output_queue = Queue()
        exit_after_finish = Event()
        test = GetPageByWebWithAnotherProcessAndMultiThreading(input_queue, output_queue, exit_after_finish)
        test.start()

        input_queue.put(('http://baidu.com', ('just', 'for', 'test')))
        input_queue.put(('http://www.10jqka.com.cn/', ('just', 'for', 'test')))
        exit_after_finish.set()
        while exit_after_finish.is_set(): time.sleep(1)

        while not output_queue.empty():
            print(output_queue.get())


class MyTestCaseForCrawling(unittest.TestCase):
    def test_write_to_file(self):
        import CrawlingFund
        context_filename = [('11', '11.txt'), ('22', '22.txt')]
        my_iter = CrawlingFund.write_to_file(False)
        my_iter.send(None)
        for i in context_filename:
            my_iter.send(i)


if __name__ == '__main__':
    unittest.main()
