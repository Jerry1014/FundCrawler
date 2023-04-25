from unittest import TestCase

from module.save_result.save_result_2_flie import SaveResult2File
from task_manager import FundCrawlingResult


class TestSaveResult2File(TestCase):
    def test(self):
        with SaveResult2File() as save:
            save.save_result(FundCrawlingResult('1', '1'))

            save.save_result(FundCrawlingResult('1', '1'))
