from unittest import TestCase

from manager.module.save_result.save_result_2_flie import SaveResult2File
from manager.task_manager import FundCrawlingResult


class TestSaveResult2File(TestCase):
    def test(self):
        with SaveResult2File() as save:
            save.save_result(FundCrawlingResult(
                {FundCrawlingResult.FundInfoHeader.FUND_CODE: '1'}
            ))

            save.save_result(FundCrawlingResult(
                {FundCrawlingResult.FundInfoHeader.FUND_NAME: '1'}
            ))
