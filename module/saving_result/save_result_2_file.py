"""
将爬取结果 保存到csv文件
"""
import os.path
from csv import DictWriter
from typing import NoReturn

from module.fund_context import FundContext
from module.process_manager import SavingResultModule
from utils.constants import FundAttrKey, DATA_ERROR


class SaveResult2File(SavingResultModule):
    RESULT_FILE_PATH = './result/'
    RESULT_FILE_NAME = 'result.csv'

    def __init__(self):
        fieldnames = [header.value for header in FundAttrKey]

        if not os.path.exists(self.RESULT_FILE_PATH):
            os.makedirs(self.RESULT_FILE_PATH)

        self._file = open(self.RESULT_FILE_PATH + self.RESULT_FILE_NAME, 'w', newline='', encoding='utf-8')
        self._writer: DictWriter = DictWriter(self._file, fieldnames=fieldnames)

        self._writer.writeheader()

    def save_result(self, result: FundContext) -> NoReturn:
        self._writer.writerow({header.value: value if value else DATA_ERROR for header, value in
                               result.to_result_row().items()})

    def exit(self):
        self._file.close()
