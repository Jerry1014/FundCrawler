"""
将爬取结果 保存到csv文件
"""
import os.path
from csv import DictWriter
from typing import Optional

from module.fund_context import FundContext
from module.process_manager import SavingResultModule
from utils.constants import DATA_ERROR


class SaveResult2CSV(SavingResultModule):
    RESULT_FILE_PATH = './result/'
    RESULT_FILE_NAME = 'result.csv'

    def __init__(self, mode: str = 'w'):
        if not os.path.exists(self.RESULT_FILE_PATH):
            os.makedirs(self.RESULT_FILE_PATH)

        self._file = open(self.RESULT_FILE_PATH + self.RESULT_FILE_NAME, mode, newline='', encoding='utf-8')
        self._writer: Optional[DictWriter] = None

    def save_result(self, result: FundContext) -> None:
        if self._writer is None:
            fieldnames = [header.value for header in result.to_result_row().keys()]
            self._writer: DictWriter = DictWriter(self._file, fieldnames=fieldnames)
            if self._file.mode == 'w':
                self._writer.writeheader()

        self._writer.writerow({header.value: value if value else DATA_ERROR for header, value in
                               result.to_result_row().items()})

    def exit(self):
        self._file.flush()
        self._file.close()
