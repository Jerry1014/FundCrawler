"""
将爬取结果 保存到csv文件
"""
import os.path
from csv import DictWriter
from typing import NoReturn

from module.fund_context import FundContext
from module.process_manager import SavingResultModule
from utils.constants import FundAttrKey


class SaveResult2File(SavingResultModule):
    default_restval = 'None'
    result_file_path = './result/'
    result_file_name = 'result.csv'

    def __init__(self):
        fieldnames = [header.value for header in FundAttrKey]

        if not os.path.exists(self.result_file_path):
            os.makedirs(self.result_file_path)

        self._file = open(self.result_file_path + self.result_file_name, 'w', newline='', encoding='utf-8')
        self._writer: DictWriter = DictWriter(self._file, fieldnames=fieldnames, restval=self.default_restval)

        self._writer.writeheader()

    def save_result(self, result: FundContext) -> NoReturn:
        self._writer.writerow({header.value: value if value else self.default_restval for header, value in
                               result.to_result_row().items()})

    def exit(self):
        self._file.close()
