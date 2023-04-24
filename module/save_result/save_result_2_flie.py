"""
将爬取结果 保存到csv文件
"""
from csv import DictWriter
from typing import NoReturn

from task_manager import SaveResultModule, FundCrawlingResult


class SaveResult2File(SaveResultModule):
    _restval = '-'

    def __init__(self):
        fieldnames = [header.value for header in FundCrawlingResult.FundInfoHeader]

        self._file = open('result.csv', 'w', newline='', encoding='utf-8')
        self._writer: DictWriter = DictWriter(self._file, fieldnames=fieldnames, restval=self._restval)

        self._writer.writeheader()

    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        row = {header.value: value if value else self._restval for header, value in result.fund_info_dict.items()}
        self._writer.writerow(row)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.close()
