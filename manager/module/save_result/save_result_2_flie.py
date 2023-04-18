from csv import DictWriter
from typing import NoReturn

from manager.task_manager import SaveResultModule, FundCrawlingResult


class SaveResult2File(SaveResultModule):
    def __init__(self):
        fieldnames = [header.value for header in FundCrawlingResult.FundInfoHeader]

        self._file = open('./result.csv', 'w', newline='')
        self._writer: DictWriter = DictWriter(self._file, fieldnames=fieldnames)

        self._writer.writeheader()

    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        row = {header.value: '-' if value is None else value for header, value in result.fund_info_dict}
        self._writer.writerow(row)

    def stop(self):
        self._file.close()
