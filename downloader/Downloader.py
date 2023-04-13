"""
http下载类 外观
"""
from typing import Optional, TypeVar


class BaseRequest:
    def __init__(self, url):
        self.url = url


R = TypeVar('R', bound=BaseRequest)


class BaseResponse:
    def __init__(self, request: R, result):
        self.request = request
        self.result = result


class HttpDownloader:
    def summit(self, request: BaseRequest):
        return NotImplemented

    def get_result(self) -> Optional[BaseResponse]:
        """
        :return 结果，None代表下载失败
        """
        return NotImplemented
