"""
http下载类 外观
"""


class BaseRequest:
    def __init__(self, url):
        self.url = url


class BaseResponse:
    def __init__(self, request: BaseRequest, result):
        # todo 这里或许还是需要传递一些跟这个请求相关的东西，例如说 url（来源）之类的
        self.result = result
        self.request = request


class HttpDownloader:
    def summit(self, request: BaseRequest):
        return NotImplemented

    def get_result(self) -> BaseResponse | None:
        """
        :return 结果，None代表下载失败
        """
        return NotImplemented
