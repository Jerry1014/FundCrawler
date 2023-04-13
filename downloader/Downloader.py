"""
http下载类 外观
"""
from typing import Optional, TypeVar, NoReturn


class BaseRequest:
    def __init__(self, url):
        self.url = url


R = TypeVar('R', bound=BaseRequest)


class BaseResponse:
    def __init__(self, request: R, result):
        self.request = request
        self.result = result


class AsyncHttpDownloader:
    def summit(self, request: BaseRequest) -> NoReturn:
        """
        提交http下载请求
        :param request: req
        """
        return NotImplemented

    def get_result(self) -> Optional[BaseResponse]:
        """
        获取下载结果
        :return 结果，None代表暂无结果
        """
        return NotImplemented

    def not_next_result(self) -> bool:
        """
        http爬取结果已经全部获取完
        :return: true，没有更多结果
        """
        return NotImplemented

    def shutdown(self) -> NoReturn:
        """
        爬取结束，可以关闭（不再接受summit，但仍然可以获取正在爬取的结果）
        """
        return NotImplemented
