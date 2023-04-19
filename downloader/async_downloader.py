"""
http下载类 外观
"""
from abc import ABC, abstractmethod
from typing import Optional, TypeVar, NoReturn


class BaseRequest:
    class UniqueKey(ABC):
        """
        下载唯一键，用于确认某个请求是谁发起的
        """
        pass

    def __init__(self, unique_key: UniqueKey, url):
        self._unique_key = unique_key
        self.url = url


R = TypeVar('R', bound=BaseRequest)


class BaseResponse:
    def __init__(self, request: R, response):
        self.request = request
        self.response = response


class AsyncHttpDownloader(ABC):
    @abstractmethod
    def summit(self, request: BaseRequest) -> NoReturn:
        """
        提交http下载请求
        :param request: req
        """
        return NotImplemented

    @abstractmethod
    def get_result(self) -> Optional[BaseResponse]:
        """
        获取下载结果
        :return 结果，None代表暂无结果
        """
        return NotImplemented

    @abstractmethod
    def has_next_result(self) -> bool:
        """
        http爬取结果已经全部获取完
        在 shutdown后，当返回false时，可以认为所有的结果均已得到处理
        :return: false，当前结果已经处理完
        """
        return NotImplemented

    @abstractmethod
    def shutdown(self) -> NoReturn:
        """
        爬取结束，可以关闭（不再接受summit，但仍然可以获取正在爬取的结果）
        """
        return NotImplemented
