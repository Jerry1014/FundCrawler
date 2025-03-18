from unittest import TestCase

from requests import Response

from module.abstract_downloader import BaseRequest
from module.downloader.download_by_requests import AsyncHttpRequestDownloader, Request


class TestDownloader(TestCase):
    class TestUniqueKey(BaseRequest.UniqueKey):
        def __init__(self):
            pass

    def test_request_downloader(self):
        url = "https://www.baidu.com"
        downloader = AsyncHttpRequestDownloader()

        for i in range(10):
            downloader.summit(Request(TestDownloader.TestUniqueKey(), url))

        downloader.shutdown()
        while downloader.has_next_result():
            result = downloader.get_result()
            if result:
                response: Response = result.response
                print(response.status_code)
