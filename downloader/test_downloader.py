from unittest import TestCase

from requests import Response

from downloader.impl.http_request_downloader import AsyncHttpRequestDownloader, Request


class TestDownloader(TestCase):
    def test_request_downloader(self):
        url = "https://www.baidu.com"
        downloader = AsyncHttpRequestDownloader()

        for i in range(10):
            downloader.summit(Request(url))

        downloader.shutdown()
        while downloader.has_next_result():
            result = downloader.get_result()
            if result:
                response: Response = result.response
                print(response.status_code)
