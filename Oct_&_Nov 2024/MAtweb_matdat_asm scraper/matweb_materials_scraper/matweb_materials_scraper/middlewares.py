# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import json
import time

from scrapy import signals
from scrapy.http.response.text import TextResponse

class Utf8ResponseMiddleware:
    def process_response(self, request, response, spider):
        if isinstance(response, TextResponse):
            # Set the encoding to UTF-8
            response = response.replace(encoding='utf-8')
            # print('Utf8ResponseMiddleware Class Is Called')
        return response

class MatwebMaterialsScraperSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class MatwebMaterialsScraperDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.
        # if 'Unauthorized' in response.text and spider.name in ['ASM', 'ASM_IDLE']:
        #     print("Unauthorized response detected.")
        #
        #     if not self.auth_token:
        #         # Prompt the user to enter a new Authorization header value
        #         self.auth_token = input("Previous Token Expired. Enter the New Authorization header value: ").strip()
        #         self.auth_token = self.auth_token.replace("'", "").replace(",", "")
        #
        #         # Update the Authorization header
        #         spider.headers['Authorization'] = self.auth_token
        #
        #         # Delete the Cookie header if it exists
        #         if 'Cookie' in request.headers:
        #             del request.headers['Cookie']
        #
        #         print("Headers updated successfully, and cookies deleted.")
        #
        #     # Retry the original request with the updated headers
        #     request.headers = spider.headers
        #     return request.replace(headers=spider.headers, dont_filter=True)

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)

class ZyteSessionRotationMiddleware:
    def __init__(self, requests_per_session, zyte_api_key):
        self.requests_per_session = requests_per_session
        self.zyte_api_key = zyte_api_key
        self.request_count = 0
        self.current_session_id = None

    @classmethod
    def from_crawler(cls, crawler):
        requests_per_session = crawler.settings.getint('REQUESTS_PER_SESSION', 250)
        zyte_api_key = crawler.settings.get('ZYTE_API_KEY')
        return cls(requests_per_session, zyte_api_key)

    def process_request(self, request, spider):
        # Increment request count
        self.request_count += 1

        # Rotate session if limit reached
        if self.request_count >= self.requests_per_session:
            self.request_count = 0  # Reset counter
            self.current_session_id = None  # Reset session ID to force new session

        # Add Zyte session headers
        if not self.current_session_id:
            # Generate a new session ID
            self.current_session_id = f"session-{id(self)}-{int(time.time())}"

        # Add the Zyte session header to the request
        request.headers['X-Zyte-Session'] = self.current_session_id
