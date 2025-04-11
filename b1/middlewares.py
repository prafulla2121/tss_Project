# b1/middlewares.py
from fake_useragent import UserAgent

class RandomUserAgentMiddleware:
    def __init__(self):
        self.ua = UserAgent()

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        request.headers['User-Agent'] = self.ua.random