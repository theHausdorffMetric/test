from unittest import TestCase

from scrapy.http import Request, Response

from kp_scrapers.lib.request import allow_inline_requests


def _consume(callback, *args):
    """Consume requests and mock a Scrapy spider's workflow.
    """
    req = next(callback(*args))
    while req:
        yield req
        try:
            resp = Response(req.url, request=req)
            req = next(req.callback(resp))
        except (TypeError, StopIteration):
            break


class MockSpider:
    @allow_inline_requests
    def parse_without_callback(self, response):
        yield Request('http://example/1')
        yield Request('http://example/2')

    @allow_inline_requests
    def parse_with_callback(self, response):
        yield Request('http://example/1', callback=self._noop)

    def _noop(self, response):
        pass


class InlineRequestsTestCase(TestCase):
    def test_inline_requests_without_callback(self):
        # given
        spider = MockSpider()

        # when
        results = [
            resp.url for resp in _consume(spider.parse_without_callback, Response('http://example'))
        ]

        # then
        self.assertEqual(results, ['http://example/1', 'http://example/2'])

    def test_inline_request_with_callback(self):
        # given
        spider = MockSpider()

        # when
        with self.assertRaises(ValueError) as context:
            for resp in _consume(spider.parse_with_callback, Response('http://example.com')):
                pass

        # then
        self.assertRegex(
            str(context.exception),
            r'^Request contains callback <bound method MockSpider._noop of <(.+)>, not supported$',
        )
