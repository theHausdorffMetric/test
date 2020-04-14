from scrapy import Request

from kp_scrapers.constants import BLANK_START_URL
from kp_scrapers.lib.request import allow_inline_requests
from kp_scrapers.spiders._internals import InternalSpider


class NoopSpider(InternalSpider):
    """Dumb spider that does nothing.

    Only intended for speeding up manual tests of everything around spiders:
    runtime, extensions, middlewares, ...

    To use this spider, simply add an additional instance method below with
    the following template:

        def my_noop_method(self, response):
            # do something with `response`, if required
            # else, simply test your own changes

    """

    name = 'Noop'
    version = '2.0.0'
    provider = '-'
    produces = []

    start_urls = (BLANK_START_URL,)

    def __init__(self, method, *args, **kwargs):
        """Init NoopSpider with arguments to call selected methods."""
        super().__init__(*args, **kwargs)

        self._attr_name = method

    def parse(self, response):
        if not hasattr(self, self._attr_name):
            self.logger.error(f"Method not yet implemented: {self._attr_name}")
            return

        return getattr(self, self._attr_name)(response)

    def noop(self, response):
        yield {'result': 'ok'}

    def trigger_sentry(self, response):
        msg = '[TEST] I should be on Sentry'
        self.logger.critical(msg)
        raise ValueError(msg)

    @allow_inline_requests
    def synchronous_request(self, _):
        results = []
        for next_url in [f'http://quotes.toscrape.com/page/{i}/' for i in range(1, 5)]:
            next_resp = yield Request(next_url)
            results.append(next_resp)

        yield {'links': results}
