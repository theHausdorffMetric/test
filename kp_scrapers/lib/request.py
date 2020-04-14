from functools import partial, wraps
import inspect
import logging
from types import GeneratorType

from scrapy import Request
from scrapy.utils.spider import iterate_spider_output
from six import create_bound_method


logger = logging.getLogger(__name__)


def allow_inline_requests(method):
    """Decorator for supporting inline scrapy requests.

    An illustrated example:

        class MySpider(Spider):
            @allow_inline_requests
            def parse(self, response):
                results = [response]
                for next_url in [f'http://quotes.toscrape.com/page/{i}/' for i in range(1, 10)]:
                    next_resp = yield Request(next_url)
                    results.append(next_resp)

                yield {'links': results}

    Args:
        method (Callable[[object, scrapy.Response], scrapy.Response]): spider method

    """
    if not list(inspect.signature(method).parameters.keys()):
        # decorated callback must be a spider method
        raise TypeError('Function must accept at least one argument.')

    @wraps(method)
    def _wrapper(self, response, **kwargs):
        callback = create_bound_method(method, self)
        _generator = RequestManager(callback, **kwargs)
        return _generator(response)

    return _wrapper


class RequestManager:
    """Wrap the callback and output requests sequentially.
    """

    def __init__(self, callback, **kwargs):
        """Init callbacks and their arguments.

        Args:
            callback (Callable[[object, scrapy.Response], scrapy.Response]): spider method
            **kwargs:

        """
        self._response_callback = callback
        self._kwargs = kwargs

    def __call__(self, response):
        """Main response entry point.

        This method calls the callback and wraps the returned generator.
        The decorated method must return a generator.

        Args:
            response (scrapy.Response):

        Returns:
            scrapy.Request:

        """
        output = iterate_spider_output(self._response_callback(response, **self._kwargs))
        if not isinstance(output, GeneratorType):
            raise TypeError(f'{self._response_callback.__name__} must return a generator')

        return self.resume(output)

    def resume(self, generator, previous=None):
        """Resume request generator.

        Args:
            generator (GeneratorType):
            previous (Optional[GeneratorType]): previous inline request, if any

        Yields:
            scrapy.Request:

        """
        while True:
            if previous:
                request, previous = previous, None
            else:
                try:
                    request = next(generator)
                except StopIteration:
                    break

            if isinstance(request, Request):
                yield self._wrap_request(request, generator)
                return

            # catches `yield` not part of inline request coroutine
            # can be either a request with callback, an item, or NoneType
            yield request

    def _wrap_request(self, request, generator):
        """Wrap request and handle successes (200) and other errors.

        Allowing existing callback or errbacks could lead to undesired results.
        To ensure the generator is always properly exhausted we must handle both callback and
        errback in order to send the result back to the generator.

        Args:
            request (scrapy.Request):
            generator (GeneratorType):

        Returns:
            scrapy.Request:

        """
        # sanity check; we don't want to mix callback and non-callback requests
        if request.callback is not None:
            raise ValueError(f'Request contains callback {request.callback}, not supported')

        if request.errback is not None:
            raise ValueError(f'Request contains errback {request.errback}, not supported')

        request.callback = partial(self._handle_success, generator=generator)
        request.errback = partial(self._handle_failure, generator=generator)
        return request

    def _handle_success(self, response, generator):
        if response.request:
            self._clean_request(response.request)

        try:
            request = generator.send(response)
        except StopIteration:
            return

        return self.resume(generator, request)

    def _handle_failure(self, failure, generator):
        # look for the request instance in the exception value
        if hasattr(failure.value, 'request'):
            self._clean_request(failure.value.request)
        elif hasattr(failure.value, 'response'):
            if hasattr(failure.value.response, 'request'):
                self._clean_request(failure.value.response.request)

        try:
            # see https://bit.ly/2Hw2Muw for handling errors
            request = failure.throwExceptionIntoGenerator(generator)
        except StopIteration:
            return

        return self.resume(generator, request)

    def _clean_request(self, request):
        request.callback = None
        request.errback = None
