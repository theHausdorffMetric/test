import http
import logging


POSSIBLE_BEHAVIORS = ['finish', 'continue']
# it seems reasonable to think that hitting a 503 or 404 page usually means we
# reached the limit of the resource or are searching for something wrong. But
# we may play safe and simply finish, in order to not waste what was scraped so
# far
DEFAULT_BEHAVIOR = 'finish'

logger = logging.getLogger(__name__)


def translate_code(code):
    """Translate code to human reason from standard http codes meaning.

    Note that we don't try to be smart when no match happens. It's certainly a
    bug and it's certainely preferable to return something to inspect
    than`None` and hide the culprit.

    Args:
        code (int): http status code

    Returns:
        Tuple[int, str]:

    Examples:
        >>> translate_code(5000)
        >>> translate_code(503)
        (503, 'SERVICE_UNAVAILABLE')
        >>> translate_code(200)
        (200, 'OK')

    """
    matches = [status for status in http.HTTPStatus if status.value == code]
    if not matches:
        logger.error('Invalid HTTP status code: %s', code)
        return None

    return matches[0].value, matches[0].name


class _SpiderHTTPError(Exception):
    """An invalid response was received."""

    def __init__(self, response, *args, **kwargs):
        self.response = response
        super().__init__(*args, **kwargs)


class HTTPErrorMiddleware(object):
    """Don't tolerate HTTP errors.

    There is no reason to ignore silently rate limited answers, server errors,
    forbidden requests, and so on... It should be visible (on Sentry) and
    eventually close the spider (we don't want to break APIs or raise
    suspicions on tricky websites).

    Note that this middleware is almost equivalent to Scrapy's built-in
    `scrapy.spidermiddlewares.httperror.HttpErrorMiddleware`.
    It just brings full control over the process and start leveraging it with
    custom behaviour.

    """

    def __init__(self, on_error):
        logger.debug(f"Spider will '{on_error}' on bad http responses")
        self.on_error = on_error

    @staticmethod
    def invalid(response):
        """Cheap abstraction and syntax sugar of whata we consider to be invalid."""
        return response.status >= 400

    def process_spider_input(self, response, spider):
        """Preprocess spider responses for "invalid" HTTP codes.

        There are 2 behaviors possible, which completely depend on the source:
            - 'finish' (default) : this is highly unexpected and should fixed ASAP
              OR
              we hit the provider limit and should stop there, keeping the
              items scraped so far available for downstream processing
            - 'continue' : it's in our tolerance range, log and continue

        """
        # don't process valid responses
        if not self.invalid(response):
            return

        if self.on_error == 'finish':
            self._log_http_error(response)
            # keep calm and close the spider
            # rational: loaders usually ignore spiders with a status different
            # than `finished` (that makes sense, we can't trust the spider
            # output). In our case maybe the spider scraped hundreds of items
            # before being rate limited. And we most probably want to load them
            # even if the batch is incomplete (think of AIS messages)
            raise _SpiderHTTPError(response, f'{translate_code(response.status)}')

        elif self.on_error == 'continue':
            # tolerate HTTP error, log and continue
            self._log_http_error(response)

        return

    def process_spider_exception(self, response, exception, spider):
        if isinstance(exception, _SpiderHTTPError):
            return []

    @classmethod
    def from_crawler(cls, crawler):
        on_error = crawler.settings.get('ON_HTTP_ERROR') or DEFAULT_BEHAVIOR
        if on_error not in POSSIBLE_BEHAVIORS:
            logger.warning(
                f"Invalid behavior configured: {on_error}\n"
                "choose from {POSSIBLE_BEHAVIORS}\n"
                "{DEFAULT_BEHAVIOR} will be used instead"
            )
            on_error = DEFAULT_BEHAVIOR

        return cls(on_error)

    @staticmethod
    def _log_http_error(response):
        message = '{} {} (%(method)s %(url)s)'.format(*translate_code(response.status))
        logger.error(
            message,
            {'method': response.request.method, 'url': response.request.url},
            extra={
                'body': response.body,
                # TODO doesn't bring much value now to yield headers/cookies
                # headers=dict(response.request.headers),
                # cookies=response.request.cookies,
            },
        )
