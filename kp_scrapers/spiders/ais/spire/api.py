# -*- coding: utf-8 -*-

"""Spire API client.

Documentation: https://spire.com/contact/developer-portal/?access=true

"""

import json
import logging

from scrapy.exceptions import CloseSpider


logger = logging.getLogger(__name__)


# `fields=decoded` ensures we receive AIS data instead of a dry metada cursor.
BASE_URL_TPL = 'https://ais.spire.com/{resource}?fields=decoded&{extra}'

# TODO move it to `kp_scrapers.lib.http`
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404
HTTP_TOO_MANY_REQUESTS = 429
HTTP_UNPROCESSABLE = 422


def headers(token):
    return {'Accept': 'application/json', 'Authorization': 'Bearer {}'.format(token)}


def naive_urlencode(opts):
    """translate dict to `k=v&` uri parameters.

    `six.moves.urllib.parse.urlencode` obfuscates the query without serving any
    feature.

    Examples:
        >>> naive_urlencode({'foo': 'bar'})
        'foo=bar'
        >>> naive_urlencode({'foo': 'bar', 'a': True})
        'foo=bar&a=True'

    """
    return '&'.join([f'{k}={v}' for k, v in opts.items()])


def make_request_url(resource, **kwargs):
    """Builds and returns a Request to the SpireApi according to given arguments.

    Note:
        On can pass either a ``since`` argument or both a ``before`` and an
        ``after`` but not the tree altogether.
        If ``since`` is given, ``before`` and ``after`` will be ignored.

    Args:
        received_after (str):  A 'after' date in ISO-8601 format.
        received_before (str): A 'before' date in ISO-8601 format.
        since (str):  A 'since' string as returned by the SpireApi or stored
                      in the spider state during a previous run.

    """
    return BASE_URL_TPL.format(resource=resource, extra=naive_urlencode(kwargs))


def fail_fast(func):
    """Close spider with a friendly message on unexpected API responses."""

    def _inner(response):
        if response.status == HTTP_TOO_MANY_REQUESTS:
            raise CloseSpider(reason='rate limit reached')
        elif response.status == HTTP_UNPROCESSABLE:
            raise CloseSpider(reason='api couldnt understand the request')
        elif response.status == HTTP_FORBIDDEN:
            raise CloseSpider(reason='api denied access')
        elif response.status == HTTP_NOT_FOUND:
            raise CloseSpider(reason='no data returned')
        elif response.status >= 400:
            raise CloseSpider(reason=f'bad http code received: {response.status}')

        return func(response)

    return _inner


@fail_fast
def ResponseFromScrapy(raw):
    """Dummy wrapper to hide how we serialize Spire responses."""
    return SpireResponse(raw.body_as_unicode(), raw.status)


class SpireResponse(object):
    """Abstract Spire JSON responses layout.

    It basically wraps data like below:

            {
                "paging": { "limit", "since/next", "actual" },
                "data: [ { ... } ]"
            }

    """

    def __init__(self, response, status):
        try:
            self._data = json.loads(response)
        except ValueError:  # If response empty
            logger.warning("empty response received from Spire: {}".format(response))
            self._data = {}
            status = HTTP_NOT_FOUND

        self.status = status

    @property
    def data(self):
        """Safe getter on response actual data."""
        return self._data.get('data', [])

    @property
    def paging(self):
        return self._data.get('paging')

    @property
    def has_next_page(self):
        return self.paging is not None and len(self.data) == int(self.paging['limit'])

    def next_page(self, resource, **opts):
        paging = self.paging
        if 'since' in paging:
            return make_request_url(resource, since=paging['since'], **opts)
        else:
            raise ValueError('invalid paging data')
