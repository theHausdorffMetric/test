# -*- coding: utf-8 -*-

"""Lloyds unofficial API client.

Each day the website publish a new charter report in the afternoon (CET, to be
specified) and history is available.


Usage
~~~~~~

Choose how many days (i.e. reports) you want to scrape.

    $ scrapy crawl LloydsAPI -a history=3

One can also restrict dates in the past by passing an offset number of days
from now. The history parameter will then take into account this offset too.

    $ scrapy crawl LloydsAPI -a history=3 -a rewind=6


Glossary
~~~~~~~~

- {R,C,D}NR: {Rate, Charterer, Date} Not Reported (will be ignored and set to None)


Notes
~~~~~

If you are reverse-engineering the url in use, you will get an url-encoded
string you can decode like this:

    .. code-block::

        import urllib
        urllib.unquote('my_url').decode('utf-8')

"""

from __future__ import absolute_import, unicode_literals
import datetime as dt
import json

import scrapy
from scrapy.http import Request
from six.moves import range
from six.moves.urllib import parse

from kp_scrapers.spiders.bases.markers import DeprecatedMixin
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.lloyds.common import extract_item


BASE_URL = 'https://lloydslist.maritimeintelligence.informa.com'
API_URL = '{base}/Download/JsonDataFromFeed/ReadJsonMarketFixture/'.format(base=BASE_URL)
FEED_URL = 'http://sidws-maritimeintelligence.informa.com:8080/SIDWebservices/TankerFixtures?report_date={0}'  # noqa
FIXTURES = ['DirtyFixtures', 'CleanFixtures']
# arbitrary kpler mapping that makes sense in july 2017
COMMOS = {'DirtyFixtures': 'crude', 'CleanFixtures': 'other'}


def _format_date(raw_date):
    """Transform raw datetime into the expected format.

    Example:
        >>> from datetime import datetime
        >>> _format_date(datetime(2017, 7, 4))
        '04-Jul-17'

    """
    return raw_date.strftime('%d-%b-%y')


def _build_url(report_date):
    params = parse.urlencode({'dateVal': _format_date(report_date), 'feedUrl': FEED_URL})

    return '?'.join([API_URL, params])


class LloydsAPISpider(DeprecatedMixin, CharterSpider, scrapy.Spider):
    name = 'LloydsAPI'

    version = '1.0.0'
    provider = 'lloyds'

    def __init__(self, history=0, rewind=0):
        super(LloydsAPISpider, self).__init__()

        # TODO support for date range from cli
        self.report_dates = [
            dt.datetime.utcnow() - dt.timedelta(days=(i + int(rewind)))
            for i in range(int(history) + 1)
        ]

    def start_requests(self):
        for report_date in self.report_dates:
            self.logger.info('requesting report on {}'.format(report_date))
            yield Request(
                url=_build_url(report_date), meta={'report_date': report_date}, callback=self.parse
            )

    def parse(self, response):
        data = json.loads(response.body)[0]

        for fixture in data['CleanFixtures']:
            fixture['commodity'] = 'clean'
            fixture['reported_date'] = response.meta['report_date'].strftime('%Y-%m-%d')
            item = extract_item(fixture)
            if item is None:
                self.logger.warning('failed to parse item: {}'.format(fixture))
                continue

            yield item

        for lloyds_commo, kp_commo in COMMOS.items():
            for fixture in data[lloyds_commo]:
                fixture.update(
                    {
                        'commodity': kp_commo,
                        'reported_date': response.meta['report_date'].strftime('%Y-%m-%d'),
                    }
                )
                item = extract_item(fixture)

                if item is None:
                    self.logger.warning('failed to parse item: {}'.format(fixture))
                    continue

                yield item
