# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
import datetime as dt
import itertools
import logging
import re

from dateutil.relativedelta import relativedelta
from scrapy import Request
from scrapy.selector import Selector
from six.moves import range

from kp_scrapers.lib.excel import GenericExcelExtractor
from kp_scrapers.models.items import StockExchangeIndex
from kp_scrapers.spiders.prices import PriceSpider


logger = logging.getLogger(__name__)


class PAJExcelExtractor(GenericExcelExtractor):
    def dollars(self):
        return self.book.sheet_by_name('Dollars')

    def years(self):
        rey = re.compile('^(\d{4})\s+1$')
        for row, cell in enumerate(self.dollars().col(0)):
            if cell.ctype == 1 and rey.match(cell.value):
                yield (row, rey.match(cell.value).group(1))

    def months(self, start_row, col):
        for off in range(12):
            try:
                c = self.dollars().cell(start_row + off, col)
            except IndexError:
                yield (off + 1, None)
            if c.ctype == 0:
                yield (off + 1, None)
            else:
                yield (off + 1, c.value)

    def parse(self, col):
        res = []
        for i, y in self.years():
            for m, c in self.months(i, col):
                logger.debug('{} {} {} {}'.format(i, y, m, c))
                res.append((int(y), m, c))
        return res


class PAJSpider(PriceSpider):
    name = 'PAJ'

    def __init__(self, history=False):
        self.history = history

    def start_requests(self):
        return [Request(url='http://www.paj.gr.jp/english/statis/', callback=self.fetch)]

    def fetch(self, response):
        sel = Selector(response)

        x = sel.xpath("//a[text()[contains(.,'Oil Import Price')]]/@href").extract_first()
        yield Request(url='http://www.paj.gr.jp' + x, callback=self.xls)

    def xls(self, response):
        def __get_items(xl, col, unit, commodity, index, cf=1.0, forward=None):
            r = [x for x in xl.parse(col) if x[2] is not None]
            # keep only the last one
            if not self.history:
                r = [r[-1]]
            for elt in r:
                month = dt.date(elt[0], elt[1], 1)
                month = month if not forward else month + relativedelta(months=+forward)
                yield StockExchangeIndex(
                    raw_unit=unit,
                    raw_value=elt[2],
                    converted_value=elt[2] * cf,
                    source_power=1,
                    index=index,
                    commodity=commodity,
                    zone='Japan',
                    provider='PAJ',
                    month=month,
                    day=dt.date.today(),
                )

        xl = PAJExcelExtractor(response.body, response.url, dt.datetime.now())
        return itertools.chain(
            __get_items(xl, 1, 'usd/mmbtu', 'lng', 'JCC', cf=0.14, forward=3),
            __get_items(xl, 9, 'usd/tons', 'lpg', 'Japan'),
        )
