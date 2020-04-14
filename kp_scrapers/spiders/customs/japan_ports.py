# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
import csv
from datetime import date
from io import StringIO

from scrapy.http import Request
from scrapy.selector import Selector
from six.moves import zip

from kp_scrapers.lib.date import rewind_time
from kp_scrapers.models.items import Customs
from kp_scrapers.settings import MONTH_LOOK_BACK_CUSTOMS_SPIDERS
from kp_scrapers.spiders.bases.markers import LngMarker
from kp_scrapers.spiders.customs.base import CustomsBaseSpider


MONTHS = ['Jan', 'Feb', 'Mar', 'Apl', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

PORT_CODES = {
    'TOKYO CUSTOMS(OTHERS)': [120, 122],
    'YOKOHAMA': [200],
    'YOKOHAMA CUSTOMS(OTHERS)': [202, 220, 222, 260],
    'KOBE CUSTOMS(OTHERS)': [303, 322, 345, 360],
    'OSAKA CUSTOMS(OTHERS)': [402],
    'NAGOYA': [500],
    'NAGOYA CUSTOMS(OTHERS)': [520, 540],
    'MOJI': [603, 604, 628, 640],
    'NAGASAKI': [700, 750],
    'HAKODATE': [814, 822],
    'OKINAWA CHIKU': [900],
}


class JapanCustomsPortsSpider(LngMarker, CustomsBaseSpider):
    name = 'JapanCustomsPorts'
    version = '1.0.0'
    # NOTE no provider found
    provider = None

    def __init__(self, months_look_back=None, *args, **kwargs):
        super(JapanCustomsPortsSpider, self).__init__(*args, **kwargs)
        self.MONTH_LOOK_BACK_CUSTOMS_SPIDERS = int(
            months_look_back or MONTH_LOOK_BACK_CUSTOMS_SPIDERS
        )

    def start_requests(self):
        url = 'http://www.e-stat.go.jp/SG1/estat/OtherListE.do?bid=000001008834&cycode=1'
        self.logger.info(
            'Rewinding %s months before current month' % self.MONTH_LOOK_BACK_CUSTOMS_SPIDERS
        )
        yield Request(url=url, callback=self.get_yearly_pages)

    def get_yearly_pages(self, response):
        past_dates = rewind_time(
            current_date=date.today(), months=self.MONTH_LOOK_BACK_CUSTOMS_SPIDERS
        )
        years = set([past_date.year for past_date in past_dates])
        for year in years:  # 8 times (2008--->2015)
            months = [past_date.month for past_date in past_dates if past_date.year == year]
            sel = Selector(response)
            link = sel.xpath(
                '//tr[contains(., "' + str(year) + '")]//a[text()="Jan."]/@href'
            ).extract()[0]
            url = 'http://www.e-stat.go.jp/SG1/estat/' + link[1:]
            yield Request(url=url, callback=lambda r: self.list_of_tables(r, months))

    def list_of_tables(self, response, months):
        sel = Selector(response)
        for port in PORT_CODES:
            not_contains = ''
            if 'OTHERS' not in port:
                not_contains = 'and not(contains(., "OTHERS"))'
            selection_value = (
                '//table//tr[count(td)>=3 and '
                'td[position()=2 and contains(.,"{}") {}]]'.format(port, not_contains)
            )
            rows = sel.xpath(selection_value)
            if len(rows) > 1:
                msg = 'Multi rows for same port %s on link %s' % (port, response.url)
                self.logger.error(msg)
                raise Exception(msg)
            elif len(rows) == 1:
                link = sel.xpath(selection_value + '[1]//a/@href').extract()
                if len(link) == 1:
                    url = 'http://www.e-stat.go.jp/SG1/estat/' + link[0][2:]
                    yield Request(url=url, meta={'months': months, 'port': port})
                elif len(link) > 1:
                    msg = 'Multiple links for port %s on link %s' % (port, response.url)
                    self.logger.error(msg)
                    raise Exception(msg)

    def parse(self, response):
        csv_file = response.body
        f = StringIO.StringIO(csv_file)
        reader = csv.reader(f, delimiter=',')
        headers = next(reader)
        included_codes = PORT_CODES[response.meta['port']]
        for row in reader:
            formatted_row = dict(list(zip(headers, row)))
            port_code = int(formatted_row['Custom'])
            if '271111' in formatted_row['HS'] and port_code in included_codes:
                item = Customs()
                item['url'] = response.url
                item['commodity'] = 'lng'
                item['type'] = 'Import' if formatted_row['Exp or Imp'] == '2' else 'Export'
                for m in response.meta['months']:
                    month = MONTHS[m - 1]
                    price = int(formatted_row['Value-' + month])
                    weight = int(formatted_row['Quantity2-' + month])
                    if price and weight:
                        item['raw_price'] = price
                        item['raw_price_currency'] = '1000YEN'
                        item['raw_weight'] = weight
                        item['raw_weight_units'] = 'tons'

                        item['year'] = int(formatted_row['Year'])
                        item['month'] = m

                        item['country_code'] = int(formatted_row['Country'])
                        item['port_code'] = port_code

                        yield item
