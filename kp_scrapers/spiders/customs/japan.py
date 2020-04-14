# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
import copy
import csv
from datetime import date
from io import StringIO
from itertools import groupby

from scrapy.http import Request
from scrapy.selector import Selector
import six
from six.moves import zip

from kp_scrapers.lib.date import rewind_time
from kp_scrapers.models.items import Customs
from kp_scrapers.spiders.bases.markers import LngMarker, LpgMarker, OilMarker
from kp_scrapers.spiders.customs.base import CustomsBaseSpider


MONTHS = ['Jan', 'Feb', 'Mar', 'Apl', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


class JapanCustomsSpider(LngMarker, LpgMarker, OilMarker, CustomsBaseSpider):

    name = 'JapanCustoms'
    version = '1.0.0'
    provider = 'JapanCustoms'

    def commodity_mapping(self):
        return {
            'lng': {'271111': None},
            'lpg': {'271112': 'propane', '271113': 'butane'},
            'oil': {
                '271019162': None,
                '271019164': None,
                '271019166': None,
                '271019169': None,
                '271019172': None,
                '271019174': None,
                '271019179': None,
            },
        }

    def start_requests(self):
        url = 'http://www.e-stat.go.jp/SG1/estat/OtherListE.do?bid=000001008801&cycode=1'
        self.logger.info('Rewinding %s months before current month' % self.months_look_back)
        yield Request(url=url, callback=self.get_yearly_pages)

    def get_yearly_pages(self, response):
        past_dates = rewind_time(current_date=date.today(), months=self.months_look_back)
        years = set([past_date.year for past_date in past_dates])
        for year in years:
            months = [past_date.month for past_date in past_dates if past_date.year == year]
            sel = Selector(response)
            # They serve the same file for every month in the table
            link = sel.xpath(
                '//tr[contains(., "{}")]//a[text()="Jan."]/@href'.format(str(year))
            ).extract_first()
            # Skip if year is not yet displayed
            if link is None:
                continue
            url = 'http://www.e-stat.go.jp/SG1/estat/' + link[1:]
            yield Request(url=url, callback=lambda r: self.list_of_tables(r, months))

    def list_of_tables(self, response, months):
        sel = Selector(response)
        link = sel.xpath('//tr[td[contains(text(), "Section V ")]]//a/@href').extract()[0]
        url = 'http://www.e-stat.go.jp/SG1/estat/' + link[1:]
        yield Request(url=url, meta={'months': months})

    def parse(self, response):
        def merge(reader, headers, sorting_func=lambda x: (x['Country'], products[x['HS']])):
            rows = []
            for row in reader:
                product_code = self.get_product(row[2])
                if product_code:
                    row[2] = product_code
                    rows.append(dict(list(zip(headers, row))))
            final_rows_per_commodity = dict()
            for commodity_name, products in six.iteritems(self.relevant_commodities()):
                _rows = [row for row in rows if row['HS'] in products]
                grouped_rows = groupby(sorted(_rows, key=sorting_func), key=sorting_func)

                final_rows = dict()
                for identifier, group in grouped_rows:
                    g = list(group)
                    new = copy.deepcopy(g[0])
                    for key, val in new.items():
                        if 'Value-' in key or 'Quantity2-' in key:
                            new[key] = int(new[key] or 0)
                    for line in g[1:]:
                        for key, val in line.items():
                            if 'Value-' in key or 'Quantity2-' in key:
                                new[key] += int(val or 0)
                    final_rows[identifier] = new
                final_rows_per_commodity[commodity_name] = final_rows

            return final_rows_per_commodity

        csv_file = response.body
        f = StringIO.StringIO(csv_file)
        reader = csv.reader(f, delimiter=',')
        headers = next(reader)
        rows = merge(reader, headers)
        for commodity, products in six.iteritems(rows):
            for (_, subcommo), row in six.iteritems(products):
                for m in response.meta['months']:
                    month = MONTHS[m - 1]
                    item = Customs()
                    item['url'] = response.url
                    item['commodity'] = subcommo or commodity
                    item['type'] = 'Import' if row['Exp or Imp'] == '2' else 'Export'

                    item['raw_price'] = int(row['Value-' + month])
                    item['raw_price_currency'] = '1000YEN'
                    item['raw_weight'] = int(row['Quantity2-' + month])
                    item['raw_weight_units'] = 'tons'

                    item['year'] = int(row['Year'])
                    item['month'] = m

                    item['country_code'] = int(row['Country'])
                    if item['raw_price'] or item['raw_weight']:
                        yield item
