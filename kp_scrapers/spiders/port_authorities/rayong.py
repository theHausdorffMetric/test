# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import re

from scrapy import Spider
from scrapy.selector import Selector

from kp_scrapers.models.items import VesselPortCall
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


COMING_FIELDS = {'eta': 2, 'vessel_name': 3, 'berthing_time': 7, 'etd': 9, 'shipping_agent': 11}

PORT_FIELDS = {
    'arrival_date': 2,
    'vessel_name': 3,
    'berthing_time': 7,
    'etd': 9,
    'shipping_agent': 11,
}


class RayongSpider(PortAuthoritySpider, Spider):

    name = 'Rayong'
    start_urls = ['http://www.tptport.com/mtp/CS/tentative.php']

    def _field(self, line, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join(line.css(q).extract()).strip()

    def parse(self, response):
        selector = Selector(response)

        # Updated time
        date_string = selector.css('table')[0].css('tr td *::text').extract()[-1]
        m = re.match('\D* ([0-9]+\D[0-9]+\D[0-9]+) \D* ([0-9]+) \D*', date_string, re.VERBOSE)
        if m:
            updated_time = ' '.join(m.groups())
        else:
            updated_time = None

        # The page starts with the PORT ships
        fields = PORT_FIELDS
        for i, line in enumerate(selector.css('table')[0].css('tr')):
            # Skip header
            if i < 1:
                continue

            if 'VESSEL TO COME' in ''.join(line.css('*::text').extract()):
                fields = COMING_FIELDS
                continue

            item = VesselPortCall()
            item['url'] = response.url
            if updated_time:
                item['updated_time'] = updated_time

            if 'navios_esperados' in response.url:
                fields = COMING_FIELDS

            for name, position in fields.items():
                value = self._field(line, position)
                if value:
                    item[name] = value

            vessel_field = item.get('vessel_name')
            if vessel_field:
                split_field = vessel_field.split(' ')
                item['vessel_name'] = ' '.join(split_field[:-1])
                item['berth'] = ' '.join(split_field[-1:])
                yield item
