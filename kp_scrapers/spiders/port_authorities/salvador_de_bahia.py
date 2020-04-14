# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider
from scrapy.selector import Selector

from kp_scrapers.models.items import VesselPortCall
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


COMING_FIELDS = {
    'vessel_name': 1,
    'local_port_call_id': 3,
    'eta': 4,
    'shipping_agent': 5,
    'vessel_flag': 6,
    'berth': 7,
    'etd': 9,
    'cargo_type': 11,
}

PORT_FIELDS = {
    'vessel_name': 1,
    'local_port_call_id': 3,
    'eta': 4,
    'berthing_time': 5,
    'shipping_agent': 6,
    'vessel_flag': 7,
    'berth': 8,
    'etd': 10,
    'cargo_type': 12,
}


class SalvadorDeBahiaSpider(PortAuthoritySpider, Spider):
    name = 'SalvadorDeBahia'
    start_urls = ['http://salvador.codeba.com.br/openport/pesquisa.aspx?WCI=RELWAPP451']

    def _field(self, line, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join(line.css(q).extract()).strip()

    def parse(self, response):
        selector = Selector(response)

        for i, table in enumerate(selector.css('form table')):
            if i == 0:
                fields = COMING_FIELDS
            elif i == 1:
                fields = PORT_FIELDS
            else:
                break

            for j, line in enumerate(table.css('tr')):
                # Skip header
                if j < 2:
                    continue

                # Only extract the LNG ships
                # if 'GAS LIQUEFEITO' not in self._field(line, 6).upper():
                # continue

                item = VesselPortCall()
                item['url'] = response.url

                for name, position in fields.items():
                    value = self._field(line, position)
                    if value:
                        item[name] = value

                # TODO only keep LNG ships
                # if item.get('cargo_type') != 'LNG':
                # continue

                yield item
