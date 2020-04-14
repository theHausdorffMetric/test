# -*- coding: utf-8 -*-
"""Kaohsiung Port Authority.

!! NOTICE !!

Unfortunately, sources with only vessel names yield too many wrong signals on
oil (bad matching), hence the spider does more bad than good.

"""

from __future__ import absolute_import

from scrapy import Spider
from scrapy.selector import Selector

from kp_scrapers.models.items import EtaEvent, EtdEvent, VesselIdentification
from kp_scrapers.spiders.bases.markers import DeprecatedMixin
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


ETA_FORECAST_FIELDS = {'eta': 2}
ETA_ARRIVAL_FIELDS = {'eta': 3}
ETD_FIELDS = {'etd': 3}


class KaohsiungSpider(DeprecatedMixin, PortAuthoritySpider, Spider):
    name = 'Kaohsiung'
    start_urls = [
        'http://163.29.117.240/khbweb2/oh/pubweb/ohr118_eng.jsp',
        'http://163.29.117.240/khbweb2/oh/pubweb/ohr134_3.jsp',
        'http://163.29.117.240/khbweb2/oh/pubweb/ohr134_4.jsp',
    ]

    def _field(self, line, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join(line.css(q).extract()).strip()

    def parse(self, response):
        selector = Selector(response)
        title = ''.join(selector.css('#HeaderText *::text').extract()).strip().lower()
        if 'forecast' in title:  # ETA
            fields = ETA_FORECAST_FIELDS
            item = EtaEvent()
        elif 'arrival' in title:  # ETA
            fields = ETA_ARRIVAL_FIELDS
            item = EtaEvent()
        elif 'departure' in title:  # ETD
            fields = ETD_FIELDS
            item = EtdEvent()
        else:
            raise RuntimeError('Unknown type of page : {}'.format(title))

        for i, line in enumerate(selector.css('table#ItemsGrid tr')):
            # Skip the header
            if i < 1:
                continue
            item['url'] = response.url

            vessel_name = self._field(line, 1)
            vessel_type = None
            if any(p in title.lower() for p in ['arrival', 'departure']):
                vessel_type = self._field(line, 2)
            vessel_base = VesselIdentification(name=vessel_name, type=vessel_type)
            item['vessel'] = vessel_base

            for name, position in fields.items():

                value = self._field(line, position)
                if value:
                    item[name] = value

            yield item
