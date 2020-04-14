# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta

from scrapy.selector import Selector

from kp_scrapers.models.items import (
    ArrivedEvent,
    BerthedEvent,
    Cargo,
    EtaEvent,
    VesselIdentification,
)
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class CostaAzulSpider(PortAuthoritySpider, PdfSpider):
    name = 'CostaAzul'

    url_base = 'http://www.puertoensenada.com.mx'
    start_urls = ('http://www.puertoensenada.com.mx/engs/0000196/schedules-ships',)
    rules = []
    commodity_matches = ('Container', 'Gas', 'Bulk')

    @staticmethod
    def _fields(line, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join([field for field in line.css(q).extract()]).strip()

    @staticmethod
    def _is_past_date(date):
        """
            Return true if date is past, false is future
            Date is UTC-8 (Mexico) compared to UTC now
            None if date is invalid
        """
        try:
            date_to_test = datetime.strptime(date, '%d/%m/%Y %H:%M') - timedelta(hours=8)
        except ValueError:
            return None
        return date_to_test < datetime.utcnow()

    @staticmethod
    def _parse_details(item, line):
        item['url'] = CostaAzulSpider.start_urls[0]
        item['vessel'] = VesselIdentification()
        item['vessel']['name'] = CostaAzulSpider._fields(line, 7)
        item['pa_voyage_id'] = CostaAzulSpider._fields(line, 9)
        item['cargo'] = Cargo()
        if 'Gas' in CostaAzulSpider._fields(line, 8):
            item['cargo']['commodity'] = 'lng'
        item['cargo']['origin'] = CostaAzulSpider._fields(line, 12)
        item['cargo']['destination'] = CostaAzulSpider._fields(line, 13)
        item['berth'] = CostaAzulSpider._fields(line, 19)
        return item

    def parse(self, response):
        selector = Selector(response)
        for i, line in enumerate(selector.css('table.tablaConsulta tbody tr')):
            if not any(word in self._fields(line, 8) for word in self.commodity_matches):
                continue
            arrival_date = self._fields(line, 3)
            is_past_date = self._is_past_date(arrival_date)
            if is_past_date is True:
                item = ArrivedEvent()
                item['arrival'] = arrival_date
            elif is_past_date is False:
                item = EtaEvent()
                item['eta'] = arrival_date
            else:
                continue
            yield self._parse_details(item, line)
            if self._fields(line, 17):
                item = BerthedEvent()
                item['berthed'] = self._fields(line, 17)
                yield self._parse_details(item, line)
