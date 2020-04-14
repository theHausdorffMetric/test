# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import date, timedelta

from scrapy.http import FormRequest, Request
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider
import six

from kp_scrapers.models.items import (
    ArrivedEvent,
    BerthedEvent,
    Cargo,
    DepartedEvent,
    VesselIdentification,
)
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


CARGO_TYPE = (u'LNG', u'LPG', u'OIL', u'PETROLEUM')

PORTS_ID_NAME = {
    u'Jeju': u'900',
    u'Seoguipo': u'901',
    u'Mokpo': u'610',
    u'Wando': u'611',
    u'Yeosu': u'620',
    u'Gwangyang': u'622',
}


class GwangyangSpider(PortAuthoritySpider, CrawlSpider):

    name = 'Gwangyang'
    allowed_domains = ['yeosu.mof.go.kr']
    start_urls = ('http://yeosu.mof.go.kr/eng_portmis/operating/eng_npcsdt_qr.jsp',)
    rules = []

    def start_requests(self):
        for url in self.start_urls:
            for crt_port_name, crt_port_id in six.iteritems(PORTS_ID_NAME):
                today = date.today()
                formdata = {
                    'FLAG': 'VSD01',
                    'PRTC': crt_port_id,
                    'DATE_FR': (today - timedelta(5)).strftime('%Y%m%d'),
                    'DATE_TO': (today + timedelta(30)).strftime('%Y%m%d'),
                    # 'FACC': 'MB801',
                }
                yield FormRequest(url=url, formdata=formdata, meta={'port_name': crt_port_name})

    def _field(self, line, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join(line.css(q).extract()).strip()

    def _inBrackets(self, s):
        return s[s.find('[') + 1 : s.rfind(']')]

    def parse(self, response):
        """
        Parse the search results.
        """
        selector = Selector(response)

        # key: string in html doc
        # value: (Type of event to yield, Field for date setting)
        event_type = {
            "departure": (DepartedEvent, "departure"),
            "up berth": (DepartedEvent, "departure"),
            "up anchor": (DepartedEvent, "departure"),
            "arrival": (ArrivedEvent, "arrival"),
            "berthed": (BerthedEvent, "berthed"),
            "anchored": (ArrivedEvent, "arrival"),
        }

        # It's really sad to select the tr by the background color
        # but it's by best I have
        for line in selector.css('tr[bgcolor="ffffff"]'):
            call_sign = self._field(line, 2)
            action = self._field(line, 7)
            action = self._inBrackets(action).lower()
            if not call_sign or action not in event_type:
                continue
            # Indication for zone_id in gwangyang_pa.py
            item = event_type[action][0]()
            item['port_name'] = response.meta['port_name']
            item['vessel'] = VesselIdentification()
            item['vessel']['call_sign'] = call_sign[: call_sign.find('[')]
            item['vessel']['gt'] = self._field(line, 3).replace(',', '.')
            item['berth'] = self._inBrackets(self._field(line, 8))
            item['shipping_agent'] = self._field(line, 5)
            cargo_type = self._field(line, 4)
            cargo_type = set(CARGO_TYPE).intersection(cargo_type.split(' '))
            item[event_type[action][1]] = self._field(line, 1)
            if cargo_type:
                item['cargo'] = Cargo()
                item['cargo']['commodity'] = cargo_type.pop().lower()
                # cargo_status: oil or other if petroleum
                if item['cargo']['commodity'] == 'oil':
                    item['cargo']['cargo_status'] = 'oil'
                if item['cargo']['commodity'] == 'petroleum':
                    item['cargo']['commodity'] = 'oil'
                    item['cargo']['cargo_status'] = 'other'
            yield Request(
                'http://yeosu.mof.go.kr/eng_portmis/operating/'
                'eng_cvlslt_detail_r.jsp?CALT=' + item['vessel']['call_sign'],
                callback=self.parseDetailsVessel,
                meta={'item': item},
            )

    def _details_field(self, table, i, j):
        q = 'tr:nth-child({}) td:nth-child({}) *::text'.format(i, j)
        return ''.join(table.css(q).extract()).strip()

    def parseDetailsVessel(self, response):
        """
        Parse the details vessel page
        """
        selector = Selector(response)
        table = selector.css('table')
        item = response.meta['item']
        # mmsi = self._details_field(table, 2, 4)
        name = self._details_field(table, 2, 8)
        length = self._details_field(table, 4, 4)
        nt = self._details_field(table, 5, 6)
        flag = self._details_field(table, 2, 5)
        # if mmsi:
        #     item['vessel']['mmsi'] = mmsi
        if name:
            item['vessel']['name'] = name
        if length:
            item['vessel']['length'] = length.replace(',', '.')
        if nt:
            item['vessel']['nt'] = nt.replace(',', '.')
        if flag:
            item['vessel']['flag'] = flag
        yield item
