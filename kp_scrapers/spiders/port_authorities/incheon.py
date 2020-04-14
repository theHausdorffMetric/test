# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta
import re

from scrapy import Spider
from scrapy.http import FormRequest
from scrapy.selector import Selector
import six

from kp_scrapers.models.items import EtaEvent, EtdEvent, VesselIdentification
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


# Ports and their ids used in the request's formdata
PORT_NAME_ID = {
    'Incheon': '030',
    'Pyeong Taek': '031',
    'Daesan': '300',
    'Samcheok': '201',
    'Boryeong': '301',
}


class IncheonSpider(PortAuthoritySpider, Spider):
    """Incheon port source: (Contains data for 5 ports)
        - Incheon port (LNG, LPG, Oil)
        - Pyong Taek port (LNG, LPG)
        - Daesan port (LPG, Oil)
        - Samchok (LNG)
        - Boryeong (LNG)

    """

    name = 'Incheon'
    start_urls = ['http://www.portincheon.go.kr/eng_portmis/entry_schedule.asp']

    def start_requests(self):
        today = datetime.today()
        for port_name, port_id in six.iteritems(PORT_NAME_ID):
            formdata = {
                'PRTC': '{}'.format(port_id),
                'DATE_FR': today.strftime('%Y%m%d'),
                'DATE_TO': (today + timedelta(60)).strftime('%Y%m%d'),
            }
            yield FormRequest(
                url=self.start_urls[0], formdata=formdata, meta={'port_name': port_name}
            )

    def parse(self, response):
        selector = Selector(response)
        port_name = response.meta['port_name']
        for line in selector.css('table tr'):
            if len(line.css('td')) == 0:
                continue

            # Extract the call sign and vessel name
            call_sign, vessel_name = re.match('(.*)\[(.*)\]', self._field(line, 4)).groups()

            berth = self._field(line, 3)
            eta = self._field(line, 5)
            etd = self._field(line, 6)

            # Extract the origin and destination
            field = line.css('td:nth-child(8) *::text').extract()
            # not used: from_port_name = None
            departure_destination = None
            if len(field) == 2:
                # not used: from_port_name = field[0]
                departure_destination = field[1]

            vessel_base = VesselIdentification(name=vessel_name, call_sign=call_sign)

            # Actual port
            yield EtaEvent(
                vessel=vessel_base, eta=eta, berth=berth, port_name=port_name, url=response.url
            )
            if etd:
                yield EtdEvent(
                    vessel=vessel_base,
                    etd=etd,
                    berth=berth,
                    next_zone=departure_destination,
                    port_name=port_name,
                    url=response.url,
                )
            # Disabling Foreign ports items because it messes vessels ETA data on LNG
            # Trello card: https://trello.com/c/Ub1hlQFs/1241-lng-port-scrapping-issue-incheon
            # Foreign port
            # yield DepartedEvent(vessel=vessel_base,
            #                     previous_zone=from_port_name,
            #                     url=response.url)
            # if from_port_name and departure_destination:
            #     # Foreign port
            #     yield EtaEvent(vessel=vessel_base,
            #                    port_name=departure_destination,
            #                    url=response.url)

    def _field(self, line, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join(line.css(q).extract()).strip()
