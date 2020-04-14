# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import date, timedelta
import re

from scrapy import Spider
from scrapy.http import FormRequest, Request
from scrapy.selector import Selector

from kp_scrapers.lib import static_data
from kp_scrapers.models.items import ArrivedEvent, Cargo, EtaEvent, VesselIdentification
from kp_scrapers.spiders.bases.markers import LngMarker, LpgMarker, OilMarker
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


COMING_FIELDS = {'port_name': 1, 'eta': 2, 'vessel_name': 4, 'shipping_agent': 5, 'cargo_type': 6}

IN_PORT_FIELDS = {
    'berth': 2,
    'vessel_name': 6,
    'arrival_date': 15,
    'cargo_type': 16,
    'cargo_ton': 17,
}

COMMO_HINTS = {'GAS LIQUEFEITO': ['lng'], 'TANKER': ['lng', 'lpg', 'oil'], 'PETROLEIRO': ['oil']}

FIELDS = {
    'http://www.portosrio.gov.br/prognav/navios_esperados': COMING_FIELDS,
    'http://www.portosrio.gov.br/prognav': IN_PORT_FIELDS,
}

EVENTS = {
    'http://www.portosrio.gov.br/prognav/navios_esperados': EtaEvent,
    'http://www.portosrio.gov.br/prognav': ArrivedEvent,
}


def yesterday():
    return (date.today() - timedelta(1)).strftime('%d/%m/%Y')


class RioSpider(LngMarker, LpgMarker, OilMarker, PortAuthoritySpider, Spider):

    name = 'Rio'
    version = '1.0.1'
    provider = 'Rio'

    start_urls = [
        'http://www.portosrio.gov.br/prognav/navios_esperados',
        'http://www.portosrio.gov.br/prognav',
    ]

    def start_requests(self):
        vessels = [
            {'name': vessel['name'].lower(), 'commos': vessel['_markets']}
            for vessel in static_data.vessels()
            if 'name' in vessel
        ]
        yield Request(self.start_urls[0], meta={'vessels': vessels})

        formdata = {'data': yesterday(), 'submit': 'Enviar'}
        yield FormRequest(
            url=self.start_urls[1],
            formdata=formdata,
            callback=self.parse_in_port,
            meta={'vessels': vessels},
        )

    def _field(self, line, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join(line.css(q).extract()).strip()

    def parse(self, response):
        selector = Selector(response)
        # Updated time
        date_string = ''.join(selector.css('#main::text').extract())
        m = re.match(
            '\D* ([0-9]+\D[0-9]+\D[0-9]+) \D* ([0-9]+\D[0-9]+) \D*', date_string, re.VERBOSE
        )
        if m:
            updated_time = ' '.join(m.groups())
        else:
            updated_time = None

        for i, line in enumerate(selector.css('#main table tbody tr')):
            vessel_category = self._field(line, 6).upper()
            if vessel_category in COMMO_HINTS:
                for event in self.extract_events(response, line, updated_time=updated_time):
                    yield event

    def parse_in_port(self, response):
        selector = Selector(response)
        for i, line in enumerate(selector.css('#main table tbody tr')):
            for event in self.extract_events(response, line):
                yield event

    def extract_events(self, response, line, updated_time=None):
        vessels = response.meta['vessels']
        item = {'url': response.url, 'updated_time': updated_time or yesterday()}

        for name, position in FIELDS[response.url].items():
            value = self._field(line, position)
            if value:
                item[name] = value

        cargo_type = item.pop('cargo_type')
        matching_vessels = [
            vessel
            for vessel in vessels
            if item.get('vessel_name', 'no name').lower() == vessel['name'].lower()
        ]

        if matching_vessels:
            commos = [commo for vessel in matching_vessels for commo in vessel['commos']]
        else:
            commos = [commo for commo in COMMO_HINTS.get(cargo_type, [])]
        compatible_commos = [commo for commo in commos if commo in COMMO_HINTS.get(cargo_type, [])]
        if not compatible_commos:
            compatible_commos = COMMO_HINTS.get(cargo_type, [])
        item['vessel'] = VesselIdentification(name=item.pop('vessel_name'))

        for commo in compatible_commos:
            item['cargo'] = Cargo(commodity=commo)
            if matching_vessels:
                yield EVENTS[response.url](**item)
            else:
                self.logger.warning('no corresponding vessel found for item: {}'.format(item))
                yield EVENTS[response.url](**item)
