# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import re

from scrapy import Spider
from scrapy.selector import Selector

from kp_scrapers.models.items import ArrivedEvent, Cargo, EtaEvent, EtdEvent, VesselIdentification
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class DahejSpider(PortAuthoritySpider, Spider):

    name = 'Dahej'
    volume_unit = 'tons'
    port_names = ['Dahej', 'Porbandar']

    start_urls = ['http://porttoport.in/schedule/Gujarat/gujaratports.php']

    @staticmethod
    def _field(line, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join(line.css(q).extract()).strip()

    def parse(self, response):
        selector = Selector(response)
        url = response.url
        for i, tables in enumerate(selector.css('div')):
            # first port dahej
            if 'DAHEJ PORT' in tables.extract():

                next_table = selector.css('div:nth-child({})'.format(i + 3)).xpath(
                    'table[2]'
                )  # Only parse the Import table
                for i, row in enumerate(next_table.css('tr')):
                    if i == 0:  # skip header
                        continue
                    vessel_name = self._field(row, 4)
                    vessel_base = VesselIdentification(name=vessel_name)
                    eta = self._field(row, 2)
                    etd = self._field(row, 3)
                    cargo_base = self.treat_cargo(self._field(row, 6))
                    if any(x in eta.lower() for x in ['in port', 'stream']):
                        yield ArrivedEvent(
                            vessel=vessel_base,
                            port_name=self.port_names[0],
                            cargo=cargo_base,
                            url=url,
                        )
                    else:
                        yield EtaEvent(
                            vessel=vessel_base,
                            port_name=self.port_names[0],
                            eta=eta,
                            cargo=cargo_base,
                            url=url,
                        )
                    if etd != '-':
                        yield EtdEvent(
                            vessel=vessel_base,
                            port_name=self.port_names[0],
                            etd=etd,
                            cargo=cargo_base,
                            url=url,
                        )
            # second port porbandar
            elif 'PORBANDAR' in tables.extract():
                next_table = selector.css('div:nth-child({})'.format(i + 3)).xpath(
                    'table[2]'
                )  # Only parse the Import table
                for i, row in enumerate(next_table.css('tr')):
                    if i == 0:  # skip header
                        continue
                    vessel_name = self._field(row, 2)
                    vessel_base = VesselIdentification(name=vessel_name)
                    eta = self._field(row, 1)
                    etd = self._field(row, 6)
                    cargo_base = self.treat_cargo(self._field(row, 4))
                    if any(x in eta.lower() for x in ['in port', 'stream']):
                        yield ArrivedEvent(
                            vessel=vessel_base,
                            port_name=self.port_names[1],
                            cargo=cargo_base,
                            url=url,
                        )
                    else:
                        yield EtaEvent(
                            vessel=vessel_base,
                            port_name=self.port_names[1],
                            eta=eta,
                            cargo=cargo_base,
                            url=url,
                        )
                    if etd != '-':
                        yield EtdEvent(
                            vessel=vessel_base,
                            port_name=self.port_names[1],
                            etd=etd,
                            cargo=cargo_base,
                            url=url,
                        )

    def treat_cargo(self, cargo_field):
        cargo = Cargo()

        if 'lng' in cargo_field.lower():
            cargo['commodity'] = 'lng'

        m = re.match('[\w\s]+ - ([0-9]+)', cargo_field)
        if m is not None:
            cargo['volume'] = '-' + m.group(1)
            cargo['volume_unit'] = self.volume_unit

        return cargo
