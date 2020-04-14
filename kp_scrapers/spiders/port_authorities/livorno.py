# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider
from scrapy.selector import Selector
from six.moves import range

from kp_scrapers.models.items import ArrivedEvent, EtdEvent, VesselIdentification
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class LivornoSpider(PortAuthoritySpider, Spider):

    name = 'Livorno'
    start_urls = ['https://tpcs.tpcs.eu/login.aspx#']

    def parse(self, response):
        selector = Selector(response)

        table = selector.xpath('//div[starts-with(@class, "ez-box tabellaNaviRow")]')

        #  Arrivals
        for (i, row) in enumerate(table):
            extracted_row = self._extract_row(row)
            if len(extracted_row) == 30:
                table = table[i:]
                break
            imo = extracted_row[5]
            vessel_name = extracted_row[10]
            # I'm not sure about what this value corresponds to
            # not used: voyage = extracted_row[14]
            arrival = extracted_row[18]

            vessel_base = VesselIdentification(imo=imo, name=vessel_name)
            yield ArrivedEvent(vessel=vessel_base, arrival=arrival)

        #  Departures
        for row in table:
            extracted_row = self._extract_row(row)
            imo = extracted_row[5]
            vessel_name = extracted_row[10]
            # I'm not sure about what this value corresponds to
            # not used: voyage = extracted_row[14]
            departure = extracted_row[18]
            # not used: C_T = extracted_row[22]
            hour = extracted_row[26]

            vessel_base = VesselIdentification(imo=imo, name=vessel_name, dwt=hour)
            yield EtdEvent(vessel=vessel_base, etd=departure)

    def _extract_row(self, line):
        extracted_dict = line.css('*::text').extract()
        for i in range(len(extracted_dict)):
            extracted_dict[i] = extracted_dict[i].replace('\r\n', '').strip()

        return extracted_dict
