# -*- coding: utf-8 -*-

"""Spider for Hastings, a port in South Australia.

It is updated twice per day, at 0900, and 1600.  ----> I think it is updated weekly,
this information is given on the sheet giving the shipping list, but this daily update
may be true when calling the data provider.

"""

from __future__ import absolute_import, unicode_literals
from datetime import datetime
import re

from scrapy import Spider

from kp_scrapers.models.items import EtaEvent, InPortEvent, VesselIdentification
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class HastingsSpider(PortAuthoritySpider, Spider):

    name = 'Hastings'

    version = '1.0.0'
    # NOTE didn't find anything in the DB
    provider = 'Hastings'

    start_urls = ['http://www.portofhastings.com/images/ShipsInPortCurrent.pdf']

    def parse_row(self, row, event):
        vessel_base = VesselIdentification()
        # split row (which is a string) on every group of 2 spaces
        data = re.split(r'\s{2,}', row)
        if len(data) > 1:
            vessel_base['name'] = data[0]
            vessel_base['imo'] = data[1]
        # data[2] : type of cargo, i can check for LPG here
        # Condition that matches the data on in port events
        is_in_port_event = len(data) > 8 and 'IN PORT' == data[6]
        # Condition that matches the data on in eta events
        is_eta_event = len(data) > 9 and not is_in_port_event
        if is_in_port_event:
            event['port_area'] = data[7]
            event['move_date'] = data[8]
            yield event
        elif is_eta_event:
            event['eta'] = data[6]
            # NOTE we don't yield foreign ETA due to a bug on the ETL
            # event['next_zone'] = data[9]
            yield event
        # TODO: add management of event etd

    def parse(self, response):
        t = datetime.now()
        body = response.body
        table = PdfSpider.pdf_to_text(body).split('\n')
        rows = table[5:10]

        # Ajouter event EtaEvent
        # ou alors EtdEvent
        for row in rows:
            data = re.split(r'\s{2,}', row)
            # Condition that matches the data on in port events
            if len(data) > 6 and 'IN PORT' == data[6]:
                event = InPortEvent(url=response.url, updated_time=t, sh_spider_name=self.name)
            else:
                event = EtaEvent(url=response.url, updated_time=t, sh_spider_name=self.name)
            for e in self.parse_row(row, event):
                yield e
