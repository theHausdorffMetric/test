# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider
from scrapy.http import Request
from scrapy.selector import Selector
import six

from kp_scrapers.lib.utils import extract_row
from kp_scrapers.models.items import (
    BerthedEvent,
    DepartedEvent,
    EtaEvent,
    EtdEvent,
    VesselIdentification,
)
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class Ternflushing(PortAuthoritySpider, Spider):
    """
    In this spider, we are scraping all the vessels and filtering on
    the PORTS and the Terminals we want
    """

    name = 'Terneuzen_Flushing'
    start_urls = ['http://www.vopakagencies.com/apps/arrivals.nsf/Arrivals?ReadForm&Office=7']

    def parse(self, response):
        sel = Selector(response)
        rows = sel.xpath('.//*[@id="content_page"]//tr//tr')
        for row in rows[1:]:
            line = extract_row(row)
            if len(line) != 8:
                self.logger.error('wrong row format **{}**'.format(row))
                continue
            vessel_link = row.css('a::attr(href)').extract()[0]

            yield Request(
                url='http://www.vopakagencies.com' + vessel_link,
                meta={'line': line},
                callback=self.parse_terminal,
            )

    def parse_terminal(self, response):
        sel = Selector(response)
        item_row = response.meta['line']
        page_rows = sel.xpath('.//*[@id="content_page"]//tr//tr')
        rows_data_dict = {
            y[0]: y[1] for y in (extract_row(x)[:2] for x in page_rows) if len(y) == 2
        }
        port = rows_data_dict['Port']
        terminal = rows_data_dict['Terminal']
        vessel_name = item_row[0]
        ETA = item_row[2]
        arrival_date = item_row[3]
        ETD = item_row[4]
        departure_date = item_row[5]
        vessel_status = item_row[6]
        movement_map = {
            ('Expected', ETA): [EtaEvent(eta=ETA), EtdEvent(etd=ETD)],
            ('In Port', ETA): [EtaEvent(eta=ETA)],
            ('In Port', arrival_date): [BerthedEvent(berthed=arrival_date)],
            ('left', departure_date): [DepartedEvent(departure=departure_date)],
        }

        for mov, evs in six.iteritems(movement_map):
            if vessel_status == mov[0] and mov[1] is not None:
                for ev in evs:
                    vessel = VesselIdentification()
                    vessel['name'] = vessel_name
                    ev['vessel'] = dict(vessel)
                    ev['url'] = self.start_urls[0]
                    ev['port_name'] = port
                    ev['berth'] = terminal
                    if any(p in port.lower() for p in ['terneuzen', 'flushing']) and any(
                        t in terminal.lower() for t in ['vlissingen', 'dow oceandock']
                    ):
                        yield ev
