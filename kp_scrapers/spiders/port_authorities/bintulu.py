# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider
from scrapy.selector import Selector
import six

from kp_scrapers.lib.utils import extract_row
from kp_scrapers.models.items import BerthedEvent, Cargo, EtaEvent, EtdEvent, VesselIdentification
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class BintuluSpider(PortAuthoritySpider, Spider):
    name = 'Bintulu'
    start_urls = ('http://eport.e-maritime.com/em/servlet/SvtVesselScheduleDisplay?port=MYBTU',)

    def parse(self, response):
        sel = Selector(response)
        # TODO: detect tables using titles
        # Its been a long time the site showing only At Berth table
        tables = {'Vessel At Berth': '2', 'Vessel Expected To Arrive': '3'}
        for t in six.iteritems(tables):
            xpath_ = '/html/body/table/tr[1]/td/table/tr/td[2]/table/tr/td/table[' + t[1] + ']//tr'
            table = sel.xpath(xpath_)[3:]
            for row in table:
                data = extract_row(row)
                # Port name 'Bintulu' verification avoids the vessels from other ports
                if 'BINTULU' in data[3]:
                    vessel_base = VesselIdentification(name=data[2])
                    terminal = data[1]
                    port_name = data[3]
                    shipping_agent = data[4]
                    etd = data[6]

                    cargoes = []
                    if 'LNG' in terminal:
                        cargoes.append(Cargo(commodity='lng'))
                    elif 'Single Buoy Mooring' in terminal:
                        cargoes.append(Cargo(commodity='oil'))

                    if not cargoes:
                        cargoes = None

                    if t[0] == 'Vessel At Berth':
                        yield BerthedEvent(
                            berthed=data[5],
                            vessel=vessel_base,
                            berth=terminal,
                            port_name=port_name,
                            shipping_agent=shipping_agent,
                            cargoes=cargoes,
                            url=response.url,
                        )
                        if etd:
                            yield EtdEvent(
                                etd=etd,
                                vessel=vessel_base,
                                berth=terminal,
                                port_name=port_name,
                                shipping_agent=shipping_agent,
                                cargoes=cargoes,
                                url=response.url,
                            )

                    elif t[0] == 'Vessel Expected To Arrive':
                        yield EtaEvent(
                            eta=data[5],
                            vessel=vessel_base,
                            berth=terminal,
                            port_name=port_name,
                            shipping_agent=shipping_agent,
                            cargoes=cargoes,
                            url=response.url,
                        )
                        if etd:
                            yield EtdEvent(
                                etd=etd,
                                vessel=vessel_base,
                                berth=terminal,
                                port_name=port_name,
                                shipping_agent=shipping_agent,
                                cargoes=cargoes,
                                url=response.url,
                            )

                    else:
                        raise Exception('Unknown port!')
