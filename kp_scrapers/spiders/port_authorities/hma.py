# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider
from scrapy.http import Request
from scrapy.selector import Selector

from kp_scrapers.models.items import VesselPortCall
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class HMASpider(PortAuthoritySpider, Spider):
    name = 'hma'
    start_urls = ['http://vts.mhpa.co.uk/Default.aspx?id=14']

    def parse(self, response):
        sel = Selector(response)

        header = [
            ''.join(field.extract()).lower()
            for field in sel.css('.igg_HeaderCaption').xpath('text()')
        ]

        array = []
        for row in sel.css('.ig_Item').xpath('tr'):
            array.append([''.join(td.xpath('./text()').extract()) for td in row.xpath('./td')])

        departure = [
            row
            for row in array
            if u'Sailing' in row[header.index('move type')]
            and (
                u'South Hook' in row[header.index('from')] or u'Dragon' in row[header.index('from')]
            )
        ]

        arrival = [
            row
            for row in array
            if u'Arrival' in row[header.index('move type')]
            and (u'South Hook' in row[header.index('to')] or u'Dragon' in row[header.index('to')])
        ]

        for vessel in arrival:
            item = VesselPortCall()
            item['eta'] = vessel[header.index('date')]
            item['vessel_name'] = vessel[header.index('ship')]
            item['gross_tonnage'] = vessel[header.index('gt')]
            item['berth'] = vessel[header.index('to')]
            item['from_port_name'] = vessel[header.index('from')]
            item['dead_weight'] = vessel[header.index('dwt')]
            item['url'] = response.url
            yield item

        for vessel in departure:
            item = VesselPortCall()
            item['etd'] = vessel[header.index('date')]
            item['vessel_name'] = vessel[header.index('ship')]
            item['gross_tonnage'] = vessel[header.index('gt')]
            item['berth'] = vessel[header.index('from')]
            item['departure_destination'] = vessel[header.index('to')]
            item['dead_weight'] = vessel[header.index('dwt')]
            item['url'] = response.url
            yield item
            if item['departure_destination']:
                next_item = VesselPortCall()
                next_item['foreign'] = True
                next_item['missing_eta'] = True
                next_item['origin_etd'] = item.get('etd')
                next_item['port_name'] = item.get('departure_destination')
                for field in ['vessel_name', 'gross_tonnage', 'url', 'dead_weight']:
                    next_item[field] = item[field]
                yield next_item

        yield Request('http://vts.mhpa.co.uk/Default.aspx?id=11', callback=self.parse_past)

    def parse_past(self, response):
        sel = Selector(response)

        header = [
            ''.join(field.extract()).lower()
            for field in sel.css('.igg_HeaderCaption').xpath('text()')
        ]

        array = []
        for row in sel.css('.ig_Item').xpath('tr'):
            array.append([''.join(td.xpath('./text()').extract()) for td in row.xpath('./td')])

        departure = [
            row
            for row in array
            if u'Sailing' in row[header.index('movement type')]
            and (
                u'South Hook' in row[header.index('from')] or u'Dragon' in row[header.index('from')]
            )
        ]

        arrival = [
            row
            for row in array
            if u'Arrival' in row[header.index('movement type')]
            and (u'South Hook' in row[header.index('to')] or u'Dragon' in row[header.index('to')])
        ]

        for vessel in arrival:
            item = VesselPortCall()
            item['vessel_name'] = vessel[header.index('ship')]
            item['cargo_type'] = vessel[header.index('ship type')]
            item['gross_tonnage'] = vessel[header.index('gt')]
            item['arrival_date'] = vessel[header.index('finished')]
            item['berth'] = vessel[header.index('to')]
            item['from_port_name'] = vessel[header.index('from')]
            item['shipping_agent'] = vessel[header.index('agent')]
            item['url'] = response.url
            yield item

        for vessel in departure:
            item = VesselPortCall()
            item['vessel_name'] = vessel[header.index('ship')]
            item['cargo_type'] = vessel[header.index('ship type')]
            item['gross_tonnage'] = vessel[header.index('gt')]
            item['departure_date'] = vessel[header.index('finished')]
            item['berth'] = vessel[header.index('from')]
            item['departure_destination'] = vessel[header.index('to')]
            item['shipping_agent'] = vessel[header.index('agent')]
            item['url'] = response.url
            yield item
            if item['departure_destination']:
                next_item = VesselPortCall()
                next_item['foreign'] = True
                next_item['missing_eta'] = True
                next_item['origin_etd'] = item.get('departure_date')
                next_item['port_name'] = item.get('departure_destination')
                for field in [
                    'vessel_name',
                    'cargo_type',
                    'gross_tonnage',
                    'shipping_agent',
                    'url',
                ]:
                    next_item[field] = item[field]
                yield next_item
