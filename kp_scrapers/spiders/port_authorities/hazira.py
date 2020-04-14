# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import re

from scrapy.http import FormRequest
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider
from six.moves import zip

from kp_scrapers.lib import static_data
from kp_scrapers.models.items import VesselPortCall
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


AT_BERTH_HEADER = {
    'Berth': 'berth',
    'VCN No.': 'local_port_call_id',  # Vessel Call Number
    'Vessels Name': 'vessel_name',
    'Cargo': 'cargo_type',
    'Qty in MT': 'cargo_ton',  # METRIC TON
    # 'Type': '',  # I ?
    'Agent': 'shipping_agent',
    'ETD': 'etd',
}

ANCHORAGE_HEADER = {}

EXPECTED_HEADER = {
    'BERTH': 'berth',
    '\tVESSELS NAME': 'vessel_name',
    'VCN': 'local_port_call_id',
    'CARGO': 'cargo_type',
    'QTY MT': 'cargo_ton',
    # 'PCS': '',  # PCs of containers?
    # 'IEB': '',  #  I, E or I/E
    'VESSELS AGENT': 'shipping_agent',
    'ETA': 'eta',
}

SAILED_HEADER = {
    '\tVESSELS NAME': 'vessel_name',
    'VCN': 'local_port_call_id',
    # 'CARGO': 'cargo_type',  # Operation completion time (loading/unloading of cargo)
    # 'LAST LINE': '',  # Date before disembarkment
    'PILOT DISEMBARK': 'departure_date',
}

BERTH_HIST_HEADER = {
    'VCN': 'local_port_call_id',
    '\tVESSELS NAME': 'vessel_name',
    # u'POB': '',  # Personnel On Board
    # u'ATB': '',  # Time of Articulated Tug Barges (ATB) operation?
    # u'All Fast': '',  # same as LAST LINE? (the time the vessel departed the port limits)
    'PILOT DISEMBARK': 'departure_date',  # Final date of departure
    # u'ATA': ''  # Actual Time of Arrival
}


class HaziraSpider(PortAuthoritySpider, CrawlSpider):
    name = "Hazira"
    start_urls = [
        'http://www.adaniports.com/Hazira_port_operations_VesselsBearth.aspx',
        # 'http://www.adaniports.com/Hazira_port_operations_VesselsAnchorage.aspx',
        'http://www.adaniports.com/Hazira_port_operations_VesselsExpected.aspx',
        'http://www.adaniports.com/Hzira_port_operations_VesselsSailed.aspx',
        'http://www.adaniports.com/Hzira_port_operations_VesselsBerthIn48hrs.aspx',
    ]

    def parse(self, response):
        sel = Selector(response)
        table = sel.css('table.infrastructureTable.simpleTable.vesselsTable tr')

        if table:  # Depends if he finds the table or not

            title = table[0].css('th::text').extract()

            if 'Bearth' in response.url:
                HEADER = AT_BERTH_HEADER
            elif 'Anchorage' in response.url:
                # TODO: Create expected header list (for now, no table is available).
                return
            elif 'Expected' in response.url:
                HEADER = EXPECTED_HEADER
            elif 'Sailed' in response.url:
                HEADER = SAILED_HEADER
            elif 'BerthIn48hrs' in response.url:
                HEADER = BERTH_HIST_HEADER

            for tr in table[1:]:
                row = tr.xpath('td/text()').extract()

                if not row:
                    continue

                if not len(title) == len(row):
                    self.logger.error('Header and row have different length')

                item = VesselPortCall()
                for key, val in zip(title, row):
                    if HEADER.get(key):
                        item[HEADER[key]] = val

                if 'vessel_name' not in item:
                    self.logger.error('Vessel name is missing')
                date_fields = ['eta', 'etd', 'arrival_date', 'departure_date']
                if not (any(l in item for l in date_fields)):
                    self.logger.error('Date is missing')

                # hazira port add cargo type in front of the vessel name, eg 'LNG Al Huwaila'
                # so we split the name in 2
                name = item['vessel_name'].split()
                item['cargo_type'] = name[0]
                name_without_cargo_type = " ".join(name[1:])

                # but for vessels that name really start with LNG, they don t
                # duplicate, eg no 'LNG LNG Oyo' 1 #so we test if after removing
                # the cargo_type in the name, it is a valid vessel name
                if name_without_cargo_type in [v['name'] for v in static_data.vessels()]:
                    item['vessel_name'] = name_without_cargo_type

                item['url'] = response.url
                if item['cargo_type'] == 'LNG':
                    yield item
        else:
            self.logger.warning('Table is missing or empty')

        # scrap next page
        if not response.meta.get('next_page'):
            other_pages = sel.xpath('//tr[@class="borderBorrom"]//a/@href').extract()
            for page in other_pages:
                page_number = re.search('(\d+).*', page).group(1)
                formdata = {
                    '__EVENTTARGET': 'gvBreakBulkVessels',
                    '__EVENTARGUMENT': 'Page$' + page_number,
                    '__VIEWSTATE': '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',  # noqa
                    '__VIEWSTATEGENERATOR': '0F687C81',
                    '__EVENTVALIDATION': '/wEdAAVK/r2TLt/ma37K+1nQJUNVeEB0g4USW5kXY53HuZE3i/w/jkBNwg/yVhGc0oQypPkbzfkr32iJv18Vg2yuArOB3bnMusYDgvMlgczsCbAKn1NBXBEF8UtYi4dKVVw8HslDPc9Z',  # noqa
                }

                yield FormRequest(url=response.url, formdata=formdata, meta={'next_page': True})
