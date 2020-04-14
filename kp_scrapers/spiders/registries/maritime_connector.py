# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
import re

from scrapy.http import Request
from scrapy.selector import Selector

from kp_scrapers.models.items import Vessel
from kp_scrapers.settings import USER_AGENT
from kp_scrapers.spiders.bases.markers import DeprecatedMixin
from kp_scrapers.spiders.registries import RegistrySpider


FIELDS = {
    'IMO number': ['imo'],
    'Name of the ship': ['name'],
    'Type of ship': ['type'],
    'MMSI': ['mmsi'],
    'Gross tonnage': ['gross_tonnage'],
    'DWT': ['dead_weight'],
    'Year of build': ['build_year'],
    'Builder': ['builder'],
    'Flag': ['flag_name'],
    'Home port': ['home_port'],
    'Class society': ['ship_class'],
    'Manager & owner': ['manager', 'owner'],
    'Manager': ['manager'],
    'Owner': ['owner'],
    'Former names': ['former_names'],
    'Last known flag': ['last_flag'],
}


class MaritimeConnectorSpider(DeprecatedMixin, RegistrySpider):
    name = 'MaritimeConnector'
    start_urls = [
        'http://maritime-connector.com/ship-search/?keyword=&ship=&imo=&type=lng-tanker&limit=25',
        'http://maritime-connector.com/ship-search/?keyword=&ship=&imo=&type=lpg-tanker&limit=25',
    ]

    def parse(self, response):
        sel = Selector(response)
        vessel_count_raw = sel.xpath(
            '//div[@class="box-head" and contains(., "Listing")]/p[@class="result-count"]/text()'
        ).extract()[
            0
        ]  # noqa

        vessel_count = re.search('\d{3,4}', vessel_count_raw).group()
        url = response.url.replace('25', vessel_count)
        yield Request(url=url, callback=self.parse_vessel_list)

    def parse_vessel_list(self, response):
        sel = Selector(response)
        links = sel.xpath('//a[@class="ship-name"]/@href').extract()
        for link in links:
            yield Request(
                url=link,
                # url = 'http://maritime-connector.com/ship/berriz-6510215/',  # noqa
                callback=self.parse_vessel,
            )

    def parse_vessel(self, response):
        sel = Selector(response)
        item = Vessel()
        item['url'] = response.url
        rows = sel.xpath('//table[@class="ship-data-table"]//tr')
        for row in rows:
            th = row.xpath('th/text()').extract()
            td = row.xpath('td/text()').extract()
            label = th[0] if th else None
            value = td[0] if td else None
            if label and value:
                for field in FIELDS.get(label):
                    item[field] = value

        if item.get('imo') is None:
            self.logger.warning(
                'Vessel without an IMO: vessel name is {} ({})'.format(
                    item.get('name', 'unknown vessel'), response.url
                )
            )
            yield item
        else:
            yield Request(
                url='http://www.marinetraffic.com/ais/details/ships/' + item['imo'],
                # url= 'http://www.marinetraffic.com/ais/details/ships/6510215',
                headers={'User-Agent': USER_AGENT},
                meta={'item': item},
                callback=self.get_status,
            )

    def get_status(self, response):
        item = response.meta['item']
        if not response.url.endswith('abuse'):
            sel = Selector(response)
            if sel.xpath(
                '//div[@class="group-ib nospace-between short-line" and contains(., "IMO")]/b/text()'  # noqa
            ):
                # IMO from MarineTraffic
                MT_imo = sel.xpath(
                    '//div[@class="group-ib nospace-between short-line" and contains(., "IMO")]/b/text()'  # noqa
                ).extract()[0]
                if MT_imo == item['imo']:
                    status = sel.xpath(
                        "//div[@class='group-ib nospace-between short-line' and contains(., 'Status')]/b/text()"  # noqa
                    )
                    if status:
                        item['status'] = status.extract()[0]
                else:
                    self.logger.warning(
                        'Vessel imo %s doesnt match with Marine Traffic imo %s '
                        % (item['imo'], MT_imo)
                    )  # noqa
            else:
                self.logger.warning(
                    'Vessel imo {} is missing from Marine Traffic'.format(item['imo'])
                )  # noqa
        yield item
