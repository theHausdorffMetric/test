# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy.http import Request
from scrapy.selector import Selector
from six.moves import range

from kp_scrapers.models.items import Vessel
from kp_scrapers.spiders.bases.markers import DeprecatedMixin, OilMarker
from kp_scrapers.spiders.registries import RegistrySpider


class FPSOSpider(DeprecatedMixin, RegistrySpider, OilMarker):

    name = 'FPSO'
    start_urls = ['http://fpso.com/fpso/']

    def parse(self, response):
        selector = Selector(response)
        number_of_pages = (
            selector.xpath('//div[@class="paginate"]')[0].css('a *::text')[-2].extract()
        )

        for i in range(1, int(number_of_pages) + 1):
            yield Request(url='{}/?page={}'.format(self.start_urls[0], i), callback=self.parse_page)

    def parse_page(self, response):
        selector = Selector(response)
        table = selector.xpath('//table')
        for (i, row) in enumerate(table.css('tr')):
            # Skip header row
            if i == 0:
                continue

            data_dict = row.css('td *::text').extract()
            vessel_name = data_dict[0]
            owner = data_dict[1]
            # operator = data_dict[2]
            # field_operator = data_dict[3]
            # location_field = data_dict[4]  # TODO: migration
            # country = data_dict[5]  # It is not the flag
            capacity = data_dict[6]

            item = Vessel()
            item['name'] = vessel_name
            item['capacity'] = capacity
            item['owner'] = owner

            yield item
