# -*- coding: utf8 -*-

"""UK Operators send out in realtime.

There are several installations available on this website.
We are interested in South Houk, Dragon, and Grain.
However, grain send out is unreliable on this site.
We have an other source for Grain send out, which is not in realtime.
(See `operator_grain_sendout.py`)
"""

from __future__ import absolute_import, unicode_literals
import csv

from scrapy import Request
from scrapy.spiders import CrawlSpider

from kp_scrapers.lib.utils import to_unicode
from kp_scrapers.models.items import RealTimeSendOut

from . import OperatorSpider


BASE_URL = 'http://mip-prod-web.azurewebsites.net'
INSTALLATIONS = ['SOUTH HOOK', 'DRAGON']


class UKOperatorRealTimeSendoutSpider(OperatorSpider, CrawlSpider):

    name = 'UKOperatorRealTimeSendOut'
    allowed_domains = ['mip-prod-web.azurewebsites.net']
    start_urls = [
        'http://energywatch.natgrid.co.uk/EDP-PublicUI/Public/InstantaneousFlowsIntoNTS.aspx'
    ]

    def parse(self, response):
        d_link = response.xpath('//a[@id="CsvDataLinkButton"]/@href').extract_first()
        url = BASE_URL + d_link

        yield Request(url=url, callback=self.parse_csv)

    @staticmethod
    def parse_csv(response):
        csv_file = response.body
        data = csv.reader(to_unicode(csv_file).rstrip('\r\n').split('\r\n'))
        for row in data:
            for inst in INSTALLATIONS:
                if inst in row[0]:
                    item = RealTimeSendOut()
                    item['installation'] = inst
                    item['value'] = row[2]
                    item['date'] = row[3]

                    yield item
