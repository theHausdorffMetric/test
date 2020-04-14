# -*- coding: utf-8; -*-

from __future__ import absolute_import, print_function, unicode_literals
import codecs
import datetime as dt
import json
import os
import re

from dateutil.parser import parse
from scrapy import signals
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.utils.project import data_path

from kp_scrapers.lib import static_data
from kp_scrapers.models.items import VesselPositionAndETA
from kp_scrapers.spiders.ais import AisSpider


# If X-Crawlera-UA isn’t specified, the USER-AGENT will default to crawlera
# (http://doc.scrapinghub.com/crawlera.html#x-crawlera-ua) When Crawlera is
# activated in SH, the header {'X-Crawlera-UA': 'desktop'} sets the request
# User-Agent to a random desktop agent.  To run this spider locally use instead
# HEADERS = {'USER-AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36'} # noqa
HEADERS = {'X-Crawlera-UA': 'desktop'}

ITEM_DICT = {
    'nextDestination_destination': 2,
    'nextDestination_eta': 3,
    'master_imo': 9,
    'master_shipType': 0,
    'master_callsign': 8,
}

DIRECTION_SPEED_INDEX = 7
LATLON_INDEX = 5


def _do_normalize_item_float_neg_pos(item, key, neg, pos):
    if item[key].endswith(neg):
        item[key] = float(item[key].replace(neg, '').strip()) * -1.0
    elif item[key].endswith(pos):
        item[key] = float(item[key].replace(pos, '').strip()) * 1.0
    else:
        raise ValueError(
            'Value of {} in item ends with neither {} or {}: {}'.format(key, neg, pos, item[key])
        )


def normalize_lat_lng(item):
    try:
        _do_normalize_item_float_neg_pos(item, 'position_lat', 'S', 'N')
        _do_normalize_item_float_neg_pos(item, 'position_lon', 'W', 'E')
        return item
    except ValueError:
        item['position_lat'] = ''
        item['position_lon'] = ''
        print(
            (
                'Couldnt normalize this position values lat = {} & long = {} '.format(
                    item['position_lat'], item['position_lon']
                )
            )
        )


def normalize_length(value):
    if value.endswith('km'):
        return float(value.replace('m', '').strip()) * 1000.0
    if value.endswith('m'):
        return float(value.replace('m', '').strip())


def normalize_float(item, key):
    try:
        item[key] = float(item[key].strip())
    except ValueError:
        item[key] = ''
        print(
            (
                '{} couldnt be converted from {} to float in the next item {}'.format(
                    key, item[key], item
                )
            )
        )


class VesselFinderSpider(AisSpider, Spider):
    name = 'VesselFinderManual'
    next_run = dict()
    file_path = ''

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(VesselFinderSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        return spider

    def start_requests(self):
        vessel_list = static_data.vessels()
        imos = [
            vessel['imo']
            for vessel in vessel_list
            if 'imo' in vessel and 'VF' in vessel['providers']
        ]
        for imo in imos:
            should_follow = (
                imo in self.next_run and dt.datetime.utcnow() > parse(self.next_run[imo])
            ) or (imo not in self.next_run)
            if should_follow:
                self.logger.info('Vessel %s is processing' % str(imo))
                url = 'https://www.vesselfinder.com/fr/vessels/x-imo-' + imo
                yield Request(url=url, headers=HEADERS, callback=self.parse)
            else:
                self.logger.info(
                    'Vessel {} is schedueled to run at {}'.format(str(imo), str(self.next_run[imo]))
                )

    def parse(self, response):
        sel = Selector(response)
        item = VesselPositionAndETA(provider_id='VF')
        title_selection = sel.xpath('//h1[@itemprop="name"]/text()').extract()
        if title_selection:
            item['provider_id'] = 'VF'
            item['aisType'] = 'T-AIS'
            item['master_name'] = title_selection[0]
            item['url'] = response.url
            ship_det = sel.xpath('//*[@itemprop="value"]/text()').extract()

            for item_name, item_pos in ITEM_DICT.items():
                item[item_name] = ship_det[item_pos]

            item['master_mmsi'] = sel.xpath('//*[@itemprop="productID"]/text()')[0].extract()

            # Other fields
            xpath_ = '//div[@class="details-row"]/div[@class="row param"][8]/span[2]/text()'
            draught = sel.xpath(xpath_)[0].extract().strip()
            item['position_draught'] = normalize_length(draught)

            latlon = (
                ship_det[DIRECTION_SPEED_INDEX]
                .strip()
                .replace('\xb0', '')
                .replace('\xa0', '')
                .replace('kn.', '')
            )
            (item['position_course'], item['position_speed']) = re.match(
                '(N/A|[^/]*)\s*/\s*(N/A|[^/]*)', latlon
            ).groups()
            normalize_float(item, 'position_course')
            normalize_float(item, 'position_speed')
            item['position_lat'] = (
                sel.xpath('//*[@itemprop="latitude"]/text()')[0].extract().strip()
            )
            item['position_lon'] = (
                sel.xpath('//*[@itemprop="longitude"]/text()')[0].extract().strip()
            )
            normalize_lat_lng(item)

            item['position_timeReceived'] = sel.xpath('//time/@datetime')[0].extract().strip()
            item['position_aisType'] = 'T-AIS'
            item['nextDestination_aisType'] = 'T-AIS'

            # check if out of range
            if 'N/A' in item['position_timeReceived']:
                time_received = None
            else:
                time_received = parse(item['position_timeReceived']).replace(tzinfo=None)

            if not time_received or not time_received > dt.datetime.utcnow() - dt.timedelta(days=1):
                hours_ago = str(dt.datetime.utcnow() + dt.timedelta(hours=12))
                self.next_run[item['master_imo']] = hours_ago

            if 'N/A' in item['nextDestination_eta']:
                item['nextDestination_eta'] = None

            yield item

    def spider_opened(self, spider):
        self.file_path = data_path('') + self.name + 'Hist.json'
        if os.path.exists(self.file_path):
            with codecs.open(self.file_path, 'rb', 'utf-8') as in_file:
                self.next_run = json.load(in_file)

    def spider_closed(self, spider):
        with codecs.open(self.file_path, 'wb', 'utf-8') as in_file:
            json.dump(self.next_run, in_file)
