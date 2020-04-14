from __future__ import absolute_import, unicode_literals
import re

from scrapy import Spider
from scrapy.http import Request
from scrapy.selector import Selector

from kp_scrapers.lib import static_data
import kp_scrapers.lib.utils as utils
from kp_scrapers.models.items import VesselPosition
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.ais import AisSpider


RAW_FIELDS = {'in_range': '.panel-heading .col-xs-4', 'name': 'h1'}

FIELDS = {
    'currently_in_port': 'Currently in Port',
    'last_port': 'Last Known Port',
    'draught': 'Draught',
    'destination': 'Destination',
    'area': 'Area',
    'received_time': 'received_time',
    'mmsi': 'MMSI',
    'imo': 'IMO',
    'call_sign': 'Call Sign',
    'eta': 'ETA',
    'eta_updated_time': 'Info Received',
    'air_temperature': 'Temperature',
    'wind_speed': 'Wind',
    'wind_bearing': 'Wind direction',
    'ais_source': 'AIS Source',
}

POS_FIELDS = {
    'Timestamp (UTC)': 'received_time',
    'AIS Source': 'ais_source',
    'Speed (kn)': 'speed',
    'Latitude': 'latitude',
    'Longitude': 'longitude',
    'Course': 'course',
}

port_pattern = '(.*)\[.*'

CLEANUPS = {
    'destination': port_pattern,
    'last_port': port_pattern,
    'currently_in_port': port_pattern,
    'speed': '(.*)kn',
    'course': '(.*)°',
    'wind_speed': '(.*)knots',
    'wind_bearing': '.*\((.*)o\)',
    'draught': '(.*)m',
    'air_temperature': '(.*)oC',
}

BASE_URL = 'http://www.marinetraffic.com'
SEARCH_URL = 'http://www.marinetraffic.com/en/ais/index/search/all?keyword={}'


class MarineTrafficManual(AisSpider, Spider):
    '''Scrapes vessels positions from MarineTraffic without using the API'''

    name = 'MarineTrafficManual'
    version = '1.0.0'
    provider = 'MT'
    produces = [DataTypes.Ais, DataTypes.Vessel]

    def _field(self, selector, css_path):
        q = css_path + ' *::text'
        return ''.join(selector.css(q).extract()).strip()

    def clean_fields(self, item):
        for name, pattern in CLEANUPS.items():
            if name in item:
                m = re.match(pattern, item[name])
                if m:
                    item[name] = m.groups()[0].strip()
        return item

    def start_requests(self):
        imos = [
            str(vessel['imo'])
            for vessel in static_data.vessels()
            if 'imo' in vessel and vessel['imo'] and self.provider in vessel['providers']
        ]

        for imo in imos:
            yield Request(
                url=SEARCH_URL.format(imo),
                headers={'User-Agent': utils.USER_AGENT},
                meta={'imo': imo},
                callback=self.parse_search,
            )

    def parse_search(self, response):
        vessel_path = response.css('.search_index_link::attr(href)').extract()
        if vessel_path:
            vessel_url = BASE_URL + vessel_path[0]
            yield Request(
                url=vessel_url,
                headers={'User-Agent': utils.USER_AGENT},
                meta={'imo': response.meta['imo']},
                callback=self.parse_vessel,
            )

    def parse_vessel(self, response):
        selector = Selector(response)
        item = VesselPosition()
        item['imo'] = response.meta['imo']

        scrapped_item = {}
        for div in selector.xpath('//div[@class="group-ib nospace-between short-line"]'):
            label = self._field(div, 'span')
            value = self._field(div, 'b')
            if label.endswith(':'):
                label = label[:-1]
            if label and value and value != '-':
                scrapped_item[label] = value

        # Position related Info
        for div in selector.xpath('//div[@class="table-cell cell-full collapse-768"]/div'):
            spans = div.xpath('span')
            if len(spans) >= 2:
                label = self._field(spans[0], '')
                value = self._field(spans[1], '')
                if label.endswith(':'):
                    label = label[:-1]
                if label == 'Position Received at':
                    label = 'received_time'
                    value = ''.join(spans[1].css('time::attr(datetime)').extract()).strip()
                if label and value and value != '-':
                    scrapped_item[label] = value

        # Destination, last port and ETA info
        try:
            eta_value = (
                response.css('.panel-body')[1]
                .css('.font-100')[1]
                .xpath('span//text()')[0]
                .extract()
                .strip()
            )
            if eta_value != '-':
                item['eta'] = eta_value
        except IndexError:
            self.logger.warning("ETA not found in URL: {}".format(response.url))

        try:
            last_port_value = (
                response.xpath('//div[contains(@data-toggle, "tooltip")]/@title')[0]
                .extract()
                .strip()
            )
            if last_port_value:
                item['last_port'] = last_port_value
        except IndexError:
            self.logger.warning("Last port not found in URL: {}".format(response.url))

        try:
            xpath_ = '//div[contains(@data-toggle, "tooltip")]/@title'
            destination_value = response.xpath(xpath_)[1].extract().strip()
            if destination_value and destination_value != 'Destination port not recognized':
                item['destination'] = destination_value
        except IndexError:
            self.logger.warning("Destination not found in URL: {}".format(response.url))

        # Draught and speed info
        for line in selector.css('.voyage-related tr'):
            label = self._field(line, 'td:nth-child(1)')
            value = None
            if label == 'Destination':
                values = line.css('td:nth-child(2) i::attr(title)').extract()
                if values:
                    m = re.match('Destination reported by AIS:(.*)', values[0])
                    if m:
                        value = m.groups()[0].strip()
                    else:
                        value = ''
            if value is None:
                value = self._field(line, 'td:nth-child(2)')
            if label.endswith(':'):
                label = label[:-1]
            if label and value and value != '-':
                scrapped_item[label] = value

        # Meteo related info
        for div in selector.css('.wind-area div'):
            label = self._field(div, 'span:nth-child(1)')
            value = self._field(div, 'span:nth-child(2)')
            if label.endswith(':'):
                label = label[:-1]
            if label and value and value != '-':
                scrapped_item[label] = value

        for name, css_path in RAW_FIELDS.items():
            value = self._field(selector, css_path)
            if value:
                item[name] = value

        for name, scrapped_name in FIELDS.items():
            value = scrapped_item.get(scrapped_name)
            if value:
                item[name] = value

        lat_long = scrapped_item.get('Latitude / Longitude')
        if lat_long:
            latitude, longitude = [i.strip() for i in lat_long.split('/')]
            item['latitude'] = latitude[:-1]
            item['longitude'] = longitude[:-1]

        speed_course = scrapped_item.get('Speed/Course')
        if speed_course:
            speed, course = [i.strip() for i in speed_course.split('/')]
            if speed and speed != '-':
                item['speed'] = speed
            if course and course != '-':
                item['course'] = course

        item = self.clean_fields(item)

        item['url'] = response.url

        # Log if important information is missing for a vessel position
        imo_ = item['imo']
        if 'latitude' not in item or 'longitude' not in item:
            self.logger.warning('Missing lat/long for vessel with IMO: {}'.format(imo_))
        if 'received_time' not in item:
            self.logger.warning("Missing received_time for vessel with IMO: {}".format(str(imo_)))

        yield item
