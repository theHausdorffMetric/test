# -*- coding: utf-8 -*-

"""Bypassing fleetmon API to directly get web data.

Note that Fleetmon data feed is now included into ExactEarth one, hence
covering both terrestrial and satellite sources.

"""

from __future__ import absolute_import, unicode_literals
import json

from scrapy.selector import Selector
from scrapy.spiders import Spider

from kp_scrapers.spiders.ais import AisSpider


class FleetmonWebSpider(AisSpider, Spider):
    name = 'FleetmonWeb'
    version = '0.1.0'
    # not yet deployed nor processed
    provider = None

    allowed_domains = ['fleetmon.com']
    start_urls = [
        # // Oil Product
        # Jet
        'https://www.fleetmon.com/services/tanker-tracker/jet/?formap=oilcomm_map&min_lat=-82.76537263027349&min_lon=-400.078125&max_lat=82.76537263027352&max_lon=400.078125',  # noqa
        # Gasoline
        'https://www.fleetmon.com/services/tanker-tracker/gasoline/?formap=oilcomm_map&min_lat=-82.76537263027349&min_lon=-400.078125&max_lat=82.76537263027352&max_lon=400.078125',  # noqa
        # Diesel
        'https://www.fleetmon.com/services/tanker-tracker/diesel/?formap=oilcomm_map&min_lat=-82.76537263027349&min_lon=-400.078125&max_lat=82.76537263027352&max_lon=400.078125',  # noqa
        # // Agri
        # baltic
        'https://www.fleetmon.com/services/agricultural-commodities/?formap=agricomm_map&min_lat=54.36775852406841&min_lon=-40.25390625&max_lat=71.69129271863999&max_lon=59.765625&_=1507710471430',  # noqa
        # europe + med
        'https://www.fleetmon.com/services/agricultural-commodities/?formap=agricomm_map&min_lat=27.994401411046148&min_lon=-32.607421875&max_lat=56.46249048388979&max_lon=67.412109375&_=1507710471444',  # noqa
        # suez, PG, Indian Ocean
        'https://www.fleetmon.com/services/agricultural-commodities/?formap=agricomm_map&min_lat=-2.284550660236957&min_lon=14.150390625&max_lat=34.88593094075317&max_lon=114.169921875&_=1507710471445',  # noqa
        # oceania
        'https://www.fleetmon.com/services/agricultural-commodities/?formap=agricomm_map&min_lat=-54.470037612805754&min_lon=49.5703125&max_lat=13.752724664396988&max_lon=249.609375&_=1507710471458',  # noqa
        # far east
        'https://www.fleetmon.com/services/agricultural-commodities/?formap=agricomm_map&min_lat=18.47960905583197&min_lon=90.966796875&max_lat=50.28933925329178&max_lon=190.98632812499997&_=1507710471477',  # noqa
        # us
        'https://www.fleetmon.com/services/agricultural-commodities/?formap=agricomm_map&min_lat=19.80805412808859&min_lon=-152.666015625&max_lat=51.17934297928927&max_lon=-52.64648437499999&_=1507710471485',  # noqa
        # northern Atlantic
        'https://www.fleetmon.com/services/agricultural-commodities/?formap=agricomm_map&min_lat=23.644524198573688&min_lon=-95.00976562499999&max_lat=53.69670647530323&max_lon=5.009765625&_=1507710471496',  # noqa
        # central atlantic
        'https://www.fleetmon.com/services/agricultural-commodities/?formap=agricomm_map&min_lat=-16.383391123608387&min_lon=-85.341796875&max_lat=22.350075806124867&max_lon=14.677734375000002&_=1507710471498',  # noqa
        # south am
        'https://www.fleetmon.com/services/agricultural-commodities/?formap=agricomm_map&min_lat=-60.239811169998916&min_lon=-123.22265625000001&max_lat=3.162455530237848&max_lon=76.81640625&_=1507710471515',  # noqa
    ]
    spider_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
        }
    }

    def parse(self, response):
        json_response = json.loads(response.body_as_unicode())
        for voyage_data in json_response:
            # Json object has same structure between Oil product pages and agri pages,
            # but the popupcontent key that hold html has a different html structure

            # Similar info
            provider_vessel_id = voyage_data['id']
            vessel_type = voyage_data['icon']
            voyage_state = voyage_data['voyagestate']
            latitude = voyage_data['latitude']
            longitude = voyage_data['longitude']

            # Differentiate popup html parsing for agricultural pages from oil products
            oil_product_page = response.url.find('agricultural') == -1
            additional_info = Selector(text=voyage_data['popupcontent'])

            if oil_product_page:
                parent_product = 'oil products'
                vessel_flag = additional_info.css('.flag-icon::attr(title)').extract_first().strip()
                vessel_length = (
                    additional_info.xpath(
                        '//div/div[1]/div[2]/span[2]/following-sibling::text()[1]'
                    )
                    .extract_first()
                    .strip()
                )  # noqa

                destination = (
                    additional_info.xpath('//div/div[4]/div[2]/text()').extract_first().strip()
                )  # noqa
                cargo_type = (
                    additional_info.xpath('//div/div[6]/div[2]/text()').re('.* kt (.*)')[0].strip()
                )  # noqa
                speed = (
                    additional_info.xpath('//div/div[3]/div[2]/text()').extract_first().strip()
                )  # noqa
                current_location = (
                    additional_info.xpath('//div/div[2]/div[2]/text()').extract_first().strip()
                )  # noqa
                last_report = (
                    additional_info.xpath('//div/div[5]/div[2]/text()').extract_first().strip()
                )  # noqa
                vessel_state = None

            else:  # agri
                parent_product = 'agricultural products'

                vessel_info = additional_info.css('.text')

                vessel_flag = vessel_info.css('.flag-icon::attr(title)').extract_first().strip()
                vessel_length = (
                    vessel_info.xpath('.//span[text() = "Length:"]/following-sibling::text()[1]')
                    .extract_first()
                    .strip()
                )  # noqa

                more_info = additional_info.css('.moreinfos')
                destination = (
                    more_info.xpath('.//li[span/text() = "Destination:"]/span[2]/text()')
                    .extract_first()
                    .strip()
                )  # noqa
                cargo_type = (
                    more_info.xpath('.//li[span/text() = "Cargo:"]/span[2]/text()')
                    .extract_first()
                    .strip()
                )  # noqa
                speed = (
                    more_info.xpath('.//li[span/text() = "Speed:"]/span[2]/text()')
                    .extract_first()
                    .strip()
                )  # noqa
                current_location = (
                    more_info.xpath('.//li[span/text() = "Location:"]/span[2]/text()')
                    .extract_first()
                    .strip()
                )  # noqa
                last_report = (
                    more_info.xpath('.//li[span/text() = "Last report:"]/span[2]/text()')
                    .extract_first()
                    .strip()
                )  # noqa
                vessel_state = (
                    more_info.xpath('.//li[span/text() =  "Status:"]/span[2]/text()')
                    .extract_first()
                    .strip()
                )  # noqa

            # TODO implement normalization function
            yield {
                'parent_product': parent_product,
                'voyage_state': voyage_state,
                'vessel_state': vessel_state,
                'vessel': {
                    'provider_vessel_id': provider_vessel_id,
                    'length': vessel_length,
                    'vessel_type': vessel_type,
                    'vessel_flag': vessel_flag,
                },
                'latitude': latitude,
                'longitude': longitude,
                'speed': speed,
                'current_location': current_location,
                'last_report': last_report,
                'destination': destination,
                'cargo_type': cargo_type,
            }
