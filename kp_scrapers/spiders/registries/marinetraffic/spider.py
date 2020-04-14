"""Spider module for MarineTrafficRegistry spider.

This spider is designed to scrape attribute updates of known vessels,
i.e. vessels with known IMO numbers.

Caveats:
    - It cannot be used to discover new vessels
    - It cannot be used to scrape updates for vessels without IMO numbers,
      e.g. ENI-registered vessels
    - It is HIGHLY RECOMMENDED this spider is run with Crawlera enabled, to protect ourselves

Usage
~~~~~

    $ scrapy crawl MarineTrafficRegistry \
        -a imo=9834442,9774135

"""
import json
import random
from typing import Dict, Iterator

from scrapy import Request, Spider
from scrapy.http import Response

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.static_data import vessels
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.settings.network import USER_AGENT_LIST
from kp_scrapers.spiders.ais import safe_imo
from kp_scrapers.spiders.registries import RegistrySpider
from kp_scrapers.spiders.registries.marinetraffic import normalize


class MarineTrafficRegistry(RegistrySpider, Spider):
    name = 'MarineTrafficRegistry'
    version = '3.0.0'
    # NOTE provider is actually MarineTraffic (duh), but since other sources don't provide
    # MMSI and callsign updates and MT provider is deemed low in reliability,
    # we need to trick the vessel loaders to accept MT updates as if it is of high confidence
    provider = 'GR'  # spoof GibsonRegistry provider
    produces = [DataTypes.Vessel]

    spider_settings = {
        # we witnessed HTTP 429 responses (i.e. too many requests), so we throttle
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 10,
        'AUTOTHROTTLE_MAX_DELAY': 4,
        # sometimes we request for ids Marinetraffic don't know
        'ON_ERROR_HTTP_CODE': 'continue',
    }

    start_urls = [
        'https://www.marinetraffic.com/en/reports?asset_type=vessels&columns=imo,mmsi,callsign&imo={imo}'  # noqa
    ]

    def __init__(self, imo: str = None):
        """Initialize MarineTrafficRegistry spider with IMOs to search.

        Initialises `self.imo` instance attribute that stores the list of IMOs to be scraped.

        """
        # if no IMOs supplied, use complete list of IMOs on Kpler platforms
        if not imo:
            fleet = vessels(disable_cache=True)
            _imos = tuple(v['imo'] for v in fleet if v.get('imo'))
        else:
            _imos = tuple(may_strip(i) for i in imo.split(','))

        # sanity check; in case of invalid IMO numbers
        self.imos = []
        for vessel in _imos:
            if not safe_imo(vessel):
                self.logger.warning('Invalid IMO number: %s', vessel)
            else:
                self.imos.append(vessel)

    @property
    def headers(self) -> Dict[str, str]:
        # required headers, else we get HTTP 400
        headers = {
            # TODO not sure if this has an impact on data integrity, to be checked
            'Vessel-Image': '0099d8282c443d7c8505af314d2785462b08',
        }

        # naive attempt at generating random fingerprints
        if self.settings.get('CRAWLERA_ENABLED'):
            # required for crawlera support
            headers.update({'X-Crawlera-Profile': 'desktop'})
        else:
            headers.update({'User-Agent': random.choice(USER_AGENT_LIST)})

        return headers

    def start_requests(self) -> Iterator[Request]:
        """Entrypoint for MarineTrafficRegistry spider."""

        for imo in self.imos:
            yield Request(
                url=self.start_urls[0].format(imo=imo),
                headers=self.headers,
                callback=self.parse_individual,
            )

    def parse_individual(self, response: Response) -> Dict[str, str]:
        """Parse individual vessel details page."""

        json_resp = json.loads(response.text)
        if not json_resp.get('data'):
            self.logger.info("No vessel found, or resource has changed")
            return

        raw_item = {
            'provider_name': self.provider,
            # response should return only one item, since IMO is unique
            **json_resp['data'][0],
        }
        yield normalize.process_item(raw_item)
