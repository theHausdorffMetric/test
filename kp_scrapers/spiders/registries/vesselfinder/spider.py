"""VesselFinder Spider

Usage
~~~~~

    Cargo type:
        Cargo vessels:
        - General / Dry Cargo: 4
        - Bulk Carrier: 402
        - Container / Reefer: 401

        Tankers:
        - Tanker: 6
        - LNG / LPG / CO2 Tanker: 601
        - Chemical Tanker: 602
        - Oil Tanker: 603

        Misc:
        - Passenger / Cruise: 3
        - High Speed Craft: 2
        - Yacht / Pleasure Craft: 8
        - Fishing: 5
        - Offshore: 901
        - Military: 7
        - Auxiliary: 0
        - Other / Unknown type: 1

    $ scrapy crawl VesselFinderRegistry \
        -a min_year=2000 \
        -a min_dwt=20000 \
        -a vessel_type=4

"""
import random
from urllib.parse import urlencode

from scrapy import Request, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.settings import USER_AGENT_LIST
from kp_scrapers.spiders.registries import RegistrySpider
from kp_scrapers.spiders.registries.vesselfinder import normalize, parser


class VesselFinderRegistry(RegistrySpider, Spider):
    name = 'VesselFinderRegistry'
    provider = 'VF'
    version = '2.0.1'
    produces = [DataTypes.Vessel]

    start_urls = ['https://www.vesselfinder.com/vessels']

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # init search params
        self._min_year = kwargs.get('min_year')
        self._max_year = kwargs.get('max_year')
        self._min_dwt = kwargs.get('min_dwt')
        self._max_dwt = kwargs.get('max_dwt')
        self._vessel_type = kwargs.get('vessel_type')

        # total vessels to extract
        self._total = None

    def start_requests(self):
        """Entry point of VesselFinderRegistry spider.

        Yields:
            scrapy.Request:

        """
        yield Request(url=self.search_url, callback=self.parse_search_results, headers=self.headers)

    def parse_search_results(self, response):
        """Parse vessel page and next page.

        Args:
            response:

        Yields:
            scrapy.Request:

        """
        # display total vessels obtained for visibility
        if not self._total:
            self._total = (
                response.xpath('.//div[@class="pagination-totals"]/text()').extract_first()
                or '0 vessels'
            )
            self.logger.info(f'Found {self._total} matching search parameters')

        for ship_link in response.xpath('//td[@class="v1"]//a/@href').extract():
            yield Request(
                url=self.base_url + ship_link,
                callback=self.parse_vessel_detail,
                headers=self.headers,
            )

        # traverse paginated results
        next_page = response.xpath('//a[@class="pagination-next"]/@href').extract_first()
        if next_page:
            yield Request(
                url=self.base_url + next_page,
                callback=self.parse_search_results,
                headers=self.headers,
            )

    def parse_vessel_detail(self, response):
        """Get individual vessel detail page.

        Args:
            response:

        Yields:
            Dict[str, str]:

        """
        raw_item = parser.extract_vessel_attributes(response)
        raw_item.update(provider_name=self.provider)

        yield normalize.process_item(raw_item)

    @property
    def base_url(self):
        return 'https://www.vesselfinder.com'

    @property
    def search_url(self):
        _query_string = {}
        if self._min_year:
            _query_string.update({'minYear': self._min_year})
        if self._max_year:
            _query_string.update({'maxYear': self._max_year})
        if self._min_dwt:
            _query_string.update({'minDW': self._min_dwt})
        if self._max_dwt:
            _query_string.update({'maxDW': self._max_dwt})
        if self._vessel_type:
            _query_string.update({'type': self._vessel_type})

        return f'{self.start_urls[0]}?{urlencode(_query_string)}'

    @property
    def headers(self):
        if self.settings.get('CRAWLERA_ENABLED'):
            # support crawlera since we are hitting the website too frequently
            headers = {'X-Crawlera-Profile': 'desktop'}
        else:
            headers = {'User-Agent': random.choice(USER_AGENT_LIST)}

        return headers
