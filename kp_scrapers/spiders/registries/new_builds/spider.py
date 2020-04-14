import random
from urllib.parse import urlencode

from scrapy import Request, Spider

from kp_scrapers.lib.static_data import Collection
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.settings import USER_AGENT_LIST
from kp_scrapers.spiders.registries import RegistrySpider
from kp_scrapers.spiders.registries.new_builds import normalize, parser


class NewBuildsRegistrySpider(RegistrySpider, Spider):
    name = 'NewBuildsRegistry'
    provider = 'VF'
    version = '1.1.0'
    produces = [DataTypes.Vessel]

    base_url = 'https://www.vesselfinder.com'
    collection_name = 'vessels-missing.latest.jl.gz'

    # holds newbuilds that failed to match to any known vessel on VF
    unknown_newbuilds = {}

    spider_settings = {
        # notify on Slack newbuilds that failed to match to any known vessel on VF
        'NOTIFY_ENABLED': True,
        # force notification even if no scraped items
        'NOTIFY_ON_NO_DATA': True,
        # enable GDrive export
        'KP_DRIVE_ENABLED': True,
        # yield unscraped newbuilds (custom data) on GDrive
        'KP_DRIVE_CUSTOM_EXPORT': 'unknown_newbuilds',
        # export unknown newbuilds to specific spreadsheet
        'KP_DRIVE_SHEET_ID': '14Qf63QMav6h_SWYavDFlrUsi7gc2vTKj0yx3DI6GNE4',
    }

    def start_requests(self):
        """Entry point of UnknownVesselRegistry spider.

        Yields:
            Request:

        """
        vessels = Collection(self.collection_name, index='TrackId').load_and_cache()
        # workaround scrapy's async workflow to ensure `unknown_newbuilds` is properly initialised
        for v in vessels:
            if parser.is_vessel_too_small(v):
                self.logger.debug(f"Vessel is below DWT limit, skipping: {v['name']} {v['_raw']}")
                continue

            self.unknown_newbuilds[v.get('TrackId')] = v

        for v in vessels:
            if parser.is_vessel_too_small(v):
                self.logger.debug(f"Vessel is below DWT limit, skipping: {v}")
                continue

            yield Request(
                url=self.get_search_url(v.get('name')),
                callback=self.parse_search_results,
                headers=self.headers,
                meta={'vessel': v},
            )

    def parse_search_results(self, response):
        """Parse vessel page and next page.

        Args:
            response:

        Yields:
            scrapy.Request:

        """
        # display total vessels obtained for visibility
        _total = (
            response.xpath('.//div[@class="pagination-totals"]/text()').extract_first()
            or '0 vessel'
        )
        self.logger.debug(f'Found {_total} matching search parameters')

        if _total == '0 vessel':
            return

        results = list(parser.get_vessel_general_info(response))
        for vessel in results:
            if parser.is_same_vessel(vessel, response.meta['vessel']):
                # remove known vessels from initialised `self.unknown_newbuilds`
                self.unknown_newbuilds.pop(response.meta['vessel']['TrackId'], None)
                # extract vessel attributes from source
                yield Request(
                    url=self.base_url + vessel.get('url'),
                    callback=self.parse_vessel_detail,
                    headers=self.headers,
                    meta={'vessel': response.meta['vessel']},
                )
                # stop scanning if we already match the result
                return

        # traverse paginated results if any
        next_page = response.xpath('//a[@class="pagination-next"]/@href').extract_first()
        if next_page:
            yield Request(
                url=self.base_url + next_page,
                callback=self.parse_search_results,
                headers=self.headers,
                meta={'vessel': response.meta['vessel']},
            )

    def parse_vessel_detail(self, response):
        """Get individual vessel detail page.

        Args:
            response:

        Yields:
            Dict[str, str]:

        """
        raw_item = parser.extract_vessel_attributes(response)
        # contextualise raw item with meta info
        raw_item.update(provider_name=self.provider, status=response.meta['vessel'].get('status'))

        # NOTE override VF build_year with Clarksons build_year, according to analysts
        raw_item['Year of Built'] = response.meta['vessel']['build_year']

        yield normalize.process_item(raw_item)

    def get_search_url(self, query_string):
        """Get search url of searching by name / imo / mmsi.

        Args:
            query_string (str): vessel name / imo / mmsi

        Returns:
            str: url

        """
        # sorted by build_year
        _query_string = {'name': query_string}
        return f'{self.base_url}/vessels?{urlencode(_query_string)}'

    @property
    def headers(self):
        if self.settings.get('CRAWLERA_ENABLED'):
            # support crawlera since we are hitting the website too frequently
            headers = {'X-Crawlera-Profile': 'desktop'}
        else:
            headers = {'User-Agent': random.choice(USER_AGENT_LIST)}

        return headers
