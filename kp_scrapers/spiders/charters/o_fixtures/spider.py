"""Spider module for Ocean Freight Exchange fixtures.

Each day, OFE publishes new fixtures on their website that can simply be obtained via
an unsecured endpoint providing JSON responses.

Typically, free public usage is limited to only 10 fixtures per request.
However, by accessing their endpoint directly we can obtain more than 10 fixtures at any time.

!! NOTICE !!
Due to the sensitive nature of the data, it is HIGHLY RECOMMENDED this spider
is run with Crawlera enabled, lest we are tracked and banned (or the loophole fixed).

Usage
~~~~~~

Choose how many fixtures to receive within the laycan period (month-first format):

    $ scrapy crawl O_Fixtures \
        -a batch_size=10 \
        -a laycan='23/08/2018,11/09/2018' \
        -a vessel_type='dry bulk'

"""
import datetime as dt
import json
import random
from urllib.parse import urlencode

from scrapy import Request, Spider
from scrapy.exceptions import CloseSpider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.settings.network import USER_AGENT_LIST
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.o_fixtures import normalize


class OceanFreightExchangeSpider(CharterSpider, Spider):
    name = 'O_Fixtures'
    version = '1.1.1'
    provider = 'O_Fixtures'
    produces = [DataTypes.SpotCharter]

    start_urls = [
        # unofficial api endpoint
        'https://api-app.theofe.com/api/fixtures/'
    ]

    def __init__(self, laycan=None, vessel_type=None, batch_size=None):
        """Initialize OceanFreightExchange spider with search parameters.

        Args:
            laycan (Optional[str]): laycan period (in mm/dd/yyyy format)
            vessel_type (Optional[str]): must be either `tanker` or `dry bulk`
            batch_size (Optional[str]): batch size of charters to retrieve
        """
        if laycan:
            self.laycan = laycan.split(',')
        else:
            # default of one month period from scraping date onwards
            self.laycan = (
                (dt.datetime.utcnow() - dt.timedelta(days=0)).strftime('%m/%d/%Y'),
                (dt.datetime.utcnow() + dt.timedelta(days=30)).strftime('%m/%d/%Y'),
            )
        self.vessel_type = vessel_type or 'tanker'
        self.batch_size = batch_size or '10'

    @property
    def request_url(self):
        _form = {
            'orderBy': 'laycan_start',
            'orderDir': 'desc',
            'page': '1',
            'pageSize': self.batch_size,
            'cargo_size_min': '0',
            'cargo_size_max': '400',
            'charter_type': 'voyage',  # TODO 'tc' also available (timecharter)
            'dwt_min': '0',  # TODO expose as an arg ?
            'dwt_max': '500000',  # TODO expose as an arg ?
            'laycan_start': self.laycan[0],
            'laycan_end': self.laycan[1],
            'vessel_type': self.vessel_type,
            'yearBuilt_min': '1900',
            'yearBuilt_max': '3000',
        }
        return self.start_urls[0] + f'?{urlencode(_form)}'

    @property
    def headers(self):
        # init with mocked headers
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en;q=0.5',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Host': 'api-app.theofe.com',
            'Origin': 'https://app.theofe.com',
            'Pragma': 'no-cache',
            'Referer': 'https://app.theofe.com/fixtures',
            'User-Agent': random.choice(USER_AGENT_LIST),
        }
        if self.settings.get('CRAWLERA_ENABLED'):
            # support crawlera since we are scraping valuable, sensitive commercial data
            # crawlera will override/randomise some mocked headers above
            headers['X-Crawlera-Profile'] = 'desktop'

        return headers

    def start_requests(self):
        """Entrypoint for MarineTrafficRegistry spider.

        Yields:
            scrapy.Request:
        """
        self.logger.info(
            'Searching for charters with params:\n'
            f'laycan={self.laycan}\n'
            f'vessel_type={self.vessel_type}\n'
            f'batch_size={self.batch_size}'
        )
        yield Request(url=self.request_url, headers=self.headers, callback=self.parse_json_response)

    def parse_json_response(self, response):
        """Parse response as a JSON.

        Args:
            response (scrapy.Response):

        Yields:
            scrapy.Request:
        """
        res = json.loads(response.text)
        self._validate_json(res)
        self.logger.info(f'Retrieved {len(res["data"])} charters matching search parameters')

        for raw_item in res['data']:
            # enrich raw item with some meta info
            raw_item.update(provider_name=self.provider)
            yield normalize.process_item(raw_item)

    def _validate_json(self, res):
        """Validate number of vessels returned in overview page.

        OFE's unofficial endpoint for retrieving charters is limited to 10 at a time.
        By accessing their endpoint directly we can obtain more than 10 fixtures at any time.

        Args:
            res (Dict[str, str]):
        """
        if not isinstance(res.get('data'), list):
            self.logger.error('Unable to obtain relevant data, resource has likely changed.')
            raise CloseSpider('failed')

        if res.get('count', 0) > len(res['data']):
            self.logger.warning(
                f'Total quantity found ({res["count"]}) '
                f'exceeds retrieved quantity ({len(res["data"])}), '
                'you may want to increase batch size to avoid data loss'
            )
