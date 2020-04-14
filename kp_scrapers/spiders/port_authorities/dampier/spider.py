import json

from scrapy import Request, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.dampier import normalize, utils


# different request bodies will contain different installation activity reports
REPORT_TYPES = {'4': 'in-port', '2': 'forecast'}


class DampierSpider(PortAuthoritySpider, Spider):
    name = 'Dampier'
    provider = 'Dampier'
    version = '1.4.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://kleinpod.pilbaraports.com.au/services/wxdata.svc/GetDataX']

    # memoise auth tokens
    _auth_tokens = {}

    @property
    def auth_url(self):
        return 'https://kleinpod.pilbaraports.com.au/'

    @property
    def auth_names(self):
        # list cookie names that are required to be authenticated, and to scrape the website
        return ['ASP.NET', 'incap_ses', 'visid_incap']

    def start_requests(self):
        """Entrypoint of Dampier spider.

        Yields:
            scrapy.Request:

        """
        yield Request(
            url=self.auth_url,
            callback=self.authenticate_and_scrape,
            method='GET',
            # NOTE this spider requires Splash to work around
            # the source's strict policies on bot scraping
            meta={'splash': {'args': {'html': 1, 'history': 1}}},
        )

    def authenticate_and_scrape(self, response):
        """Get authenticated session (required for scraping source data), then scrape.

        Args:
            scrapy.Response:

        Yields:

        """
        self._authenticate(response)
        for code, report in REPORT_TYPES.items():
            yield Request(
                url=self.start_urls[0],
                callback=self.parse_form_response,
                method='POST',
                body=utils.request_body(code),
                # NOTE source returns HTTP 500 for unknown reasons
                # if we send the cookies inside the headers
                headers=utils.headers(),
                cookies=self._auth_tokens,
                meta={'report_type': report},
            )

    def parse_form_response(self, response):
        """Parse json response.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        report_table = json.loads(response.body_as_unicode())['d']['Tables'][0]['Data']
        for row in report_table:
            # form response does not include table headers, so we use indexes themselves
            raw_item = {str(idx): cell for idx, cell in enumerate(row)}
            raw_item.update(
                data_type=response.meta['report_type'],
                port_name=self.name,
                provider_name=self.provider,
                reported_date=utils._get_dampier_time(),
            )
            yield from normalize.process_item(raw_item)

    def _authenticate(self, response):
        """Extract authentication data from initial response.

        Args:
            response (scrapy.Response):

        """
        # take final request since it will contain the. auth token given upon validation
        cookies = (
            response.data['history'][-1]['request']['cookies']
            + response.data['history'][-1]['response']['cookies']
        )
        for cookie in cookies:
            if any(token in cookie['name'] for token in self.auth_names):
                self._auth_tokens.update({cookie['name']: cookie['value']})

        self.logger.info(f'Obtained authenticated session:\n{self._auth_tokens}')
