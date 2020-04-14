from urllib.parse import urlparse

from scrapy import Request, Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.market_figure import BalanceType, MarketFigure
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.market import MarketSpider
from kp_scrapers.spiders.market.paj import parser


ALLOWED_BALANCE = ['1.Refinery Operations', '3.Crude, Unfinished Oil and Feed Stocks']

PRODUCT_MAPPING = {'Crude Input(KL)': 'Crude Oil'}
COUNTRY_MAPPING = {'ALL Japan': 'Japan'}
TABLE_HEADER = ['product', 'current_week', 'prev_week', 'change']
TABLE_INFO = ['product', 'volume_current', 'volume_prev', 'change']


class PAJMarketFigures(MarketSpider, Spider):
    name = 'PAJMarketFigure'
    provider = 'PAJMF'
    version = '1.0.0'
    produces = [DataTypes.MarketFigure]

    start_urls = ['https://stats.paj.gr.jp/en/pub/index.php', 'https://stats.paj.gr.jp/en/pub/{}']

    def start_requests(self):
        """Get landing page.

        Yields:
            scrapy.Request:

        """
        yield Request(url=self.start_urls[0], callback=self.parse)

    def parse(self, response):
        """Request dynamic URL containing port report within home page.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        header = {'Referer': response.url, 'host': urlparse(response.url).hostname}
        yield Request(
            url=self.start_urls[1].format('index.html'), headers=header, callback=self.parse_links
        )

    def parse_links(self, response):
        """ Parse the intermediate page to get the actual report link
        """
        for link in response.xpath('//a'):
            if 'current' in link.xpath('./@href').extract_first():
                link = link.xpath('./@href').extract_first()
                header = {'Referer': response.url, 'host': urlparse(response.url).hostname}
                yield Request(
                    url=self.start_urls[1].format(link.replace('./', '')),
                    callback=self.process_report_page,
                    headers=header,
                )

    @validate_item(MarketFigure, normalize=True, strict=True, log_level='error')
    def process_report_page(self, response):
        """Process the main report page
        """
        PROCESSING_STARTED = False
        balance = None
        country, country_type = parser.get_country(response)
        # Each row in the table represents a information on different balance type
        # (refinery intake, ending stock) for a speicific period of time
        for row in response.xpath('//tr'):
            for col in row.xpath('./td'):
                if col.xpath('./u/text()').extract_first() in ALLOWED_BALANCE:
                    PROCESSING_STARTED = True
                    balance = col.xpath('./u/text()').extract_first()

                if PROCESSING_STARTED:
                    table = col.xpath('./table')
                    if not table:
                        continue

                    trows = table.xpath('./tr')

                    # we are interested only in the first 2 rows
                    mapped_titles = row_to_dict(trows[0], TABLE_HEADER)
                    mapped_values = row_to_dict(trows[1], TABLE_INFO)

                    # for current week items
                    raw_item = map_keys(mapped_titles, self.current_week_mapping())
                    raw_item.update(map_keys(mapped_values, self.current_week_mapping()))

                    raw_item.update(
                        {
                            'reported_date': parser.get_reported_date(response),
                            'provider_name': self.provider,
                            'unit': Unit.kiloliter,
                            'country': COUNTRY_MAPPING.get(country, country),
                            'country_type': country_type,
                            'balance': self._infer_balance(balance),
                        }
                    )

                    # Dispatch for the current week
                    start_of_current_week, end_of_current_week = parser.parse_date_range(
                        raw_item.pop('current_week', None)
                    )

                    raw_item.update(
                        {
                            'start_date': start_of_current_week,
                            'end_date': end_of_current_week,
                            'volume': raw_item.pop('volume_current', None),
                        }
                    )

                    yield raw_item

                    # for previous week
                    raw_item.update(map_keys(mapped_titles, self.previous_week_mapping()))
                    raw_item.update(map_keys(mapped_values, self.previous_week_mapping()))

                    # Dispatch for the Previous week
                    start_of_prev_week, end_of_prev_week = parser.parse_date_range(
                        raw_item.pop('prev_week', None)
                    )
                    raw_item.update(
                        {
                            'start_date': start_of_prev_week,
                            'end_date': end_of_prev_week,
                            'volume': raw_item.pop('volume_prev', None),
                        }
                    )

                    yield raw_item

                    PROCESSING_STARTED = False

    def _infer_balance(self, balance):
        """ infer the type of balance
        """
        if balance == '1.Refinery Operations':
            return BalanceType.refinery_intake
        elif balance == '3.Crude, Unfinished Oil and Feed Stocks':
            return BalanceType.ending_stocks

        return None

    def current_week_mapping(self):
        return {
            'product': ('product', lambda x: PRODUCT_MAPPING.get(x, x)),
            'volume_current': ('volume_current', lambda x: float(x.replace(',', ''))),
            'current_week': ('current_week', None),
            'volume_prev': ignore_key('volume_prev'),
            'prev_week': ignore_key('prev_week'),
        }

    def previous_week_mapping(self):
        return {
            'product': ('product', lambda x: PRODUCT_MAPPING.get(x, x)),
            'volume_prev': ('volume_prev', lambda x: float(x.replace(',', ''))),
            'prev_week': ('prev_week', None),
        }
