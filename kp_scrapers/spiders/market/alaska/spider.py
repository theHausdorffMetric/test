import datetime as dt

from scrapy import Request, Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.market_figure import BalanceType, MarketFigure
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.market import MarketSpider
from kp_scrapers.spiders.market.alaska import parser


class AlaskaMarketFigures(MarketSpider, Spider):
    name = 'AlaskaInventories'
    provider = 'Alaska Department of Revenue'
    version = '1.0.2'
    produces = [DataTypes.MarketFigure]

    start_urls = ['http://www.tax.alaska.gov/programs/oil/production/ans.aspx?{param}']

    def __init__(self, start_date=None, end_date=None, *args, **kwargs):
        """AlaskaMarketFigure class constructor.

        date format should be mm/dd/yyyy

        """
        super().__init__(*args, **kwargs)

        self._start_date = start_date if start_date else parser.get_first_day_of_current_month()
        self._end_date = end_date if end_date else parser.get_first_day_of_current_month()

        # memoise reported_date so it won't need to be called repeatedly later on
        self.reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

    def start_requests(self):
        """Entrypoint of Alaska Market Figure spider.

        Default behaviour is to always scrape the most recent series datapoints from API.
        If a start/end date is provided, it is reasonable to assume we want to scrape the entire
        date range, hence we do not want to restrict the quantitiy of returned datapoints.

        Yields:
            scrapy.Request:
        """

        # customise additional query params depending on given spider args
        for request_date in parser.get_date_range(self._start_date, self._end_date):
            url = self.start_urls[0].format(param=request_date)
            yield Request(url=url, callback=self.parse_response)

    @validate_item(MarketFigure, normalize=True, strict=True, log_level='error')
    def parse_response(self, response):
        """Parse response from Alaska website.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:
        """
        # denotes if record should be processed when iterating over each table srow
        PROCESS_FLAG = False

        tables = response.xpath('(//table[@id="ContentPlaceHolder1_Table1"])')
        for raw_row in tables.xpath('//tr'):
            row = [
                may_strip(td.xpath('.//text()').extract_first()) for td in raw_row.xpath('.//td')
            ]

            # According to the source, the number colums in the table is 8
            if len(row) < 8:
                continue

            # To indicate the start of the first data row in the table
            if row[0] == 'Date':
                PROCESS_FLAG = True
                continue

            # To indicate the end of the table
            elif row[0] == 'Average':
                PROCESS_FLAG = False
                continue

            if not PROCESS_FLAG:
                continue

            yield {
                # 7 denotes the position of the volume in the table's row.
                'volume': int(row[7].replace(',', '')),
                'country': 'Alaska',
                'country_type': 'region',
                'start_date': parser.parse_input_date(row[0]),  # is inclusive
                'end_date': parser.parse_input_date(row[0], days=1),  # is exclusive
                'unit': Unit.barrel,
                'provider_name': self.provider,
                'reported_date': self.reported_date,
                'balance': BalanceType.ending_stocks,
                'product': 'Crude Oil',
            }
