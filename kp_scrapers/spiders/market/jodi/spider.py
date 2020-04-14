"""Module for JODI market spider.

Source: http://www.jodidb.org/ReportFolders/reportFolders.aspx?sCS_referer=&sCS_ChosenLang=en

JODI (Joint Organisations Data Initiative) provides key market figures for the quantity of
commodities flowing in/out of a country.


Arguments
~~~~~~~~~

JODI provides customised metrics for the aforementioned figures. Here, we list all possible
types of market figures that can be obtained from JODI:

    report_type:
        - 0: "Joint Organisations Data Initiative – Primary (all data)"
        - 1: "Joint Organisations Data Initiative – Secondary (all data)"

    unit:
        - 0: Thousand Barrels per day (kb/d)
        - 1: Thousand Barrels (kbbl)
        - 2: Thousand Kilolitres (kl)
        - 3: Thousand Metric Tons (kmt)
        - 4: Conversion factor barrels/ktons

    product:
        primary:
            - 0: Crude oil
            - 1: NGL
            - 3: Other
            - 4: Total

        secondary:
            - 0: Liquefied petroleum gases
            - 1: Naphtha
            - 2: Motor and aviation gasoline
            - 3: Kerosenes
            - 4: of which: kerosene type jet fuel
            - 5: Gas/diesel oil
            - 6: Fuel oil
            - 7: Other oil products
            - 8: Total oil products

    balance:
        primary:
            - 0: Production
            - 1: From other sources
            - 2: Imports
            - 3: Exports
            - 4: Products transferred/Backflows
            - 5: Direct use
            - 6: Stock change
            - 7: Statistical difference
            - 8: Refinery intake
            - 9: Closing stocks

        secondary
            - 0: Refinery output
            - 1: Receipts
            - 2: Imports
            - 3: Exports
            - 4: Products transferred
            - 5: Interproduct transfers
            - 6: Stock change
            - 7: Statistical difference
            - 8: Demand
            - 9: Closing stocks

"""
import datetime as dt

from scrapy import FormRequest, Request, Spider

from kp_scrapers.lib.errors import InvalidCliRun
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.models.market_figure import MarketFigure
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.market import MarketSpider
from kp_scrapers.spiders.market.jodi import api, parser


class JodiSpider(MarketSpider, Spider):
    name = 'Jodi'
    provider = 'JODI'
    version = '1.4.2'
    produces = [DataTypes.MarketFigure]

    start_urls = [
        # landing page
        'http://www.jodidb.org/ReportFolders/reportFolders.aspx?sCS_referer=&sCS_ChosenLang=en',
        # unofficial api
        'http://www.jodidb.org/TableViewer/getData.aspx?row=1&col=1&rowCount=1000&colCount=1000',
    ]

    def __init__(self, report_type, unit, product, balance):
        """Instantiate Jodi spider.

        NOTE all arguments values should be one of the values exposed by the JODI "unofficial" api

        Args:
            report_type (str):
            unit (str):
            product (str):
            balance (str):

        """
        self._validate_cli_arg('report_type', report_type, parser.REPORT_TYPE)
        self._validate_cli_arg('unit', unit, parser.UNIT)
        self._validate_cli_arg('product', product, parser.PRODUCT[self.report_type])
        self._validate_cli_arg('balance', balance, parser.BALANCE[self.report_type])

    def _validate_cli_arg(self, key, value, mapping):
        setattr(self, key, value)
        if not mapping.get(getattr(self, key, None)):
            raise InvalidCliRun(key, value)

    def start_requests(self):
        """Get landing page.

        Yields:
            scrapy.Request:

        """
        yield Request(url=self.start_urls[0], callback=self.send_api_request)

    def send_api_request(self, response):
        """Send form to obtain market figures for specified report_type/product/unit/balance.

        Yields:
            scrapy.FormRequest:

        """
        formdata = api.get_form(
            response.text, self.report_type, self.unit, self.product, self.balance
        )

        yield FormRequest(
            self.start_urls[1], formdata=formdata, callback=self.parse, dont_filter=True
        )

    @validate_item(MarketFigure, normalize=True, strict=True, log_level='error')
    def parse(self, response):
        """Parse table.

        Row represents country name, columns are time period, and cells are volume, for example:

        --------------------------------------------------------------
        | Time    | Jan2002 | Feb2002 | Mar2002 |   ...    | Oct2019 |
        | Country |         |         |         |          |         |
        --------------------------------------------------------------
        | Algeria |   699   |   699   |   699   |   ...    |   699   |
        --------------------------------------------------------------
        | ...     |   ...   |   ...   |   ...   |   ...    |   ...   |
        --------------------------------------------------------------
        | Yemen   |   428   |   427   |   425   |   ...    |   425   |
        --------------------------------------------------------------

        Args:
            response:

        Returns:
            Dict[str, str]:

        """
        # get first row, not containing first col
        periods = response.xpath('//Headers/ColHeader/ColDim/ColLabels/ColLabel/text()').extract()

        # process each row, but first, get row length
        for row in response.xpath('//Rows/Row'):
            _country = row.xpath('.//RowLabels/RowLabel/text()').extract_first()
            country = parser.ZONE_MAPPING.get(_country, _country)
            # skip irrelevant countries
            if not country:
                continue

            cells = row.xpath('.//Cells/C/@f').extract()
            # sanity check; in case tabular data has different table length
            if len(periods) != len(cells):
                self.logger.error('Data format might have changed.')
                return

            # parse market quantity associated with each market period
            for idx, period in enumerate(periods):
                raw_item = {
                    'balance': parser.BALANCE[self.report_type][self.balance],
                    'country': country,
                    'country_type': parser.COUNTRY_TYPE_MAPPING.get(country, 'country'),
                    'product': parser.PRODUCT[self.report_type][self.product],
                    'provider_name': self.provider,
                    'reported_date': dt.datetime.utcnow()
                    .replace(hour=0, minute=0, second=0)
                    .isoformat(timespec='seconds'),
                    'unit': parser.UNIT[self.unit],
                }

                # if format is not valid then the srat_date and end_date won't be added
                # hence the model validation fails.
                if parser.is_valid_period(period):
                    raw_item.update(zip(('start_date', 'end_date'), parser.get_period(period)))
                else:
                    self.logger.error('Date format changed.')
                    continue

                _quantity = try_apply(cells[idx].replace(',', ''), float)
                # usually because of `N/A` or `x` quantities
                if _quantity is None:
                    continue

                # TODO no support of `kb/d` unit yet, until specs are clear
                if 'kb/d' in raw_item['unit']:
                    self.logger.warning('No support of `kb/d` unit yet')
                    continue

                if raw_item['unit'] == Unit.kilobarrel:
                    raw_item.update(volume=_quantity * 1000, unit=Unit.barrel)
                elif raw_item['unit'] == Unit.kiloliter:
                    raw_item.update(volume=_quantity * 1000, unit=Unit.liter)
                else:
                    raw_item.update(mass=_quantity * 1000, unit=Unit.tons)

                yield raw_item
