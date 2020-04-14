import datetime as dt

from dateutil.parser import parse as parse_date
from scrapy import Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.colombia import normalize
from kp_scrapers.spiders.port_authorities.colombia.session import ColombiaSession
from kp_scrapers.spiders.port_authorities.colombia.utils import (
    build_date_string,
    DEFAULT_DATE_FORMAT,
    RETRY_TIMES,
)


# how many days to lookbehind and lookahead for data
DEFAULT_START_OFFSET = -7
DEFAULT_END_OFFSET = 21


class ColombiaSpider(PortAuthoritySpider, Spider):
    name = 'Colombia'
    provider = 'Colombia'
    version = '2.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://app.dimar.mil.co/zonadescarga/Formularios/frmConsultaFletamento.aspx']

    spider_settings = {
        # required because source will return internal errors occasionally for unknown reasons
        'RETRY_TIMES': RETRY_TIMES
    }

    # memoise current date as reported date since source does not provide one
    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    )

    def __init__(self, date_range=None):
        """Initialize Colombia spider with vessel lineup search parameters.

        Args:
            date_range (str | None): range of dates during which vessel has an ETA
        """
        # default scraping options
        if not date_range:
            self.start_date = build_date_string(offset=DEFAULT_START_OFFSET)
            self.end_date = build_date_string(offset=DEFAULT_END_OFFSET)

        # scrape for a specific date range
        else:
            _start, _, _end = date_range.partition(',')
            if not _end:
                raise ValueError(f'`date_range` supplied has no end date: {date_range}')

            _time_delta = parse_date(_end, dayfirst=True) - parse_date(_start, dayfirst=True)
            if _time_delta > dt.timedelta(days=31):
                raise ValueError(f'`date_range` supplied cannot be > 1 month: {date_range}')

            self.start_date = parse_date(_start, dayfirst=True).strftime(DEFAULT_DATE_FORMAT)
            self.end_date = parse_date(_end, dayfirst=True).strftime(DEFAULT_DATE_FORMAT)

    def parse(self, response):
        """Entrypoint of Colombia spider.

        Args:
            response (scrapy.Response):

        Yields:
            scrapy.FormRequest:
        """
        self.logger.info(f'Searching for data: {self.start_date} -> {self.end_date}')
        # init a new session to handle ASP.NET states properly
        session = ColombiaSession(
            homepage=response,
            start_date=self.start_date,
            end_date=self.end_date,
            on_traversal=self.extract_table_data,
        )
        return session.traverse_all()

    def extract_table_data(self, response):
        """Extract table rows with valid data.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:
        """
        table = response.xpath('//table[@id="ctl00_ContentPlaceHolder1_dgdReporteFletamento"]//tr')
        for idx, row in enumerate(table):
            row = [may_strip(x) for x in row.xpath('.//text()').extract() if may_strip(x)]
            # header will always be first row of table
            if idx == 0:
                header = row
                continue

            # table ends with an empty list (row)
            if not row:
                break

            raw_item = {header[cell_idx]: cell for cell_idx, cell in enumerate(row)}
            # contextualise raw item with some meta info
            raw_item.update(provider_name=self.provider, reported_date=self.reported_date)
            yield normalize.process_item(raw_item)
