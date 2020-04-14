import datetime as dt
import re

from scrapy import Spider
from scrapy.exceptions import CloseSpider
from w3lib.html import remove_tags

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.isa import normalize


class ISASpider(PortAuthoritySpider, Spider):
    name = 'ISA'
    provider = 'ISA'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    def __init__(self, year=None, month=None):
        if not year and month:
            self.logger.error(f'`month` cannot be specified without specifying `year`')
            raise CloseSpider('cancelled')

        year = dt.datetime.utcnow().year if not year else year
        month = dt.datetime.utcnow().month if not month else month

        # scrape whole month depending on argument provided
        self.start_urls = []
        for day in range(1, 32):
            self._build_start_urls(day, month, year)

    def parse(self, response):
        """Entrypoint of ISA spider.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:
        """
        if self._is_response_empty(response):
            self.logger.info(f'No data from {response.url}')
            return

        reported_date = self._get_reported_date(response.url)
        header = response.xpath('//thead//th/text()').extract()
        table = response.xpath('//tbody/tr')
        for row in table:
            # remove html tags
            # we do this instead of using xpath/css selectors since the selector does not
            # return empty strings (we need empty strings to map to a dict's key)
            row = [remove_tags(cell) for cell in row.xpath('./td').extract()]

            raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
            # contextualise raw item with some meta info
            raw_item.update(provider_name=self.provider, reported_date=reported_date)
            yield normalize.process_item(raw_item)

    def _build_start_urls(self, day, month, year):
        self.start_urls.append(
            'http://www.isa-agents.com.ar/line_up.php?lang=en'
            f'&select_day={day}'
            f'&select_month={month}'
            f'&select_year={year}'
        )

    @staticmethod
    def _is_response_empty(response):
        return 'No hay resultados disponibles' in response.body_as_unicode()

    @staticmethod
    def _get_reported_date(url):
        """Extract reported date of response.

        Response URL should be of the following format:
            - http://www.isa-agents.com.ar/line_up.php?lang=en \
              &select_day=<DAY> \
              &select_month=<MONTH> \
              &select_year=<YEAR>

        Args:
            response (scrapy.Response):

        Returns:
            str: ISO-8601 formatted date string
        """
        _match = re.search(r'select_day=(\d+)&select_month=(\d+)&select_year=(\d+)', url)
        if not _match:
            raise ValueError(f'Unable to extract reported date from URL:\n{url}')

        return to_isoformat(' '.join(_match.groups()), dayfirst=True)
