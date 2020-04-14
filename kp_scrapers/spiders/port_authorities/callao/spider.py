import re
from urllib.parse import unquote

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta
from scrapy import Request

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.callao import normalize


# string pattern that will identify report to extract
REPORT_NAME = 'Listado'
# string pattern that will identify a table header
HEADER_INDICATOR = 'SHIPS'
# 0-based index of column in which vessel name is listed
VESSEL_COLUMN_INDEX = 3


class CallaoSpider(PortAuthoritySpider, PdfSpider):
    name = 'Callao'
    provider = 'Callao'
    version = '1.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.apmterminalscallao.com.pe/default.aspx?id=116&articulo=320']

    # pdf extraction options for java runtime
    tabula_options = {'--lattice': [], '--pages': ['all']}

    # decides if we have already incremented the month each row belongs to
    # this is required since there is only day info in the table, no year nor month, so we need to
    # use some tricks to enrich each row with such info (see `self._is_row_next_month`)
    _has_incremented_month = False
    _page_number = 0

    def parse(self, response):
        """Dispatch response to corresponding parsing function depending on URL.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            scrapy.Request:

        """
        relative_path = response.xpath(
            f'//p[@class="imagen-izquierda"]/a[contains(@href,"{REPORT_NAME}")]/@href'
        ).extract_first()

        yield Request(
            url=f'http://www.apmterminalscallao.com.pe/{relative_path}', callback=self.parse_pdf
        )

    def parse_pdf(self, response):
        """Parse and extract data from a pdf report.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:
        """
        # memoise reported date so it won't need to be called repeatedly below
        reported_date = self.extract_reported_date(response)

        table = self.extract_pdf_io(response.body, **self.tabula_options)
        for idx, row in enumerate(table):
            # extract headers to use for creating the json
            if HEADER_INDICATOR in str(row):
                header = row
                continue

            # get month and year associated with current vessel movement row
            month, year = self.get_enriched_date_info(row, reported_date)

            # discard useless rows without vessel names
            if len(row) <= VESSEL_COLUMN_INDEX or not row[VESSEL_COLUMN_INDEX]:
                continue

            if not may_strip(''.join(row)):
                continue

            raw_item = {head: row[head_idx] for head_idx, head in enumerate(header)}
            # contextualise raw item with some meta info
            raw_item.update(
                month=month,
                port_name=self.name,
                provider_name=self.provider,
                reported_date=reported_date,
                year=year,
            )
            yield normalize.process_item(raw_item)

    def get_enriched_date_info(self, row, reported_date):
        """Get enriched month and year info associated with a row's vessel movement.

        This function is basically required due to the way the source structures their table.
        Each row does not contain any info regarding the month nor year in which the event occured.

        We need to enrich it ourselves through looking at the table structure.
        The table is structured such that rollover months will always be preceded first by empty
        rows.

        Args:
            row (List[str]):
            reported_date (str):

        Returns:
            Tuple[int, int]:
        """
        _enriched_date = parse_date(reported_date, dayfirst=False)
        if self._is_row_next_month(row):
            _enriched_date += relativedelta(months=1)

        return _enriched_date.month, _enriched_date.year

    def _is_row_next_month(self, row):
        """Check if table row contains info dated for the next month.

        Args:
            row (List[str]):

        Returns:
            bool: True if we are in the table section that is the next month's
        """
        # reset boolean when we are no longer on the same page
        # each page will not be a continuation of the previous
        if 'LISTADO DE NAVES APMT CALLAO' in str(row):
            self._page_number += 1
            self._has_incremented_month = False

        # third page of report will always be about the next month
        if self._page_number == 3:
            self._has_incremented_month = True

        # sometimes the current month's page may have next month' vessels in it
        if not self._has_incremented_month and all(not cell for cell in row):
            self._has_incremented_month = True

        return self._has_incremented_month

    @staticmethod
    def extract_reported_date(response):
        """Extract reported date from response.

        Source will provide reported date in the URL itself, so we can simply extract it as is.

        Given the following URL:
            - "https://.../images/Trafico/Listado%20de%20Naves%2030.08.2018%20(3).pdf"

        Then, the raw reported date extracted:
            - "Listado de Naves 30.08.2018 (3).pdf"

        Finally, the ISO-8601 compatible string:
            - "2018-08-30T00:00:00"

        Args:
            response (scrapy.Response):

        Returns:
            str: ISO-8601 compatible string
        """
        raw_date = unquote(response.url.split('/')[-1])
        match = re.search(r'(\d{2}\.\d{2}\.\d{4})', raw_date)
        if match:
            return to_isoformat(match.group(1), dayfirst=True)
        else:
            raise ValueError(f'Unknown reported date format: {raw_date}')
