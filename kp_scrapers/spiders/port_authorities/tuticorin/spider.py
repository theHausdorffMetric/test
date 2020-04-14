from scrapy import Request

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.tuticorin import normalize


HEADER_SIGN = 'VESSEL NAME'
SPECIAL_COLUMN_IDX = 0


class TuticorinSpider(PortAuthoritySpider, PdfSpider):
    name = 'Tuticorin'
    provider = 'Tuticorin'
    version = '1.1.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.vocport.gov.in/DailyVesselPosition.aspx']

    tabula_options = {'--pages': ['all'], '--lattice': []}

    def parse(self, response):
        """Landing page.

        Args:
            response (Response):

        Yields:
            Request:

        """
        # cache reported date first
        self.reported_date = response.xpath(
            '//li[@class="last_modified"]/span/text()'
        ).extract_first()

        # url list contains vessels waiting at anchorage and vessels expected
        url_list = response.xpath(
            '//div[@id="PlaceHolder_Content_WebsiteContent"]//iframe/@src'
        ).extract()

        for url in url_list:
            yield Request(url=url, callback=self.parse_frame)

    def parse_frame(self, response):
        headers = None
        valid_row_index = None
        _cached = None

        for raw_row in self.extract_pdf_io(response.body, **self.tabula_options):
            row = [may_strip(cell).upper() for cell in raw_row]

            # extract headers
            if HEADER_SIGN in row:
                headers = row
                valid_row_index = row.index(HEADER_SIGN)
                continue

            # sanity check; in case headers are never found, or row is incomplete
            if headers and len(row) > valid_row_index and row[valid_row_index]:
                # handle special columns
                # in some tables, this special column may be empty
                # if empty, it means the value is the same as the previous record's columns
                if row[SPECIAL_COLUMN_IDX]:
                    _cached = row[SPECIAL_COLUMN_IDX]
                else:
                    row[SPECIAL_COLUMN_IDX] = _cached

                raw_item = {headers[idx]: cell for idx, cell in enumerate(row)}
                # contextualise raw item with meta info
                raw_item.update(
                    port_name=self.name,
                    provider_name=self.provider,
                    reported_date=self.reported_date,
                )

                yield normalize.process_item(raw_item)
