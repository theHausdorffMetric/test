import re

from scrapy import Request

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.chittagong import normalize, parser


class ChittagongSpider(PortAuthoritySpider, PdfSpider):
    name = 'Chittagong'
    provider = 'Chittagong'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://cpa.gov.bd/site/view/commondoc/Berthing%20Schedule']

    def __init__(self, history=False, number_pages_history=10, **kwargs):
        """Init SouthKorea spider search filters.

        Args:
            history (bool): parse history or not
            number_pages_history (int): number of historic pages to parse (all history is big)
        """
        super().__init__(**kwargs)
        self.history = history
        self.number_pages_history = number_pages_history

    def parse(self, response):
        """Find latest port activity report URL from main page.

        There are two PDFs that need to be downloaded here

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            scrapy.Request:

        """
        first_two = 0
        for url in response.xpath(
            '//div[@id="printable_area"]//tbody/tr/td[last()]/a/@href'
        ).extract():
            if first_two < 2 or self.history:
                first_two = first_two + 1
                url = 'http:' + url
                yield Request(url=url, callback=self.parse_pdf_report)

        # traverse next page if any
        base_url = 'http://cpa.gov.bd/'
        if self.history:
            pagination = response.xpath('//ul[@class="pagination"]/li/a').extract()
            last_page_number = pagination[3].split('?page=')[1].split('&amp')[0]
            if self.number_pages_history is not None:
                last_page_number = self.number_pages_history
            next_page = base_url + pagination[2].split('a href="')[1].split('">&gt')[0]
            next_page_number = pagination[2].split('?page=')[1].split('&amp')[0]
            if int(next_page_number) <= int(last_page_number):
                yield Request(next_page, callback=self.parse)

    def parse_pdf_report(self, response):
        document_type = self.extract_pdf_table(
            response, parser._find_document_type, **parser.TABULA_OPTIONS_BERTHED
        )
        if document_type == ['JETTY NO.']:
            status = 'berthing'
            table = self.extract_pdf_table(
                response, parser._process_berthed_rows, **parser.TABULA_OPTIONS_BERTHED
            )
        elif document_type == ['SL. NO.']:
            status = 'vessel_arriving'
            table = self.extract_pdf_table(
                response, parser._process_eta_rows, **parser.TABULA_OPTIONS_ETA
            )
        else:
            raise ValueError('Unsupported PDF report: {}'.format(response.url))

        # will always be either 'berthed' or 'eta' in this case
        event_type = table[0][-1]
        reported_date = self.get_reported_date(response, event_type)
        headers, data_rows, arrived_rows = parser.extract_row_headers(table, status)
        for row in data_rows:
            raw_item = {
                re.sub(r'([\W])', '', head): row[idx] for idx, head in enumerate(headers[0])
            }
            raw_item.update(
                port_name=self.name, reported_date=reported_date, provider_name=self.provider
            )
            yield normalize.process_item(raw_item, event_type)

        # only for "VESSEL - <date>.pdf"
        event_type = 'arrived'
        for row in arrived_rows:
            raw_item = {
                re.sub(r'([\W])', '', head): row[idx] for idx, head in enumerate(headers[1])
            }
            raw_item.update(
                port_name=self.name, reported_date=reported_date, provider_name=self.provider
            )
            yield normalize.process_item(raw_item, event_type)

    def get_reported_date(self, response, event_type):
        """Extract reported date from a specific area on the 1st page

        Args:
            response (scrapy.HtmlResponse):
            event_type (str): used to filter type of pdf

        Returns:
            str: reported date in ISO-8601 compatible format

        """
        if event_type == 'berthed':
            date_area = self.extract_pdf_table(
                response, information_parser=None, **parser.BERTHED_DATE_AREA
            )
            date = [
                str(line).replace(' ', '').split('DATED:')
                for line in date_area
                if 'DATED' and 'VESSELS' in str(line)
            ]
            return to_isoformat(date[0][1][:10])

        elif event_type == 'eta':
            date_area = self.extract_pdf_table(
                response, information_parser=None, **parser.ETA_DATE_AREA
            )
            date = [str(line).split(':') for line in date_area if 'DATED' in str(line)]
            return to_isoformat(date[0][1])
