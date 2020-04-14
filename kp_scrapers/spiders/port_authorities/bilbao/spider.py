import datetime as dt

from scrapy import Request

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.bilbao import normalize, parser


class BilbaoSpider(PortAuthoritySpider, PdfSpider):
    name = 'Bilbao'
    provider = 'Bilbao'
    version = '1.2.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://www.bilbaoport.eus/boletin/{report_date}_referencia/boletin_EN.pdf']

    def __init__(self, report_date=dt.datetime.utcnow().strftime('%y%m%d')):
        """Init Bilbao spider.

        Args:
            report_date (str): report date to retrieve PDF

        """
        super().__init__()
        self._url_date = report_date
        self.reported_date = dt.datetime.strptime(report_date, '%y%m%d').isoformat()

    def start_requests(self):
        """Request PDF with the specified report date.

        Yields:
            scrapy.Request:

        """
        yield Request(
            url=self.start_urls[0].format(report_date=self._url_date),
            callback=self.parse_pdf_report,
        )

    def parse_pdf_report(self, response):
        """Parse and extract data from the pdf tables.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]: raw item

        """
        # extract table rows from pdf report
        for idx, row in enumerate(
            self.extract_pdf_io(
                body=response.body, preprocessor=parser._preprocess_table, **parser.TABLE_OPTIONS
            )
        ):
            # first row is always the table header
            if idx == 0:
                header = row
                continue

            raw_item = {head: row[col_idx] for col_idx, head in enumerate(header)}
            # contextualise raw item with meta info
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=self.reported_date
            )

            yield normalize.process_item(raw_item)
