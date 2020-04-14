from datetime import datetime as dt
from urllib.parse import parse_qs, urlparse

from scrapy import Request, Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.pajaritos import normalize, parser


# pajaritos has 2 tabs, arrival and sheet 1
TABS = ['001', '002']

HEADERS = [
    'NOMBRE DEL BUQUE',
    'BANDERA',
    'T.R.B.',
    'ESLORA',
    'PIES',
    'FECHA',
    'HORA',
    'ORIGEN',
    'AGENTE NAVIERO',
    'CARGA',
    'DESCARGA',
    'PRODUCTO',
    'ASIGNADO',
]


class PajaritosPDFSpider(PortAuthoritySpider, PdfSpider):
    name = 'PajaritosPDF'
    provider = 'Pajaritos'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://www.puertocoatzacoalcos.com.mx/programacion-de-buques']

    tabula_options = {'--lattice': [], '--guess': [], '--stream': []}

    def parse(self, response):
        res_url = response.xpath('//iframe/@src').extract_first()
        domain = urlparse(response.url)

        pdf_web_link = "http://" + domain.hostname + res_url

        link = parse_qs(urlparse(pdf_web_link).query)
        final_link = link['file']

        yield Request(
            url="http://" + domain.hostname + final_link[0], callback=self.get_pdf_document
        )

    def get_pdf_document(self, response):
        table = self.extract_pdf_table(response, lambda x: x, **self.tabula_options)
        for idx, row in enumerate(table):
            # The valid rows start after 3, hence 3
            if idx <= 3:
                continue

            if len(row) and len(HEADERS):
                raw_item = dict(zip(HEADERS, row))

                raw_item.update(
                    {
                        'reported_date': dt.now().isoformat(),
                        'provider_name': self.provider,
                        'port_name': 'Coatzacoalcos',
                    }
                )

                yield normalize.process_item(raw_item)


class PajaritosSpider(PortAuthoritySpider, Spider):
    name = 'Pajaritos'
    provider = 'Pajaritos'
    version = '1.1.6'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://www.puertocoatzacoalcos.com.mx/programacion-buques']

    def parse(self, response):
        """Request dynamic URL containing port report within home page.

        Args:
            response (scrapy.Response):

        Yields:
            scrapy.Request: request for dynamic port activity page

        """
        for tab in TABS:
            yield Request(
                url=parser.get_report_link(response, tab),
                callback=self.parse_report_table,
                meta={'reported_date': parser.extract_reported_date(response)},
            )

    def parse_report_table(self, response):
        """Parse the port activity table rows in the dynamic URL.

        Args:
            response (scrapy.Response):

        Yields:
            raw_item (Dict[str, str]):

        """
        table_rows, headers = parser.extract_table_and_headers(response)
        for idx, row in enumerate(table_rows):
            raw_item = row_to_dict(
                row,
                headers,
                reported_date=response.meta['reported_date'],
                port_name='Coatzacoalcos',
                provider_name=self.provider,
            )
            yield normalize.process_item(raw_item)
