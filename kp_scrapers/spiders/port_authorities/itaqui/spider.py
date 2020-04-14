from scrapy import Request

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_apply
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.itaqui import normalize, parser


class ItaquiSpider(PortAuthoritySpider, PdfSpider):
    name = 'Itaqui'
    provider = 'ItaquiPorts'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    start_urls = ['http://www.portodoitaqui.ma.gov.br/']

    port_name = 'Sao Luis'

    pdf_parsing_options = {
        # extract all pages
        '--pages': ['all'],
        # lattice-mode extraction (more reliable if there are ruling lines between cells)
        '--lattice': [],
    }

    def parse(self, response):
        """Parse overview website and obtain URLs for the individual PDF reports.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        pdf_url = response.xpath("//a[@id='link-download-mapa']/@href").extract()
        pdf_url = 'http://www.portodoitaqui.ma.gov.br' + pdf_url[0]
        # scan reported date through url
        reported_date = pdf_url.split('- ')[1].split(' clientes')[0]
        day, month, year = (
            reported_date.split(' ')[0],
            reported_date.split(' ')[1],
            reported_date.split(' ')[2],
        )
        self.reported_date = may_apply(f'{day}/{month}/{year}', to_isoformat)

        yield Request(url=pdf_url, callback=self.parse_pdf_report)

    def parse_pdf_report(self, response):
        raw_table = self.extract_pdf_table(
            response, parser._parse_pdf, **parser.TABULA_OPTIONS_FIND
        )
        for raw_item in raw_table:
            raw_item.update(
                port_name=self.name, reported_date=self.reported_date, provider_name=self.provider
            )
            yield from normalize.process_item(raw_item)
