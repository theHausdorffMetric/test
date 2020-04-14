from scrapy import FormRequest, Request

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.beira import normalize, parser


class BeiraSpider(PortAuthoritySpider, PdfSpider):
    name = 'Beira'
    provider = 'Beira'
    version = '1.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.exaf.eu/exaf/actus.php']

    tabula_options = {
        '--pages': ['all'],  # extract all pages
        '--lattice': [],  # when pdf table has cell borders
    }

    reported_date = None

    def start_requests(self):
        """Beira spider entry point.

        Website expects one to supply the country ("pays") and the port ("port").
        Note that Tema (536) is a port within Ghana (45).

        This function will get the HTML page containing the link to the Tema port spreadsheet.

        Yields:
            scrapy.FormRequest:

        """
        yield FormRequest(
            url=self.start_urls[0],
            # `pays` is country (here it is Ghana), `port` 536 is Tema
            formdata={'pays': '45', 'port': '536'},
            callback=self.get_pdf,
        )

    def get_pdf(self, response):
        """Gets pdf from the HTML port page.

        Args:
            response (scrapy.Response):

        Returns:
            scrapy.Request:

        """
        yield Request(
            url='http://www.exaf.eu' + response.xpath('//div[@id="result"]//@href').extract_first(),
            callback=self.parse_pdf,
        )

    def parse_pdf(self, response):
        """Parse pdf table and yield items.

        Args:
            response (Response):

        Returns:
            PortCall | None:

        """
        table = self.extract_pdf_table(response, lambda x: x, **self.tabula_options)
        # first row of first cell always contains reported date
        self.reported_date = parser.parse_reported_date(next(elem for elem in table[0] if elem))

        for raw_item in parser.parse_table(table[1:]):
            raw_item.update(self.meta_field)

            yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {
            'port_name': self.name,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
