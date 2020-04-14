from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.djibouti import normalize


class DjiboutiSpider(PortAuthoritySpider, PdfSpider):
    name = 'Djibouti'
    provider = 'Djibouti'
    version = '2.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.portdedjibouti.com/category/vessel-schedule/']

    tabula_options = {'--guess': [], '--pages': ['all'], '--lattice': []}

    def parse(self, response):
        """Get latest vessel schedule page link.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]:

        """
        # first link contains latest report link
        url = response.css('h2.post-title a::attr(href)').extract_first()
        yield response.follow(url=url, callback=self.get_vessel_schedule_pdf)

    def get_vessel_schedule_pdf(self, response):
        """Get vessel schedule pdf report link.

        Args:
            response (scrapy.HtmlResponse):

        Yields:

        """
        self.reported_date = (
            response.xpath('//h1[@class="entry-title post-title responsive"]//text()')
            .extract_first()
            .split()[-1]
        )

        url = response.css('a.gde-link::attr(href)').extract_first()
        yield response.follow(url, self.parse_pdf)

    def parse_pdf(self, response):
        """Parse vessel schedule pdf file.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            EtaEvent:

        """
        for row in self.extract_pdf_io(response.body, **self.tabula_options):
            # skip rows of all empty cells
            if all(not cell for cell in row):
                continue

            # skip rows without vessel name
            if len(row) <= 1 or not row[1]:
                continue

            # memoise table headers
            if row[1] == 'SHIP NAME':
                header = row
                continue

            raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
            # contextualise raw item with meta inof
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=self.reported_date
            )
            yield normalize.process_item(raw_item)
