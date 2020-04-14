from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.mencar import normalize, parser


class MenCarSpider(parser.MenCarParser, PortAuthoritySpider, PdfSpider):
    name = 'MenCar'
    provider = 'Men-Car'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.men-car.com/out/01.pdf']

    # ports to extract portcall from pdf
    port_names = [
        'Barcelona',
        # 'Tarragona',  # FIXME deactivated for now, since cargo data is no longer provided
    ]

    def parse(self, response):
        """Dispatch response to corresponding parsing function depending on URL.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]:

        """
        # try and find page where port activity tables are in (will vary across days)
        if not self.init_port_activity_page(response):
            self.logger.error('Unable to detect page in which portcall table is present')
            return

        # memoise reported date so it does not have to be called for each row below
        # this attribute will be used in `parser.MenCarParser` methods
        self.reported_date = self.extract_reported_date(response)

        # there are multiple ports in the data source
        for port in self.port_names:
            table_rows, headers = self.extract_table_and_headers(response, port=port)
            for row in table_rows:
                raw_item = {header: row[idx] for idx, header in enumerate(headers)}
                raw_item.update(
                    port_name=port, reported_date=self.reported_date, provider_name=self.provider
                )
                yield normalize.process_item(raw_item)
