import datetime as dt

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.nigeria import normalize


class NigeriaSpider(PortAuthoritySpider, PdfSpider):
    name = 'Nigeria'
    provider = 'NigeriaPorts'
    version = '3.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://shippos.nigerianports.gov.ng/']

    port_name = None

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
        # there are two port reports provided:
        #   - expected vessels
        #   - current in-port activity
        reported_date = dt.datetime.utcnow().isoformat()
        status_ids = ['berthed', 'expected', 'awaiting']
        for status in status_ids:
            for table in response.xpath(
                "//div[@id='" + status + "']//div[@role='tabpanel']//table"
            ):
                port_id = table.attrib['id']
                header = response.xpath(
                    "//div[@id='"
                    + status
                    + "']//div[@role='tabpanel']//table[@id='"
                    + port_id
                    + "']//th/text()"
                ).extract()
                for row in table.xpath(
                    "//div[@id='"
                    + status
                    + "']//div[@role='tabpanel']//table[@id='"
                    + port_id
                    + "']//tr"
                ):
                    raw_item = row_to_dict(row, header)
                    raw_item.update(
                        port_name=port_id, provider_name=self.provider, reported_date=reported_date
                    )
                    yield normalize.process_item(raw_item)
