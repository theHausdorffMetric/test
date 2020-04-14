from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.topolobampo import normalize, parser


class TopolobampoSpider(PortAuthoritySpider, Spider):
    name = 'Topolobampo'
    provider = 'Topolobampo'
    version = '1.2.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://www.puertotopolobampo.com.mx/archivos/programacion.php']

    reported_date = None

    def parse(self, response):
        """Dispatch response to corresponding callback given URL.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            dict[str, str]:

        """
        table, headers = parser.extract_table_and_headers(response)
        # memoise reported_date so it won't have to be called repeatedly for each row
        reported_date = parser.extract_reported_date(response)

        for row in parser.extract_rows_from_table(table):
            if len(row.xpath('.//td')) == len(headers):
                raw_item = row_to_dict(row, headers)
                # contextualise raw item with meta info
                raw_item.update(
                    port_name=self.name, provider_name=self.provider, reported_date=reported_date
                )

                yield normalize.process_item(raw_item)
