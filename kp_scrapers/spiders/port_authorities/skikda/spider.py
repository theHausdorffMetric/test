from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.skikda import normalize


class SkikdaSpider(PortAuthoritySpider, Spider):
    name = 'Skikda'
    provider = 'Skikda'
    version = '1.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'http://www.skikda-port.com/navire-attendus/',  # expected
        'http://www.skikda-port.com/navire-en-rade/',  # arrived
        'http://www.skikda-port.com/navire-en-quai/',  # berthed
    ]

    def parse(self, response):
        """Entrypoint of Skikda spider.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:
        """
        # memoise reported date so it won't need to be called repeatedly later
        reported_date = response.xpath('//div[@id="full"]/h3/text()').extract_first()

        header, table = self.extract_table_and_header(response)
        for row in table:
            raw_item = row_to_dict(row, header)
            # let's add a bit of metadata
            raw_item.update(
                port_name=self.name,
                provider_name=self.provider,
                reported_date=reported_date,
                url=response.url,
            )
            yield normalize.process_item(raw_item)

    @staticmethod
    def extract_table_and_header(response):
        """Extract table contents from the given response.

        Args:
            response (scrapy.Response):

        Returns:
            Tuple[List[str], List[scrapy.Selector]]:
        """
        table = response.xpath('//div[@id="full"]//tr')
        # header will always be the first row in this table
        # for some reason there are empty header columns even though the html does not show any
        header = [cell.strip() for cell in table[0].xpath('.//text()').extract() if cell.strip()]

        return header, table[1:]
