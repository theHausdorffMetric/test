import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.sohar import normalize


class SoharSpider(PortAuthoritySpider, Spider):
    name = 'Sohar'
    provider = 'Sohar'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.soharportandfreezone.com/en/soharport/shipping-vessel']

    def parse(self, response):
        """Parse and extract raw items from source HTML.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]:

        """
        # memoise reported_date so it won't need to be called repeatedly
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        # each table contains data on a specific vessel movement: eta, arrival, departure
        for table in response.xpath('//div[@class="accordion-group"]'):
            for row_idx, row in enumerate(table.xpath('.//tr')):
                cells = [
                    may_strip(cell) for cell in row.xpath('.//text()').extract() if may_strip(cell)
                ]

                # header will always be the first row in the table
                if row_idx == 0:
                    header = cells
                    continue

                raw_item = {head: cells[head_idx] for head_idx, head in enumerate(header)}
                # contextualise raw item with meta info
                raw_item.update(
                    port_name=self.name, provider_name=self.provider, reported_date=reported_date
                )

                yield normalize.process_item(raw_item)
