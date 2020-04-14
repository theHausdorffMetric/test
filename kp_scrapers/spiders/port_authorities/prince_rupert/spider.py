import datetime as dt
from typing import Any, Dict

from scrapy import Spider
from scrapy.http import Response

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.prince_rupert import normalize


XPATH_HEADER = '//div[@class="page_ship-shippingChart__chartTitles"]/span/text()'
XPATH_TABLE = '//div[@class="arrdep_chart"]'


class PrinceRupert(PortAuthoritySpider, Spider):
    name = 'PrinceRupert'
    provider = 'Prince Rupert'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://www.rupertport.com/arrivals-departures/']  # vessels arriving

    def parse(self, response: Response) -> Dict[str, Any]:
        """Entrypoint for parsing Prince Rupert port website."""

        # source doesn't give reported date, so we'll just default to date of scraping
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        header = [may_strip(head.extract()) for head in response.xpath(XPATH_HEADER)]
        table = response.xpath(XPATH_TABLE)

        # transform HTML elements into dict representation
        for line in table:
            data = [may_strip(cell.extract()) for cell in line.xpath('.//span/text()')]
            raw_item = {head: data[idx] for idx, head in enumerate(header)}
            raw_item.update(
                port_name='Prince Rupert', provider_name=self.provider, reported_date=reported_date
            )

            yield normalize.process_item(raw_item)
