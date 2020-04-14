import datetime as dt

from scrapy import Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.santos import normalize, parser


class SantosSpider(PortAuthoritySpider, Spider):
    name = 'Santos'
    provider = 'Santos'
    version = '1.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # ETA schedule
        'http://www.portodesantos.com.br/en/ship-tracker/scheduled-arrivals/',
        # arrival schedule
        'http://www.portodesantos.com.br/en/ship-tracker/expected-arrivals/',
        # Berthed schedule
        'http://www.portodesantos.com.br/en/ship-tracker/berthed-ships/',
    ]

    # disambiguate from similarly named ports on the platform
    port_name = 'Santos SP'

    def parse(self, response):
        """Parse port activity pages.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        # website does not provide a reported date, use current day scrapped instead
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        for row in response.xpath('//table/tbody/tr'):
            raw_item = {
                str(idx): parser.remove_td_tags(cell)
                for idx, cell in enumerate(row.xpath('td').extract())
            }
            raw_item.update(
                port_name=self.port_name,
                provider_name=self.provider,
                reported_date=reported_date,
                url=response.url,
            )
            yield normalize.process_item(raw_item)
