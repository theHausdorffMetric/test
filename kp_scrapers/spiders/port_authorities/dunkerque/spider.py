from scrapy import Spider

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.dunkerque import normalize


class DunkerqueSpider(PortAuthoritySpider, Spider):
    name = 'Dunkerque'
    version = '1.0.0'
    provider = 'Dunkerque'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'http://www.dunkerque-port.fr/en/commercial-activities/ships-entry-transit.html',
        'http://www.dunkerque-port.fr/en/commercial-activities/ships-in-port.html',
        # TODO check with analysts if we need ETDs
        # 'http://www.dunkerque-port.fr/en/commercial-activities/ships-exit.html',
    ]

    def parse(self, response):
        """Extract vessel movements listed in `start_urls`.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:
        """
        # memoize reported date so it won't need to be repeatedly computed later
        reported_date = to_isoformat(
            response.xpath('//p/text()').extract_first(), dayfirst=False, yearfirst=True
        )

        table = response.xpath('//table/tr')
        header, data_rows = table[0].xpath('./th//text()').extract(), table[1:]
        for row in data_rows:
            raw_item = row_to_dict(
                row,
                header,
                port_name=self.name,
                reported_date=reported_date,
                provider_name=self.provider,
            )
            yield normalize.process_item(raw_item)
