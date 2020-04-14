import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.milford import normalize


class MilfordHavenSpider(PortAuthoritySpider, Spider):
    name = 'Milford'
    provider = 'Milford'
    version = '2.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'https://www.mhpa.co.uk/vessels-arriving/',
        'https://www.mhpa.co.uk/vessels-at-berth/',
    ]

    def parse(self, response):
        """Entry point of Milford spider.

        Args:
            response:

        Returns:
            Dict[str, str]:

        """
        # memoise reported_date so it won't need to be called repeatedly
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        for idx, row in enumerate(response.xpath('//table[@id="DataTable"]//tr')):
            # first row always contains header
            if idx == 0:
                header = row.xpath('.//th/text()').extract()
                continue

            raw_item = row_to_dict(row, header)
            # contextualise raw item with meta info
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=reported_date
            )

            yield normalize.process_item(raw_item)
