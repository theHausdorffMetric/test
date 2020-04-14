import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.dakar import normalize


class DakarSpider(PortAuthoritySpider, Spider):
    name = 'Dakar'
    provider = 'Dakar'
    version = '1.0.2'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.portdakar.sn/en/infos-pratiques/previsions-trafic']  # berthed vessels

    def parse(self, response):
        """Extract table rows and header from vessel berthed page.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            dict[str, str]:

        """
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        headers = response.xpath('//table/thead//th/text()').extract()
        for row in response.xpath('//table/tbody/tr'):
            raw_item = row_to_dict(row, headers)
            # contextualise raw item with meta info
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=reported_date
            )
            yield normalize.process_item(raw_item)
