from datetime import datetime

from scrapy import Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.kuantan import normalize


class KuantanSpider(PortAuthoritySpider, Spider):
    name = 'Kuantan'
    provider = 'Kuantan'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://portal.kuantanport.com.my:819/']

    def parse(self, response):
        """Request URL containing port report.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:
        """

        # take the current date as reported_date since there is no reported_date avaiable in source
        reported_date = datetime.now().isoformat()

        table = response.xpath('//table[@id="MainContent_dgvVessel"]')
        for rows in table.xpath('.//tr'):
            header_info = rows.xpath('.//th//text()').extract()
            if header_info:
                header = [may_strip(val) for val in header_info if val]

            record_info = rows.xpath('.//td//text()').extract()
            if record_info:
                values = [may_strip(val) for val in record_info if val]

                if len(header) != len(values):
                    self.logger.error("Table structure may have changed")
                    return

                raw_item = dict(zip(header, values))
                raw_item.update(
                    {
                        'reported_date': reported_date,
                        'port_name': self.provider,
                        'provider_name': self.provider,
                    }
                )

                yield normalize.process_item(raw_item)
