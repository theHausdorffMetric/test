import datetime as dt

from scrapy import Spider
from w3lib.html import remove_tags

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.mundra import normalize


class MundraPA(PortAuthoritySpider, Spider):
    name = 'Mundra'
    provider = 'Mundra Adani'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://www.adaniports.com/Ports-and-Terminals/Mundra-Port/VesselSchedule']

    def parse(self, response):
        reported_date = dt.datetime.utcnow().isoformat()
        table = response.xpath('//table[@class="table bg-gray"]')

        for tbl in table:
            header = [hd for hd in tbl.xpath('./thead//th//text()').extract()]
            for idx, row in enumerate(tbl.xpath('.//tbody//tr')):
                row = [remove_tags(cell) for cell in row.xpath('./td').extract()]

                raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(
                    port_name='Mundra', provider_name=self.provider, reported_date=reported_date
                )
                yield normalize.process_item(raw_item)
