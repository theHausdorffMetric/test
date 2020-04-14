import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.altamira import normalize


class AltamiraSpider(PortAuthoritySpider, Spider):
    name = 'Altamira'
    provider = 'Altamira'
    version = '2.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'https://www.puertoaltamira.com.mx/engs/0002056/ship-programming-sheet',
        # pdf mirror: http://www.puertoaltamira.com.mx/upl/sec/Programacion.pdf
    ]

    def parse(self, response):
        table = response.xpath('//table[@class="tablaConsulta"]//tr')

        # extract tabular vessel movements
        for idx, row in enumerate(table):
            # headers will always be in the first row
            if idx == 0:
                header = [may_strip(cell) for cell in row.xpath('./td/text()').extract()]
                continue

            # don't scrape table sub-headers as row values
            if len(row.xpath('./td')) < len(header):
                continue

            raw_item = row_to_dict(row, header)
            # contextualise raw item with meta info
            raw_item.update(
                port_name=self.name,
                provider_name=self.provider,
                reported_date=dt.datetime.utcnow()
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .isoformat(),
            )

            yield normalize.process_item(raw_item)
