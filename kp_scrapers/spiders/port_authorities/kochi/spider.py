import re

from scrapy import Spider

from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.kochi import normalize


SCHEDULE_TABLE_ORDER = 2
STARTING_ROW_ID = 2
HEADER_ROW_ID = 1
DATE_ROW_ID = 0


class KochiSpider(PortAuthoritySpider, Spider):
    name = 'Kochi'
    provider = 'Kochi'
    version = '1.3.2'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://cochinport.gov.in/index.php?opt=shipsatport&cat=ev&tab=2']

    def parse(self, response):
        table = response.css('table.shipsinport')[SCHEDULE_TABLE_ORDER].css('tr')
        rows = table[STARTING_ROW_ID:]
        headers = [may_strip(head) for head in table[HEADER_ROW_ID].css('::text').extract()]
        title = table[DATE_ROW_ID].extract()
        last_updated = re.search('[0-9]{2}.[0-9]{2}.20[0-9]{2}', title).group()

        for row in rows:
            raw_item = row_to_dict(row, headers)
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=last_updated
            )
            yield normalize.process_item(raw_item)
