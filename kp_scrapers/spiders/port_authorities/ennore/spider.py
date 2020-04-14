import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.ennore import normalize


class EnnoreSpider(PortAuthoritySpider, Spider):
    name = 'Ennore'
    provider = 'Ennore'
    version = '1.1.3'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.ennoreport.gov.in/content/vessel_reports.php']

    def parse(self, response):
        # last table provides ETA schedules, other tables are not required
        table = response.xpath('//table')[-1]
        header = table.xpath('.//th//text()').extract()

        # memoise reported date
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        # iterate through table row entries
        for row in table.xpath('./tbody/tr'):
            raw_item = row_to_dict(row, header)
            # contextualise raw item with meta info
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=reported_date
            )

            # yield raw_item
            yield normalize.process_item(raw_item)
