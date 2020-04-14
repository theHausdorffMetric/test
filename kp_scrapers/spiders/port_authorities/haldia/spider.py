import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.haldia import normalize


class HaldiaSpider(PortAuthoritySpider, Spider):
    name = 'Haldia'
    provider = 'Haldia Dock'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'http://www.kolkataporttrust.gov.in/report_view.php'
        '?view_req=1&layout=3&lang=1&level=0&linkid=&rid=52'
    ]

    def parse(self, response):
        # source doesn't provide reported date; use date-of-scraping
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        for idx, row in enumerate(response.xpath('//table//tr')):
            # first row contains the table name only, can be skipped
            if idx == 0:
                continue

            # header will always be in the second row
            if idx == 1:
                headers = row.xpath('.//td//text()').extract()
                continue

            raw_item = row_to_dict(row, headers)
            # contextualise raw item with meta info
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=reported_date
            )

            yield normalize.process_item(raw_item)
