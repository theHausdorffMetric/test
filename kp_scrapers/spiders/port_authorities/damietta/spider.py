from datetime import datetime

from scrapy import Spider

from kp_scrapers.lib.utils import extract_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.damietta import normalize


DATE_IDX = 6
# google translate original website, from right to left (Arabic)
headers = [
    "VESSEL'S NAME",
    'FLIGHT NUMBER',
    'AGENT',
    'UNLOADING GOODS',
    'CARGO CARGO',
    'SHIPPING COMPANY',
    'DATE OF ARRIVAL HIJACKER',
    'SUBMERSIBLE',
    'SIDEWALK',
]


class DamiettaSpider(PortAuthoritySpider, Spider):
    name = 'Damietta'
    provider = 'Damietta'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    start_urls = ['http://reports.dpa.gov.eg/default.aspx']

    reported_date = datetime.utcnow().isoformat(timespec='seconds')

    def parse(self, response):
        """Entry point of Damietta spider.

        Args:
            response:

        Returns:
            PortCall:

        """
        for row in response.xpath('//table[@id="gr"]//tr')[3:]:
            # in Arabic, don't translate here to avoid googletrans api request limit
            cells = extract_row(row)

            # only proceed the rows when date is presented
            if len(cells) == len(headers) and cells[DATE_IDX]:
                raw_item = {headers[idx]: cell for idx, cell in enumerate(cells)}
                raw_item.update(self.meta_field)
                yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {
            'port_name': self.name,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
