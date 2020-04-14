"""Dutch port authority spider for Zeebrugge port.

Forecast:   http://www.zedis.be/lijsten/SchepenVerwacht.aspx
In harbour: http://www.zedis.be/lijsten/SchepenHaven.aspx
Departed:   http://www.zedis.be/lijsten/SchepenAfgevaren.aspx

We are only using the Forecast page now,
because the other two do not provide relevant details for now.

"""
import re

from scrapy import Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.zeebrugge import normalize


# name of key used in website's HTML tag containing relevant details of a portcall
KEY_PATTERN = r'^Uc_DossierInfoReadonly1_(\S*)(?<!Text)$'


class ZeebruggeSpider(PortAuthoritySpider, Spider):
    name = 'Zeebrugge'
    provider = 'Zeebrugge'
    version = '1.0.2'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    start_urls = [
        # vessel forecast only
        'http://www.zedis.be/lijsten/SchepenVerwacht.aspx?agent=0'
    ]

    spider_settings = {
        # sequential requests mandatory for preventing incoherent viewstates
        'CONCURRENT_REQUESTS': 1
    }

    def parse(self, response):
        """Parse overview of vessel forecast table from webpage.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        # contains more than one table, not all tables are relevant or contain data
        raw_tables = response.xpath('//td/table')

        # reported date is in the third table
        reported_date = raw_tables[2].xpath('.//span/text()').extract_first()
        # actual portcall data is in the fifth table
        data_table = raw_tables[4].xpath('.//tr')

        for idx, row in enumerate(data_table):
            # table first row is always the header, skip
            if idx == 0:
                continue

            # each row has a unique url for detailed info; parse for vessel attributes
            yield response.follow(
                response.urljoin(row.xpath('.//a/@href').extract_first()),
                callback=self.parse_details,
                meta={'reported_date': reported_date},
                # support Crawlera scraping, if enabled
                headers={'X-Crawlera-Profile': 'desktop'},
            )

    def parse_details(self, response):
        """Parse details of an individual vessel portcall.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        # initialise raw item with context baked in
        raw_item = {
            'port_name': self.name,
            'provider_name': self.provider,
            'reported_date': response.meta['reported_date'],
        }

        # relevant data is in the fourth table
        table = response.xpath('//td/table')[3]
        for field in table.xpath('.//td/span'):
            # the relevant data will always be contained in a tag with the specified id pattern
            key_match = re.match(KEY_PATTERN, field.xpath('@id').extract_first())
            if key_match:
                raw_item.update({key_match.group(1): field.xpath('text()').extract_first()})

        return normalize.process_item(raw_item)
