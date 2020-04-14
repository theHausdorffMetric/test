import datetime as dt

from scrapy import Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.sines import normalize, parser


RELEVANT_TABLES = [
    'Berthed ships',
    'Ships at anchor',
    'Ships in maneuvers',
    # 'Ships hovering',
    'Arrival forecasts',
    # 'Output forecasts',
]


class SinesSpider(PortAuthoritySpider, Spider):
    name = 'Sines'
    provider = 'Sines'
    version = '1.0.3'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # Scraping the english version of Sines port authority website
        'http://www.portodesines.pt/en/ships/'
    ]

    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    def parse(self, response):
        """Entry point of Sines spider.

        Args:
            scrapy.Response:

        Yields:
            scrapy.Response:

        """
        for table_title_sel in response.xpath('//h3'):
            title = table_title_sel.xpath('.//text()').extract_first().strip()
            # allows one to easily disable/enable tables to be scraped
            if title not in RELEVANT_TABLES:
                continue

            table = table_title_sel.xpath('./following-sibling::div[1]/div/table')
            for row, detail_link in parser._extract_table(table):
                # store info on portcall type (based on table name)
                row.update(title=title)

                yield response.follow(
                    detail_link, callback=self.parse_detail, meta={'raw_item': row}
                )

    def parse_detail(self, response):
        """Parse portcall detail page.

        Args:
            scrapy.Response:

        Returns:
            Dict[str, Any]:

        """
        raw_item = response.meta['raw_item']

        # append shipping management data to raw_item
        raw_item.update(**parser._extract_mgmt_table(response))

        # append merchandise data to raw_item, only if they are present
        merchandise = response.xpath('//table')
        if merchandise:
            raw_item.update(cargoes=[row for row, _ in parser._extract_table(merchandise[0])])

        # append contexualise meta info to raw_item
        raw_item.update(
            port_name=self.name, provider_name=self.provider, reported_date=self.reported_date
        )

        yield normalize.process_item(raw_item)
