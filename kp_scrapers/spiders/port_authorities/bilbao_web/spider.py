"""Spider module for Bilbao Port Authority.

An alternative form of portcall data from the main Bilbao spider,
but from the same provider/website.

Sometimes, this source provides more precise data, other times the main
source is the one to do so.

"""

from scrapy import Spider

from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.bilbao_web import normalize


class BilbaoWebSpider(PortAuthoritySpider, Spider):
    name = 'BilbaoWeb'
    provider = 'Bilbao'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://www.bilbaoport.eus/en/vessel-activity/']

    def __init__(self, table_name='Scheduled Calls', *args, **kwargs):
        """Init BilbaoWeb spider.

        There are five tables of vessel movements given by the source.
            - Vessel Arrivals
            - Vessel Departures
            - Vessel Operating
            - Scheduled Calls
            - Inactive Stay

        Each spider job needs to choose one table to scrape.

        """
        super().__init__(*args, **kwargs)
        self.table_name = may_strip(table_name)

    def parse(self, response):
        """Parse and extract raw items from html tables.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, Any]:

        """
        reported_date = response.xpath(
            '//div[@class="panel radius text-center"]/text()'
        ).extract_first()

        for table in response.xpath('//table[@class="listado-boletin"]'):
            # verify if table is the one we want to scrape
            if may_strip(table.xpath('./caption/text()').extract_first()) != self.table_name:
                continue

            header = table.xpath('.//th/text()').extract()
            data_rows = table.xpath('.//tr')
            for row in data_rows:
                raw_item = row_to_dict(row, header)
                # contextualise raw item with meta info
                raw_item.update(
                    port_name='Bilbao', provider_name=self.provider, reported_date=reported_date
                )
                yield normalize.process_item(raw_item)
