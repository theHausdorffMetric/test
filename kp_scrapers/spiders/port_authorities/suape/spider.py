from scrapy import Spider

from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.suape import normalize


# number of columns for the table to be extracted
TABLE_WIDTH = 13


class SuapeSpider(PortAuthoritySpider, Spider):
    name = 'Suape'
    provider = 'Suape'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.suape.pe.gov.br/en/port/real-time-port/daily-ship-moviments']

    def parse(self, response):
        """Parse and extract raw items from html tables.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]:

        """
        # memoise reported date so it won't need to be called repeatedly later
        reported_date = response.xpath('//div[@id="tabelasNavios"]/h3/text()').extract_first()

        all_tables = response.xpath('//div[@id="tabelasNavios"]/table')
        table_types = [
            may_strip(x)
            for x in response.xpath(
                '//div[@id="tabelasNavios"]/h2[@class="tableName"]//text()'
            ).extract()
            if may_strip(x)
        ]

        for idx, table in enumerate(all_tables):
            table_type = table_types[idx]
            # parse each row of the table body
            for row in table.xpath('./tbody/tr'):
                # use raw indexes instead of table headers since they are inconsistent
                raw_item = row_to_dict(
                    row,
                    [str(idx) for idx in range(TABLE_WIDTH)],
                    # contextualise raw item with some meta info
                    port_name=self.name,
                    provider_name=self.provider,
                    reported_date=reported_date,
                    table_type=table_type,
                )
                yield normalize.process_item(raw_item)
