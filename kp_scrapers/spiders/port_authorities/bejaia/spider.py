import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.bejaia import normalize


class BejaiaSpider(PortAuthoritySpider, Spider):

    name = 'Bejaia'
    provider = 'Bejaia'
    version = '1.3.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://portdebejaia.dz/situation-des-navires/']

    def parse(self, response):
        """Parse and extract raw items from html tables.

        Each entry in the port activity table has a link on the vessel name, with the vessel IMO
        in the link itself. We append vessel imo to each row we extract, since it is not technically
        part of the table cells.

        Vessel imo appears as part of the html query string, e.g.:
        ".../phpcodes/navire_a.php?ship=9297905"
                                        ^^^^^^^
                                          imo

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]:

        """
        # memoise reported_date so it won't need to be called repeatedly
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        # each index corresponds to the vessel movement type in the table
        # 0: attendus
        # 1: a quai
        # 2: en rade
        for table_idx in range(3):
            table = response.xpath(f'//div[contains(@class, "et_pb_tab_{table_idx}")]//table')
            header = [may_strip(head) for head in table.xpath('.//th/text()').extract()]

            for row in table.xpath('./tbody//tr'):
                raw_item = row_to_dict(row, header)
                # conextextualise raw item with meta info
                raw_item.update(
                    port_name=self.name,
                    provider_name=self.provider,
                    reported_date=reported_date,
                    vessel_imo=row.xpath('./td//@href').extract_first().split('ship=')[1],
                )
                yield normalize.process_item(raw_item)
