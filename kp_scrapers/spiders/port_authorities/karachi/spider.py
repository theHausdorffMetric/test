from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.karachi import normalize


class KarachiPASpider(PortAuthoritySpider, Spider):
    name = 'Karachi'
    provider = 'Karachi'
    version = '2.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'http://kpt.gov.pk/ASP/Shipping%20Intelligence/ShowExpArrivals.asp'  # expected arrivals
    ]

    def parse(self, response):
        tables = response.xpath('//center/table')
        table_names = response.xpath('//center/font//text()').extract()

        # memoise reported date
        reported_date = response.xpath('//body/table/tr[2]//b/text()').extract_first()

        for table_idx, table in enumerate(tables):
            # NOTE first row is actually empty and should be skipped
            # header is in second row, but we don't need to use it
            # table rows are in the subsequent rows
            rows = table.xpath('tr')[2:]
            for row in rows:
                # import ipdb; ipdb.set_trace()
                header = [str(idx) for idx in range(len(row.xpath('td')))]
                raw_item = row_to_dict(
                    row,
                    header,
                    port_name=self.name,
                    vessel_type=table_names[table_idx],
                    provider_name=self.provider,
                    reported_date=reported_date,
                )
                yield normalize.process_item(raw_item)
