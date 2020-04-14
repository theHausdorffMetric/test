import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.arzew import normalize


class ArzewSpider(PortAuthoritySpider, Spider):
    name = 'Arzew'
    provider = 'Arzew'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'http://www.arzewports.dz/?pages=attendus',  # vessels expected
        'http://www.arzewports.dz/?pages=rade',  # vessels at anchorage
        'http://www.arzewports.dz/?pages=quai',  # vessels at berth
    ]

    def parse(self, response):
        """Entrypoint for parsing website response.

        Args:
            response (scrapy.Response):

        Yields:
            event (Dict[str, str]):

        """
        # memoise reported date since source does not provide any
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        # extract tabular data on vessel movements
        for idx, row in enumerate(response.xpath('//table//tr')):
            # header will always exist in the first row
            if idx == 0:
                header = row.xpath('.//th/text()').extract()
                continue

            raw_item = row_to_dict(row, header)
            # contextualise raw item with meta info
            raw_item.update(
                port_name=self.name,
                provider_name=self.provider,
                reported_date=reported_date,
                # event type depends on the page url, so remember it
                # (in addition to debugging benefits)
                url=response.url,
            )
            yield normalize.process_item(raw_item)
