import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.klaipeda import normalize


EVENT_MAPPING = {
    'planned-calls': 'eta',
    'list-of-vessels-within-the-port': 'berthed',
    'last-departures': 'departure',
}


class KlaipedaSpider(PortAuthoritySpider, Spider):
    name = 'Klaipeda'
    provider = 'Klaipeda'
    version = '1.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    start_urls = [
        # forecast of vessels due to arrive
        'https://www.portofklaipeda.lt/planned-calls',
        # TODO in-port activity
        # 'https://www.portofklaipeda.lt/list-of-vessels-within-the-port',
        # TODO vessels recently departed
        # 'https://www.portofklaipeda.lt/last-departures',
    ]

    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    def parse(self, response):
        # get nature of portcall data scraped from each url
        for event in EVENT_MAPPING:
            if event in response.url:
                event_type = EVENT_MAPPING[event]
                break

        for idx, row in enumerate(response.xpath('//table[@class="ships"]//tr')):
            # first row is always the headers
            if idx == 0:
                headers = [
                    # NOTE some headers don't have a key assigned
                    th.xpath('.//text()').extract_first() or 'unknown'
                    for th in row.xpath('./th')
                ]
                continue

            raw_item = row_to_dict(row, headers)
            # contextualise raw item with meta info
            raw_item.update(
                event_type=event_type,
                port_name=self.name,
                provider_name=self.provider,
                reported_date=self.reported_date,
            )

            yield normalize.process_item(raw_item)
