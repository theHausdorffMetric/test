import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import extract_nth_cell
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.algiers import normalize


class AlgiersSpider(PortAuthoritySpider, Spider):
    name = 'Algiers'
    provider = 'Algiers'
    version = '1.1.3'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'https://www.portalger.com.dz/situation-du-port/navires-en-rade',  # vessels in port
        'https://www.portalger.com.dz/situation-du-port/navires-a-quai',  # vessels berthed
        # TODO vessels sailing; ask analysts for relevance
        # 'https://www.portalger.com.dz/situation-du-port/navires-sortis',
    ]

    def parse(self, response):
        table = response.xpath('//table//tr[position()>=1]')
        # sanity check; sometimes the source itself will display internal server errors
        if not table or 'error' in ''.join(table.extract()):
            self.logger.warning("Unable to parse URL, skipping: %s", response.url)
            return

        # memoise reported date so it won't need to be assigned repeatedly again
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        headers_row = table[0].xpath('.//th')
        # why this approach here? one of the specific URL contains headers with an empty column,
        # this will misalign the header and value matching.
        # This appraoch will better capture this
        headers = [
            header.xpath('.//text()').extract_first() if header.xpath('.//text()') else 'DUMMY'
            for header in headers_row
        ]

        for row in table[1:]:
            raw_item = {head: extract_nth_cell(row, idx + 1) for idx, head in enumerate(headers)}
            # contextualise raw_item with meta info
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=reported_date
            )
            yield normalize.process_item(raw_item)
