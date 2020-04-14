import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.le_havre import normalize


class LeHavreSpider(PortAuthoritySpider, Spider):
    name = 'LeHavre'
    provider = 'LeHavre'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # forecast of vessels expected to arrive soon
        'https://www.havre-port.com/fr/liste/128/arrivees-au-grand-port-maritime-du-havre',
        # TODO activity of vessels at berth
        # 'https://www.havre-port.com/fr/liste/2000/navires-a-quai',
        # TODO forecast of vessels expected to depart soon
        # 'https://www.havre-port.com/fr/liste/129/departs-au-grand-port-maritime-du-havre',
    ]

    # site does not provide any reported date; use date-of-scraping
    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    port_name = 'Le Havre'

    def parse(self, response):
        """Entrypoint of LeHavre spider.

        Yields:
            scrapy.Response:

        """
        # each portcall listed in the table has a link to another page
        # showing more details of that same portcall
        for portcall_link in response.xpath('//tr//a/@href').extract():
            yield response.follow(portcall_link, callback=self.parse_vessel_details)

    def parse_vessel_details(self, response):
        """Parse page containing detailed portcall info.

        Each page contains six tables:
            - "Indentification escale" (internal port identification numbers)
            - "Entree" (eta, arrival, piloting date and timestamps)
            - "Sortie" (etd, departure dates and timestamps)
            - "Sejour a Quai" (berthed date and timestamp)
            - "Provenance / Destination" (previous port visited, next port to be visited)
            - "Caracteristiques du navire" (vessel attributes)

        All tables except "Caracteristiques du navire",
        are structured identically and can be parsed as such.

        Table "Caracteristiques du navire" will be parsed separately.

        Yields:
            Dict[str, str]:

        """
        raw_item = {}

        # parse and extract from identical tables
        for table in response.xpath('//table[@class="ormo table table-striped "]'):
            for idx, row in enumerate(table.xpath('.//tr')):
                # headers will always be the first row
                if idx == 0:
                    headers = row.xpath('./th/text()').extract()
                    continue

                headers = table.xpath('.//th//text()').extract()
                raw_item.update(row_to_dict(row, headers))

        # parse and extract from "Caracteristiques du navire" table
        for field in response.xpath('//table[@class="ormo ref"]//td'):
            key = field.xpath('./div/div[1]//text()').extract_first()
            value = field.xpath('./div/div[2]//text()').extract_first()
            raw_item[key] = value

        # contextualise raw item with meta info
        raw_item.update(
            port_name=self.port_name, provider_name=self.provider, reported_date=self.reported_date
        )

        yield normalize.process_item(raw_item)
