from scrapy import Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.botany import normalize


class BotanySpider(PortAuthoritySpider, Spider):
    name = 'Botany'
    provider = 'Botany'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    reported_date = None

    start_urls = [
        'https://ships.portauthoritynsw.com.au/marineoperations/DailyVesselMovements.aspx'
    ]

    def parse(self, response):
        """Entry point of Botany spider.

        Args:
            response (scrapy.Response):

        Yields:
            scrapy.Response:

        """
        # extract reported date
        _date_now = response.xpath('.//*[@id="ctl00_lblDateNow"]/text()').extract_first()
        _time_now = response.xpath('.//*[@id="ctl00_lblTimeNow"]/text()').extract_first()
        reported_date = f'{_date_now} {_time_now}'

        for idx, table in enumerate(response.xpath('//table')):
            # strip header and only keep non-empty texts
            headers = [
                may_strip(header)
                for header in table.xpath('.//tr/th/text()').extract()
                if may_strip(header)
            ]

            # determine installation, event type
            # TODO could be improved to not rely on hardcoded table widths
            installation = 'Botany' if idx <= 2 else 'Sydney Harbour'
            if idx % 3 == 0:
                event_type = 'berthed'
            elif idx % 3 == 1:
                event_type = 'eta'
            else:
                event_type = 'departure'

            # scrape each table row
            for row in table.xpath('.//tr'):
                vessel_url = row.xpath('.//a/@href').extract_first()
                # skip header row
                if not vessel_url:
                    continue

                cells = [
                    may_strip(cell)
                    for cell in row.xpath('.//td//text()').extract()
                    # from provider:
                    # "* in the first column indicates movement had completed"
                    if cell != '*'
                ]

                raw_item = {header: cells[head_idx] for head_idx, header in enumerate(headers)}
                # contextualise raw item with meta info
                raw_item.update(
                    event_type=event_type,
                    installation=installation,
                    # NOTE overall zone is actually Sydney; Botany is a smaller bay within it
                    port_name='Sydney',
                    provider_name=self.provider,
                    reported_date=reported_date,
                )

                # we still need to retrieve the detailed vessel attributes
                # allows for easier matching of vessels
                yield response.follow(
                    url=vessel_url, callback=self.parse_vessel, meta={'raw_item': raw_item}
                )

    def parse_vessel(self, response):
        """Parse detailed vessel page.

        This will retrieve detailed vessel attributes such as LOA, DWT, IMO, flag, etc...

        Args:
            response (scrapy.Response):

        Returns:
            Dict[str, Any]:

        """
        keys = [
            may_strip(each)
            for each in response.xpath('.//*[@class="DtlFieldLabel"]/text()').extract()
        ]
        values = response.xpath('.//td/span/text()').extract()

        response.meta['raw_item'].update({keys[idx]: value for idx, value in enumerate(values)})
        return normalize.process_item(response.meta['raw_item'])
