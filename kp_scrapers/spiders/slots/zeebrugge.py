import re

from dateutil.parser import parse as parse_date
from scrapy import Spider

from kp_scrapers.models.items import Slot
from kp_scrapers.spiders.slots import SlotSpider


# FIXME a database id has nothing to do here
INSTALLATION_ID = 3430  # Zeebrugge Terminal
SELLER = 'FLUXYSLNG'


class ZeebruggeSpider(SlotSpider, Spider):
    name = 'SlotZeebrugge'
    provider = 'Fluxys'
    version = '2.0.3'

    start_urls = ['https://www.fluxys.com/en/products-services/lng-ship-unloading']

    def parse(self, response):
        """Parse slots availability page for Zeebrugge LNG installation.

        Args:
            response (scrapy.Response):

        Yields:
            Slot:

        """
        raw_period = ''.join(
            response.xpath(
                '//div[@class="fluxys-text prod-descr__content col-xs-12 col-md-7 field-content"]'
                '/h3[contains(text(), "Zeebrugge")]'
                '/following-sibling::ul[1]/li/p/text()'
            ).extract()
        )

        # sanity check; in case resource changes
        if not raw_period.strip():
            self.logger.error('Unable to extract slot period, resource may have changed')
            return

        # there may be more than one slot period provided
        for period in (slot.strip() for slot in raw_period.splitlines() if slot.strip()):
            # second sanity check; in case string contains noise only
            if len(period) < 10:
                continue

            match = re.search(r'(?P<date>\d{1,2}\s+\w+\s+\d{4})[\w\d\s]+by\s+(.*)', period)
            if not match:
                self.logger.error('Unable to parse slot period, verify wording: %s', period)
                continue

            yield Slot(
                installation_id=INSTALLATION_ID,
                seller=SELLER,
                date=parse_date(match.group('date'), dayfirst=True).strftime('%Y-%m-%d'),
            )
