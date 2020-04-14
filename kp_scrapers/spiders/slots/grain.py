import re

from scrapy import Spider
from scrapy.exceptions import CloseSpider

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.items import Slot
from kp_scrapers.spiders.slots import SlotSpider


class SlotGrainSpider(SlotSpider, Spider):
    name = 'SlotGrain'
    provider = 'SlotGrain'
    version = '0.2.3'

    start_urls = ['http://grainlng.com/operational-information/uioli-availability/']

    def parse(self, response):
        """Entrypoint for SlotGrain spider.

        Args:
            response (scrapy.Response):

        Yields:
            Slot:
        """
        if 'available slots' not in response.text:
            self.logger.error('Unable to extract data; resource might have changed')
            raise CloseSpider('cancelled')

        slots = response.xpath('//div[@class="content article--content"]/*//text()').extract()
        for line in slots:
            # sanity check, in case of irrelevant lines
            if not may_strip(line) or line == '\u200b' or line == '.':
                continue

            date = self._extract_date(may_strip(line.replace('\xa0', ' ')))
            if not date:
                continue

            yield Slot(
                # FIXME a database id has nothing to do here
                installation_id=3513,  # Grain LNG Terminal
                seller='',
                date=date,
            )

    @staticmethod
    def _extract_date(sentence):
        """Extract and convert relevant slot dates to ISO8601 format.

        Args:
            sentence (str):

        Returns:
            str: ISO8601 formatted date

        Example:
            >>> SlotGrainSpider._extract_date('A berthing slot is available on the 2nd July 2018')
            '2018-07-02T00:00:00'
            >>> SlotGrainSpider._extract_date('A berthing slot is available on 11 July 2018')
            '2018-07-11T00:00:00'
            >>> SlotGrainSpider._extract_date('A berthing slot on the 29th June 2018 \
            with associated storage space and regasification capacity between 28th June 2018 \
            and 8th July 2018')
            '2018-06-29T00:00:00'
        """
        date_match = re.search(r'(\d{1,2}(?:[A-z]{2})?\s+[A-z]+\s+\d{4})', sentence)
        if not date_match:
            return None

        return to_isoformat(date_match.group(1))
