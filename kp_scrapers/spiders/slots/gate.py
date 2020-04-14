import re

from scrapy import Spider

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.i18n import DUTCH_TO_ENGLISH_MONTHS, translate_substrings
from kp_scrapers.spiders.slots import SlotSpider


class SlotGateSpider(SlotSpider, Spider):
    name = 'SlotGate'
    # NOTE database seems to only define GateOperator, which might be a
    # confusion with the other spider
    provider = 'Gate'
    version = '2.0.0'
    produces = []  # TODO formalise Slot data model

    start_urls = ['https://www.gateterminal.com/en/commercial/services-gate/']

    def parse(self, response):
        """Parse page for text containing UIOLI slots.

        Example of data contained within the page (as of 5 December 2019):

            > Primaire capaciteit: 1,0 BCM per jaar
            > UIOLI slots: Gate werd op de hoogte gebracht dat op de secundaire markt de volgende slots beschikbaar zijn : 02 jan 2020.  # noqa
            > Terminal usage 1 september 2011 - 30 september 2012
            > Terminal usage 1 oktober 2012 - 30 september 2013
            > Terminal usage 1 oktober 2013 - 30 september 2014
            > Terminal usage 1 oktober 2014 - 30 september 2015
            > Terminal usage 1 oktober 2015 - 30 september 2016
            > Terminal usage 1 oktober 2016 - 30 september 2017
            > Terminal usage 1 oktober 2017 - 30 september 2018
            > Terminal usage 1 oktober 2018 - 30 september 2019
            > Terminal usage huidig gasjaar

        We are looking for the line containing the substring "UIOLI slots",
        and extracting the date as specified on that line.

        """
        for potential_slot in response.xpath('//div[@class="naam"]/p/text()').extract():
            slot_date = self._parse_slot_date(potential_slot)
            if not slot_date:
                self.logger.info('Not a valid slot description:\n%s', potential_slot)
                continue

            # TODO formalise Slot data model
            item = {
                'date': slot_date,
                # FIXME a database id has nothing to do here
                'installation_id': 3490,  # Gate LNG
                'seller': '',
                # if false, slot is not an Offered slot, but a Scheduled Delivery slot
                'on_offer': True,
            }
            yield item

    @staticmethod
    def _parse_slot_date(raw_text):
        """Check if a string contains slots information and parse it, if any.

        Takes into consideration Dutch abbreviations of month names.

        Examples:  # noqa
            >>> SlotGateSpider._parse_slot_date('UIOLI slots: Gate werd op de hoogte gebracht dat op de secundaire markt de volgende slots beschikbaar zijn : 02 jan 2020.')
            '2020-01-02T00:00:00'
            >>> SlotGateSpider._parse_slot_date('Terminal usage 1 september 2011 - 30 september 2012')

        """
        match = re.search(r'UIOLI slot.*(\d{1,2}\s[a-zA-Z]{3,}\.?\s\d{2,4})', raw_text)
        if not match:
            return None

        # sometimes abbreivated months can have dot suffixes
        raw_date = match.group(1).replace('.', '')

        return to_isoformat(translate_substrings(raw_date, DUTCH_TO_ENGLISH_MONTHS))
