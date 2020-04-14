import datetime as dt

from scrapy import Spider

from kp_scrapers.models.items import Slot
from kp_scrapers.spiders.slots import SlotSpider


# FIXME a database id has nothing to do here
INSTALLATION_ID = 3445  # Dunkerque LNG
SELLER = 'DUNKERQUE LNG'


class SlotDunkerqueSpider(SlotSpider, Spider):
    name = 'SlotDunkerque'
    provider = 'Fluxys'
    version = '0.1.0'

    start_urls = ['https://www.ebb.dlng-sico.com/AvailableUioli']

    def parse(self, response):
        """Parse slots availability page for Dunkerque LNG installation.

        Args:
            response (scrapy.Response):

        Yields:
            Slot:

        """
        # we need a set because some date on the website are duplicated
        slot_dates = set()

        # we are interested only by 4th col "Validity Period"
        # e.g. "11/03/2019 - 14/03/2019"
        for period in response.xpath('//tbody/tr/td[4]/text()').extract():
            # sanity check, in case date comes in an incorrect format
            if len(period.split('-')) != 2:
                self.logger.error(f'Unable to parse invalid slot period: {period}')
                return

            start_date, end_date = [
                dt.datetime.strptime(d.strip(), '%d/%m/%Y') for d in period.split('-')
            ]

            # get all datetimes between start and end
            for day in range((end_date - start_date).days + 1):
                slot_dates.add(start_date + dt.timedelta(day))

        for day in slot_dates:
            yield Slot(
                installation_id=INSTALLATION_ID, seller=SELLER, date=day.strftime('%Y-%m-%d')
            )
