import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.donges import normalize


class DongesSpider(PortAuthoritySpider, Spider):
    name = 'Donges'
    provider = 'Montoir'
    version = '3.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # vessels anchored near port or at berth (contains cargo info)
        'http://www.nantes.port.fr/live-at-the-port/vessel-movements-in-real-time/vessels-alongside-quay/?L=1',  # noqa
        # vessels scheduled to arrive (does not contain cargo info)
        'http://www.nantes.port.fr/php/functions.php?action=displayMouvement&lang=en',
    ]

    def parse(self, response):
        """Dispatch response to corresponding parsing function depending on URL.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            scrapy.HtmlResponse:

        """
        # extract "In Quay" activity
        if 'vessels-alongside-quay' in response.url:
            yield from self.parse_in_quay_page(response)

        # extract "Expected Vessels" activity
        elif 'functions.php' in response.url:
            yield from self.parse_expected_schedule_page(response)

    def parse_in_quay_page(self, response):
        header = response.xpath('//th/text()').extract()
        # `Unloading` column can appear in headers list sometimes, so we need to try and remove it
        self._remove_elements(header, 'Unloading')

        # obtain table rows
        for row in response.xpath('//table[@id="tab_navire_a_quai_bottom"]//tr'):
            raw_item = row_to_dict(
                row,
                header,
                # contextualise raw item with meta data
                port_name=self.name,
                provider_name=self.provider,
                reported_date=dt.datetime.utcnow(),
                # data on this page is exclusively arrived vessels
                event='arrival',
            )
            yield normalize.process_item(raw_item)

    def parse_expected_schedule_page(self, response):
        header = response.xpath('//th/text()').extract()

        # obtain table rows
        for row in response.xpath('//div[@id="tab_mouvement_bottom_div"]//tr'):
            raw_item = row_to_dict(
                row,
                header,
                # contextualise raw item with meta data
                port_name=self.name,
                provider_name=self.provider,
                reported_date=dt.datetime.utcnow(),
                # data on this page is exclusively vessels yet to arrive
                event='eta',
            )
            yield normalize.process_item(raw_item)

    @staticmethod
    def _remove_elements(lst, *elements):
        """Try and remove specified elements from a list.

        Args:
            lst (list):
            *elements: elements to attempt to remove from `lst`

        """
        for element in elements:
            while element in lst:
                lst.remove(element)
