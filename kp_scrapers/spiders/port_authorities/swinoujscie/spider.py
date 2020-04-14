import re

from scrapy import Spider
from w3lib.html import remove_tags

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.swinoujscie import normalize


class SwinoujscieSpider(PortAuthoritySpider, Spider):

    name = 'Swinoujscie'
    provider = 'Swinoujscie'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    # Webpage to get start_urls : http://www.ums.gov.pl/bezpieczenstwo-morskie/ruch-statkow.html
    start_urls = [
        'http://195.245.226.20/ums/inbound.php',  # vessels arriving
        # 'http://195.245.226.20/ums/port.php',  # vessels in port
        # 'http://195.245.226.20/ums/outbound.php',  # vessels departing
    ]

    def parse(self, response):
        # page has multiple tables for different ports
        for article in response.css('article'):
            port_name = article.css('h2::text').extract_first().split(' ')[0].title()

            reported_date = self.extract_reported_date(
                may_strip(article.css('div::text').extract_first().title())
            )

            table = article.css('table tbody tr')
            for row in table:
                headers = [
                    may_strip(remove_tags(header))
                    for header in article.css('table thead tr th').extract()
                ]

                raw_item = row_to_dict(
                    row,
                    headers,
                    # contextualise raw item with meta data
                    port_name=port_name,
                    provider_name=self.provider,
                    reported_date=reported_date,
                )
                yield normalize.process_item(raw_item)

    @staticmethod
    def extract_reported_date(raw_reported):
        """Extract reported date from webpage

        Args:
            raw_date (str)

        Returns:
            str | None:
        """
        _match = re.match(r'.*?(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}).*', raw_reported)

        if _match:
            return to_isoformat(_match.group(1))
