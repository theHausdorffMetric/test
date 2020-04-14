from datetime import datetime
from urllib.request import urlopen

from scrapy import Spider
import xlrd

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.ceyhan import normalize


class CeyhanSpider(PortAuthoritySpider, Spider):
    name = 'Ceyhan'
    provider = 'IMEAK'
    version = '2.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.dtoiskenderun.org.tr/raporlar']

    # source does not provide any reported date; use date-of-scraping
    reported_date = (
        datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    def parse(self, response):
        """Dispatch response to corresponding callback, depending on response url.

        Args:
            response (scrapy.HtmlResponse):

        Returns:
            scrapy.HtmlResponse:

        """
        latest_report = response.xpath('//a[@class="list-item"]/@href').extract_first()
        yield response.follow(latest_report, callback=self.parse_workbook_content)

    def parse_workbook_content(self, response):
        """Parse workbook content.

        Args:
            response (scrapy.HtmlResponse):

        Returns:
            dict[str, str]:

        """
        sheet = xlrd.open_workbook(
            file_contents=urlopen(response.url).read(), on_demand=True
        ).sheet_by_name(
            'Genel'
        )  # sheet name is hardcoded by source

        for idx, raw_row in enumerate(sheet.get_rows()):
            # skip empty rows
            if all(cell.value == '' for cell in raw_row):
                continue

            row = [
                may_strip(cell.value) if isinstance(cell.value, str) else cell.value
                for cell in raw_row
            ]

            # build raw item and contextualise with meta info
            raw_item = {str(idx): cell for idx, cell in enumerate(row)}
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=self.reported_date
            )

            yield normalize.process_item(raw_item)
