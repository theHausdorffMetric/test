from scrapy import Spider
from scrapy.http import Request

from kp_scrapers.models.items import Slot
from kp_scrapers.spiders.slots import SlotSpider
from kp_scrapers.spiders.slots.base_france import SlotBaseFrance


class SlotFosCavaouSpider(SlotBaseFrance, SlotSpider, Spider):
    name = 'SlotFosCavaou'
    provider = 'Fosmax'
    version = '0.1.0'

    base_url = 'http://www.fosmax-lng.com/'
    start_url = "http://www.fosmax-lng.com/en/commercial-section/primary-capacities.html"
    filename = 'fos_cavaou_slots_'

    def start_requests(self):
        yield Request(url=self.start_url, callback=self.get_pdf)

    def get_pdf(self, response):
        pdf_url = response.xpath('//*[@id="c153"]/p[2]/a/@href').extract_first()
        pdf_url = self.base_url + pdf_url
        yield Request(url=pdf_url, callback=self.parse)

    def parse(self, response):
        rows = self._get_text_from_response(response.body, 2)
        header, start_line = self._get_header_and_start_line(rows)

        self.year = rows[start_line - 1].strip()

        columns_size = self._get_columns_size(header)

        # Keep only rows that interest us, +1 to pop header, +31 to go at the end
        cross_lines = rows[start_line + 2 : start_line + 2 + 31]
        extracted_dates = self._get_dates(cross_lines, columns_size)

        for date in extracted_dates:
            item = Slot()
            item['date'] = date
            # FIXME a database id has nothing to do here
            item['installation_id'] = 3446  # Fos Cavaou LNG Terminal
            item['on_offer'] = False

            yield item
