from scrapy import Spider
from scrapy.http import Request

from kp_scrapers.models.items import Slot
from kp_scrapers.spiders.slots import SlotSpider
from kp_scrapers.spiders.slots.base_france import SlotBaseFrance


class SlotMontoirFosTonkinSpider(SlotBaseFrance, SlotSpider, Spider):
    name = 'SlotMontoirFosTonkin'
    version = '1.0.1'
    provider = 'Elengy'

    base_url = 'https://www.elengy.com/'
    start_url = 'https://www.elengy.com/en/contracts-and-operations/primary-capacities.html'
    filename = 'fos_cavaou_slots_'

    def start_requests(self):
        yield Request(url=self.start_url, callback=self.get_pdf)

    def get_pdf(self, response):
        pdf_urls = response.xpath('//div[@itemprop="articleBody"]//ul//a/@href').extract()[0:2]
        for pdf_url in pdf_urls:
            yield Request(url=self.base_url + pdf_url, callback=self.parse)

    def parse(self, response):
        rows = self._get_text_from_response(response.body, 2)
        header, start_line = self._get_header_and_start_line(rows)

        self.year = ''.join([c for c in rows[start_line] if c.isdigit()])

        columns_size = self._get_columns_size(header)

        # Keep only rows that interest us, +1 to pop header, +31 to go at the end
        cross_lines = rows[start_line + 1 : start_line + 1 + 31]
        extracted_dates = self._get_dates(cross_lines, columns_size)

        url = response.url.lower()
        # FIXME a database id has nothing to do here
        if 'fostonkin' in url:
            installation_id = 3447  # Fos Tonkin LNG Terminal
        elif 'montoir' in url:
            installation_id = 3448  # Montoir Terminal
        else:
            self.logger.error("Couldn't identify installation from url : {}".format(response.url))
            return

        for date in extracted_dates:
            item = Slot()
            item['date'] = date
            item['installation_id'] = installation_id
            item['on_offer'] = False

            yield item
