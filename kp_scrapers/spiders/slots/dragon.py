import datetime as dt

from scrapy import Spider

from kp_scrapers.models.items import Slot
from kp_scrapers.spiders.slots import SlotSpider


class SlotDragonSpider(SlotSpider, Spider):
    name = 'SlotDragon'
    provider = 'Dragon'
    version = '0.1.0'

    start_urls = ['http://dragonlng-dnas.co.uk/']

    def parse(self, response):
        lines = response.xpath("//table/tr[count(th)=3]/../tr")
        for line in lines[1:]:
            row = line.xpath('td/span/text()').extract()

            item = Slot()
            # FIXME a database id has nothing to do here
            item['installation_id'] = 3512  # Dragon LNG
            item['seller'] = row[0]
            item['date'] = str(dt.datetime.strptime(row[1], "%d-%m-%Y").date())

            yield item
