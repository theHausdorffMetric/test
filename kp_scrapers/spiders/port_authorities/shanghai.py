# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider
from scrapy.http import Request
from scrapy.selector import Selector

from kp_scrapers.lib.utils import extract_row
from kp_scrapers.models.items import Draught, EtaEvent, VesselIdentification
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class ShanghaiSpider(PortAuthoritySpider, Spider):

    name = 'Shanghai'
    start_urls = ['http://www.ysmsa.cn/Web/Pages/List.aspx?UpCatalogNo=56&CatalogNo=133']

    def parse(self, response):
        sel = Selector(response)
        xpath_ = '//div[@id="ctl00_DefaultContent_listDiv"]//li[1]/a/@href'
        page_link = sel.xpath(xpath_).extract_first()
        yield Request(
            url='http://www.ysmsa.cn/Web/Pages/' + page_link, callback=self.parse_daily_page
        )

    def parse_daily_page(self, response):
        sel = Selector(response)
        xpath_ = '//p[@id="ctl00_DefaultContent_lbContentDate"]//text()'
        updated_time = sel.xpath(xpath_).extract_first()
        rows = sel.xpath('//div[@class="con"]//tr')
        for row in rows[2:]:  # Skip first 2 lines (Headers)
            row_data = extract_row(row)
            row_data = [i for i in row_data if i.strip()]
            if len(row_data) == 10:
                item = EtaEvent()
                vessel_base = VesselIdentification(name=row_data[3], call_sign=row_data[4])
                draught_base = Draught(arrival=row_data[8])
                item['vessel'] = vessel_base
                item['draught'] = draught_base
                item['url'] = response.url
                item['port_name'] = 'Shanghai Area'  # Zone covers 3 installations
                item['eta'] = row_data[0]
                item['updated_time'] = updated_time.encode('utf-8').decode('ascii', 'ignore')

                yield item
