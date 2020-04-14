# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import re
import time

from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider
import six
import six.moves.urllib.parse

from kp_scrapers.models.items import EtaEvent, InPortEvent, VesselIdentification
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


def normalize_whitespace(raw):
    return re.sub(r'\s+', ' ', raw.strip())


class BonnySpider(PortAuthoritySpider, CrawlSpider):

    name = 'Bonny'
    start_urls = ('http://ports.co.za/shipmovements/bonny/articles.php',)

    def parse(self, response):
        sel = Selector(response)
        page = sel.xpath('//a[@class="headerlink"]/@href')[0].extract()
        return Request(
            url=six.moves.urllib.parse.urljoin(response.url, page), callback=self.parsePage
        )

    def parsePage(self, response):
        ship_lists = {'Ships in Port': 0, 'Ships expected': 1, 'Ships due at the Port': 1}
        stop_flags = ('ships', '#text', 'not available')
        for li_name, li_type in ship_lists.items():
            sel = Selector(response)
            xpath_str = (
                '//body/table//tr[2]/td[2]//h4[contains(text(), "'
                + li_name
                + '")]/following-sibling::*/text()'
            )
            rows = sel.xpath(xpath_str).extract()
            if len(rows) == 0:
                yield None
            lineRE = re.compile('^\s*(\d{2}/\d{2}/\d{2,4})\s+(.*)\s*')
            page_time = self.get_date(sel)
            for row in rows:
                for flag in stop_flags:
                    if flag in row.lower():
                        break
                row = row.strip()
                if isinstance(row, six.text_type) and row != '':

                    if li_type == 1:
                        line_content = lineRE.match(row)
                        if line_content is not None:
                            date = line_content.group(1)
                            vessels = [v.strip() for v in line_content.group(2).split(',')]
                            for v in vessels:
                                item = EtaEvent()
                                item['url'] = response.url
                                item['vessel'] = VesselIdentification(name=normalize_whitespace(v))
                                item['eta'] = date
                                item['updated_time'] = page_time
                                item['port_name'] = 'Bonny'
                                yield item

                    elif li_type == 0:
                        line_content = row.split(',')
                        for v in line_content:
                            item = InPortEvent()
                            item['url'] = response.url
                            item['vessel'] = VesselIdentification(name=normalize_whitespace(v))
                            item['updated_time'] = page_time
                            item['port_name'] = 'Bonny'

                            yield item

    def get_date(self, sel):
        # str_date = sel.xpath('//body/table//p[4]//text()').extract()[0]
        str_date = sel.xpath('//body/table//p[2]//text()').extract()[0]
        time_obj = time.strptime(str_date, '%d %B, %Y')
        new_time_str = time.strftime('%d/%m/%y', time_obj)

        return new_time_str
