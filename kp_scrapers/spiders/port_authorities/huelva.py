# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta
import re

from scrapy.http import FormRequest, Request  # For POST request with parameters
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider, Rule
from six.moves.urllib.parse import urlencode

from kp_scrapers.models.items import VesselPortCall
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class HuelvaSpider(PortAuthoritySpider, CrawlSpider):
    name = 'Huelva'
    domain = '195.53.243.17'
    allowed_domains = [domain]
    start_urls = ('http://' + domain + '/historicos/getListForm.asp',)

    rules = (
        Rule(LinkExtractor(allow=('getForm\.asp',))),
        Rule(
            LinkExtractor(allow=('getForm1\.asp',), tags=('frame',), attrs=('src',)),
            callback='extract_link_js',
            follow=False,
        ),
    )

    def start_requests(self):
        today = datetime.today()
        formdata = {
            'FecIni': (today - timedelta(7)).strftime('%d/%m/%Y'),
            'FecSal': (today + timedelta(30)).strftime('%d/%m/%Y'),
        }
        for url in self.start_urls:
            yield FormRequest(url=url, formdata=formdata)

    def extract_link_js(self, response):
        selector = Selector(response)
        for link in selector.css('a::attr(href)').extract():
            m = re.match('javascript:newWindowList\((.*)\)', link)
            if m:
                params_list = m.groups()[0].replace("'", '').split(',')
                params = urlencode(
                    {
                        'CodAtr': params_list[0],
                        'AutPor': params_list[1],
                        'SubPue': params_list[2],
                        'tipo': params_list[3],
                    }
                )
                url_path = params_list[4].replace('../', '')
                url = 'http://{}/{}?{}'.format(self.domain, url_path, params)
                yield Request(url, callback=self.parse_item)

    def _field(self, table, i, j):
        q = 'tr:nth-child({}) td:nth-child({}) *::text'.format(i, j)
        s = ''.join(table.css(q).extract()).replace('\r\n', '').strip()
        previous = None
        r = []
        for c in s:
            if not (c == previous == ' '):
                r.append(c)
            previous = c
        return ''.join(r)

    def field(self, response, i, j):
        selector = Selector(response)
        table = selector.css('table tr:nth-child(1) table tr:nth-child(2) table')
        return self._field(table, i, j)

    def parse_item(self, response):

        item = VesselPortCall()
        item['url'] = response.url
        item['local_port_call_id'] = self.field(1, 2)
        item['vessel_name'] = self.field(3, 2)
        item['imo'] = self.field(3, 4)
        item['vessel_flag'] = self.field(6, 4)
        item['berth'] = self.field(9, 2)
        item['arrival_date'] = self.field(10, 2)
        item['etd'] = self.field(10, 4)
        item['from_port_name'] = self.field(11, 2)
        item['departure_destination'] = self.field(11, 4)

        yield item
