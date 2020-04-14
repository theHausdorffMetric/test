# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy.http import Request
from scrapy.selector import Selector

from kp_scrapers.spiders.bases.persist import PersistSpider

from . import OperatorSpider
from .extractors.italia import ExcelExtractorGLETTemplate


class AdriaticSpider(OperatorSpider, PersistSpider):
    """Access to the documentation related to Terminale GNL Adriatico, with
    particular reference to access to regasification service at offshore
    terminal 'Adriatic LNG'"""

    name = 'AdriaticOperator'
    allowed_domains = ['adriaticlng.com']
    start_urls = [
        (
            'http://www.adriaticlng.com/wps/portal/alng/en/business/!ut/p/c5/'
            'rZDdboJAEIWfxRdgZ2FdlkuoyO9uw58Fbgw1tQGKmmIk8PTF9KY0or3oTDI3X87MOYNyNPahuJTvxbk8HooPlK'
            'KcblWDW8LwZLDWhgpOwi075LESa3Tk2Q8O5hMBh1ng0RXDEKgP1C8oBbKNqv7kDPUQVtD17WfQcX_XcbPu28oQ'
            'HGgvTCyiuh4if-NENXbPvg7ANLxZB6b-HIRZmyx-OZnccjUMf8kBM6U_UrsoL18bqds1EkgyU6gqY4yBXiceU-b'  # noqa
            'z-m9vE37ji1d-x12GcnV2v01Q_I9_nmSlCoElVojMKGMyZihdIWEfmzd0apKL5y9dcy_2guiLL5pXCy0!/dl3/d3'  # noqa
            '/L2dBISEvZ0FBIS9nQSEh/'
        )
    ]

    all_links = []
    url_base = "http://www.adriaticlng.com/wps/portal"
    url_base_2 = "http://www.adriaticlng.com"

    stop_crawling = False

    def __init__(self, start_date=None):
        super(AdriaticSpider, self).__init__(start_date)

    def parse(self, response):
        sel = Selector(response)
        first_link = sel.css('#link-verde ul ul li a::attr(href)').extract()[0]

        return Request(self.url_base + first_link, callback=self.search_in_li)

    def search_in_li(self, response):
        sel = Selector(response)
        link_li = sel.css("#link-verde ul ul li a")

        for link in link_li:
            txt_link = link.css('a::text').re('THERMAL\sYEAR\s[0-9]{4}/[0-9]{2}')
            if len(txt_link) != 0 and txt_link:
                new_url = self.url_base + link.css('a::attr(href)').extract()[0]
                self.all_links.append(new_url)

        return Request(self.all_links.pop(0), callback=self.open_operational_data_label)

    def open_operational_data_label(self, response):
        if self.stop_crawling:
            return None

        sel = Selector(response)
        sub_obj_li = sel.css('#link-verde ul ul li a')
        for sub_obj in sub_obj_li:
            txt = sub_obj.css('a::text').re('OPERATIONAL[\s]*DATA[\s]*LNG')
            if txt:
                self.stop_crawling = True
                new_url = self.url_base + sub_obj.css('a::attr(href)').extract()[0]
                return Request(new_url, callback=self.search_xls_link_inpage)

        return Request(self.all_links.pop(0), callback=self.open_operational_data_label)

    def search_xls_link_inpage(self, response):
        sel = Selector(response)
        url_page = sel.css('#main-interno ul li a::attr(href)').extract()

        if len(url_page) == 0:
            return Request(self.all_links.pop(0), callback=self.open_operational_data_label)

        new_url = self.url_base_2 + url_page[0]
        return Request(new_url, callback=self.parse_xls)

    def parse_xls(self, response):
        xl_obj = ExcelExtractorGLETTemplate(response.body, response.url, self.start_date)
        return xl_obj.parse_sheets()
