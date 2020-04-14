# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy.http import Request
from scrapy.selector import Selector

from kp_scrapers.spiders.bases.persist import PersistSpider

from . import OperatorSpider
from .extractors.italia import ExcelExtractorGLETTemplate


class LaSpeziaOperator(OperatorSpider, PersistSpider):

    name = 'LaSpeziaOperator'
    allowed_domains = ['gnlitalia.it', 'snamretegas.it']
    start_urls = ('http://www.gnlitalia.it/en/business_services/transparency_template/',)

    all_links = []
    url_base = 'http://www.gnlitalia.it'

    # There is 3 level of label : Thermal Years -> Planning LNG -> Operational Data

    def parse(self, response):
        sel = Selector(response)
        link_li = sel.css('#interno_col_sx #menu ul li a')

        for link in link_li:
            txt_link = link.css('a span::text').re('Thermal\sYear\s[0-9]{4}/[0-9]{4}')
            if len(txt_link) != 0 and txt_link:
                new_url = self.url_base + link.css("a::attr(href)").extract()[0]
                self.all_links.append(new_url)

        return Request(self.all_links.pop(0), callback=self.open_planning_label)

    def open_planning_label(self, response):
        sel = Selector(response)
        links = sel.css('#interno_col_sx #menu ul li a')

        for link in links:
            link_title = link.css("a::attr(title)").extract()
            if len(link_title) > 0 and link_title[0] == u'Planning of the deliveries of LNG':
                new_url = self.url_base + link.css('a::attr(href)').extract()[0]
                return Request(new_url, callback=self.open_operation_data_label)

        return Request(self.all_links.pop(0), callback=self.open_planning_label)

    def open_operation_data_label(self, response):
        sel = Selector(response)
        top_li_labels = sel.css('#interno_col_dx .top_link_bx .top_list_link li')
        for label in top_li_labels:
            span_txt = label.css('span::text').extract()[0]
            if span_txt == 'Operational Data':
                new_url = self.url_base + label.css('a::attr(href)').extract()[0]
                return Request(new_url, callback=self.select_download_link)
        return Request(self.all_links.pop(0), callback=self.open_planning_label)

    def select_download_link(self, response):
        sel = Selector(response)
        new_url = sel.css('.form_downFile .multy_list a::attr(href)').extract()[0]
        return Request(new_url, callback=self.parse_xls)

    def parse_xls(self, response):
        xl_obj = ExcelExtractorGLETTemplate(response.body, response.url, self.start_date)
        return xl_obj.parse_sheets()
