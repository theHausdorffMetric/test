# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider

from kp_scrapers.spiders.bases.markers import KplerMixin


class PriceSpider(KplerMixin, Spider):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:price']}

    @classmethod
    def category(cls):
        return 'price'
