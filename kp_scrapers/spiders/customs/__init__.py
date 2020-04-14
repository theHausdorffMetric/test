# -*- coding: utf-8; -*-

from __future__ import absolute_import

from scrapy import Spider

from kp_scrapers.spiders.bases.markers import KplerMixin


class CustomsSpider(KplerMixin, Spider):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:customs']}

    @classmethod
    def category(cls):
        return 'customs'
