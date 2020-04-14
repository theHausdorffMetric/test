# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider

from kp_scrapers.spiders.bases.markers import KplerMixin


class ContractSpider(KplerMixin, Spider):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:contract']}

    @classmethod
    def category(cls):
        return 'contract'
