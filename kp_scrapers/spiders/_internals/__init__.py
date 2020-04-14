# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider

from kp_scrapers.spiders.bases.markers import KplerMixin


class InternalSpider(Spider, KplerMixin):
    """Spiders that don't serve data sources."""

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:internal']}

    @classmethod
    def category(cls):
        return 'internal'
