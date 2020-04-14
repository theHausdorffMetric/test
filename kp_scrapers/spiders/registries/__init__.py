# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals

from scrapy.spiders import CrawlSpider

# it's actualy not true for FPSO spider but since it's deprecated, we're taking the shortcut
from kp_scrapers.spiders.bases.markers import CoalMarker, CppMarker, LngMarker, LpgMarker, OilMarker


class RegistrySpider(LngMarker, LpgMarker, OilMarker, CppMarker, CoalMarker, CrawlSpider):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:registry']}

    @classmethod
    def category(cls):
        return 'registry'
