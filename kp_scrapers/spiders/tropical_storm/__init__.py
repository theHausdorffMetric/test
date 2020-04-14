# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals

from scrapy import Spider

from kp_scrapers.spiders.bases.markers import CoalMarker, CppMarker, LngMarker, LpgMarker, OilMarker


class TropicalStormSpider(LngMarker, LpgMarker, OilMarker, CppMarker, CoalMarker, Spider):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:tropical_storm']}

    @classmethod
    def category(cls):
        return 'tropical_storm'
