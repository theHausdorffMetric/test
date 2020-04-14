# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals

from kp_scrapers.spiders.bases.markers import LngMarker


class SlotSpider(LngMarker):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:slot']}

    @classmethod
    def category(cls):
        return 'slot'
