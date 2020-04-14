# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals

from kp_scrapers.spiders.bases.markers import LngMarker


class OperatorSpider(LngMarker):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:port-operator']}

    @classmethod
    def category(cls):
        return 'port-operator'
