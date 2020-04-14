# -*- coding: utf-8; -*-
from __future__ import absolute_import, unicode_literals

from scrapy.spiders import Spider


STATUS_DETAILS_ALLOWED = ['In Service/Commission', 'Launched']


class AisFleetSpider(Spider):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:ais-fleet']}

    @classmethod
    def category(cls):
        return 'ais-fleet'
