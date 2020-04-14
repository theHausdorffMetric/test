# -*- coding: utf-8; -*-

from kp_scrapers.spiders.bases.markers import KplerMixin


class MarketSpider(KplerMixin):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:market']}

    @classmethod
    def category(cls):
        return 'market'
