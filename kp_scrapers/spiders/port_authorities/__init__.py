# -*- coding: utf-8; -*-

from kp_scrapers.spiders.bases.markers import KplerMixin


class PortAuthoritySpider(KplerMixin):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:port-authority']}

    @classmethod
    def category(cls):
        return 'port-authority'
