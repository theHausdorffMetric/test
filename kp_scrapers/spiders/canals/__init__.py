# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals

from kp_scrapers.spiders.bases.markers import KplerMixin, SpiderCategory


class CanalSpider(KplerMixin):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:canal']}

    @classmethod
    def category(cls):
        return SpiderCategory.canal.value
