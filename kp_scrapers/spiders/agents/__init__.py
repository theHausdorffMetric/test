# -*- coding: utf-8; -*-

from __future__ import absolute_import

from kp_scrapers.spiders.bases.markers import KplerMixin


class ShipAgentMixin(KplerMixin):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:ship-agent']}

    @classmethod
    def category(cls):
        return 'ship-agent'
