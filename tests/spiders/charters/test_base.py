# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import unittest

from kp_scrapers.spiders.charters import CharterSpider


class NoopKlass(CharterSpider):
    pass


class CharterBaseClassTestCase(unittest.TestCase):
    def setUp(self):
        self.noopKlass = NoopKlass()

    def test_charter_category(self):
        self.assertEqual(self.noopKlass.category(), 'charter')

    def test_charter_commos(self):
        self.assertListEqual(self.noopKlass.commodities(), ['coal', 'lng', 'lpg', 'oil', 'cpp'])

    def test_specific_datadog_setup(self):
        expected_settings = {'DATADOG_CUSTOM_TAGS': ['category:charter']}
        self.assertTrue(hasattr(self.noopKlass, 'category_settings'))
        self.assertEqual(self.noopKlass.category_settings, expected_settings)
