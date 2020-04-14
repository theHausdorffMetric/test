# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import re
import unittest

from scrapy.item import Field, Item

from kp_scrapers import __version__
from kp_scrapers.models.base import VersionedItem


# uuid v4 regex matcher (e.g. '31cda1df-e350-45c5-98fa-55f879b196b7')
UUID_REGEX = '^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z'
uuid4hex = re.compile(UUID_REGEX, re.I)


class BaseItemTestCase(unittest.TestCase):
    def setUp(self):
        class ValidItem(VersionedItem, Item):
            some_field = Field()

        self.valid_item = ValidItem(some_field='some_value')

    @staticmethod
    def _is_uuid_v4(candidate):
        return uuid4hex.match(candidate) is not None

    def test_default_initialization(self):
        self.assertEqual(self.valid_item['kp_package_version'], __version__)
        self.assertTrue(self._is_uuid_v4(self.valid_item['kp_uuid']))

    def test_child_item(self):
        class SomeItem(VersionedItem):
            some_field = Field()

        item = SomeItem(some_field='a value')
        self.assertEqual(item['some_field'], 'a value')
        self.assertEqual(item['kp_package_version'], __version__)
        self.assertTrue(self._is_uuid_v4(item['kp_uuid']))

    def test_default_parent_id(self):
        self.assertIsNone(self.valid_item.get('kp_parent_uuid'))

    def test_custom_parent_id(self):
        class SomeItem(VersionedItem):
            some_field = Field()

        item = SomeItem(kp_parent_uuid='1234')
        self.assertEqual(item['kp_parent_uuid'], '1234')
