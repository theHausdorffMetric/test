# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
import unittest
from unittest.mock import patch

import kp_scrapers.lib.utils as utils


class UtilsTestCase(unittest.TestCase):
    def test_bad_item_validation(self):
        self.assertFalse(utils.is_valid_item({}, ['foo']))

    def test_good_item_validation(self):
        self.assertTrue(utils.is_valid_item({'foo': 'bar'}, ['foo']))

    def test_good_and_bad_item_validation(self):
        self.assertFalse(utils.is_valid_item({'foo': 'bar'}, ['foo', 'fizz']))

    def test_mapper(self):
        key_map = {'foo': ('bar', int)}
        self.assertEqual(utils.map_keys({'foo': '3'}, key_map), {'bar': 3})

    def test_mapper_ignore(self):
        key_map = {'foo': ('bar', None)}
        self.assertEqual(utils.map_keys({'foo': '3'}, key_map), {'bar': '3'})
        key_map = {'foo': (None, int)}
        self.assertEqual(utils.map_keys({'foo': '3'}, key_map), {})

    @patch('kp_scrapers.lib.utils.time.sleep')
    def test_random_delay(self, sleep_mock):
        # GIVEN
        @utils.random_delay(average=1)
        def func(l):
            for e in l:
                yield e

        test_list = ['a', 'b', 'c']

        # WHEN
        res_list = [v for v in func(test_list)]

        # THEN
        sleep_mock.assert_called_once()
        self.assertEqual(res_list, test_list)
