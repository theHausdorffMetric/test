# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import unittest

from kp_scrapers.models.items import SpotCharter
from kp_scrapers.models.utils import filter_item_fields


class ModelsUtilsTestCase(unittest.TestCase):
    def test_filter_item_fields(self):
        original_item = {
            'departure_zone': 'ZONE',
            'arrival_zone': 'ZONE',
            'reported_date': 'a date',
            'rate_value': '80',
            'rate_raw_value': 'W80',
            'lay_can_start': '017-04-26',
            'charterer': 'CHARTERER',
            'vessel': 'a vessel item',
            'cargo': 'a cargo item',
            # they are not wanted by the SpotCharter item
            'commodity': 'a commodity',
            'dwt': '120',
            'vessel_name': 'NAME',
        }
        filtered_item = {
            'departure_zone': 'ZONE',
            'arrival_zone': 'ZONE',
            'reported_date': 'a date',
            'rate_value': '80',
            'rate_raw_value': 'W80',
            'lay_can_start': '017-04-26',
            'charterer': 'CHARTERER',
            'vessel': 'a vessel item',
            'cargo': 'a cargo item',
        }

        self.assertEqual(filter_item_fields(SpotCharter, original_item), filtered_item)
