# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import unittest
from unittest.mock import patch

from kp_scrapers.lib import static_data
from tests._helpers.mocks import fixtures_path


ORIGINAL_CACHE_PATH = static_data._BASE_LOCAL_CACHE


def gen_items(*args, **kwargs):
    for _ in list(range(5)):
        yield {
            'status': 'Under Construction',
            '_env': ['lpg_production'],
            'name': 'Hanne',
            'providers': ['EE', 'VF API', 'MT_API'],
            'mmsi': None,
            'imo': '9712553',
            'status_detail': 'Launched',
            '_markets': ['lpg'],
            'call_sign': None,
        }


class StaticDataTestCase(unittest.TestCase):
    @patch('kp_scrapers.lib.services.s3.fetch_file', new=gen_items)
    def test_vessels_collection_shortcut_init_with_local_cache(self):
        fleet = static_data.vessels()
        self.assertTrue(isinstance(fleet, static_data.Collection))
        self.assertTrue(isinstance(fleet, list))
        self.assertEqual(fleet.index, 'imo')

    def test_vessels_collection_from_local_cache(self):
        static_data._BASE_LOCAL_CACHE = fixtures_path()

        fleet = static_data.vessels()
        self.assertTrue(isinstance(fleet, static_data.Collection))
        for vessel in fleet:
            self.assertTrue(vessel.get('imo'))

        static_data._BASE_LOCAL_CACHE = ORIGINAL_CACHE_PATH

    def test_fleet_selection_not_eligible(self):
        fleet = static_data.fetch_kpler_fleet(lambda x: False)
        self.assertEqual(len(list(fleet)), 0)

    def test_fleet_selection_without_filter(self):
        # TODO ensure we're dealing with the mock dataset in debug mode
        for vessel in static_data.fetch_kpler_fleet(lambda x: True):
            self.assertTrue(vessel)
