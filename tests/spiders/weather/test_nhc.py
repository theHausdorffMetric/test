# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import unittest

from scrapy import Selector

from kp_scrapers.spiders.tropical_storm.nhc.parser import find_active_blocks, find_info_link
from tests._helpers.mocks import FakeXmlResponse, fixtures_path


class NHCParserTestCase(unittest.TestCase):
    def setUp(self):
        fake_file = fixtures_path('weather', 'nhc_active.kml')
        self.mock_hurricanes_index = FakeXmlResponse(fake_file)

    def test_find_active_blocks(self):
        sel = Selector(text=self.mock_hurricanes_index.body)
        actives = find_active_blocks(sel)
        self.assertTrue(len(actives) == 1)

    def test_find_cone_link(self):
        i = -1
        sel = Selector(text=self.mock_hurricanes_index.body)
        for i, el in enumerate(find_active_blocks(sel)):
            res = find_info_link(el, 'cone')
            self.assertIsNotNone(res)
        # make sure we actually tested something (enumerate is 0-based)
        self.assertTrue(i == 0)

    def test_find_track_link(self):
        i = -1
        sel = Selector(text=self.mock_hurricanes_index.body)
        for i, el in enumerate(find_active_blocks(sel)):
            res = find_info_link(el, 'track')
            self.assertIsNotNone(res)
        # make sure we actually tested something (enumerate is 0-based)
        self.assertTrue(i == 0)
