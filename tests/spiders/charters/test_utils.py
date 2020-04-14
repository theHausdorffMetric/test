# -*- coding: utf-8; -*-
from __future__ import absolute_import, unicode_literals
from unittest import TestCase

from kp_scrapers.spiders.charters.utils import parse_arrival_zones


class ParseArrivalZonesTestCase(TestCase):
    def test_simple_zone(self):
        raw_arrival_zone = 'WAF'
        arrival_zone = parse_arrival_zones(raw_arrival_zone)
        self.assertEqual(['WAF'], arrival_zone)

    def test_several_zones(self):
        raw_arrival_zones = 'WAF-UKCM-USGC'
        arrival_zones = parse_arrival_zones(raw_arrival_zones)
        self.assertEqual(['WAF', 'UKCM', 'USGC'], arrival_zones)

    def test_simple_zone_east(self):
        raw_arrival_zone = 'FEAST'
        arrival_zone = parse_arrival_zones(raw_arrival_zone)
        self.assertEqual(['Eastern Asia'], arrival_zone)

    def test_several_zones_east(self):
        raw_arrival_zones = 'WAF-UKCM-EAST'
        arrival_zones = parse_arrival_zones(raw_arrival_zones)
        self.assertEqual(['WAF', 'UKCM', 'Eastern Asia'], arrival_zones)
