# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import datetime
import unittest
from unittest.mock import Mock, patch

from kp_scrapers.models.base import strip_meta_fields
from kp_scrapers.models.items import Vessel
from kp_scrapers.spiders.registries.equasis import parser
from tests._helpers.mocks import selector_source


class TestEquasisParser(unittest.TestCase):
    def test_parse_date_with_small_day(self):
        # GIVEN
        expected_dt = datetime.datetime(2017, 3, 12, 0, 0)

        # WHEN
        iso_dt = parser.parse_date(
            'some words 12/03/2017 before and after',
            exclude=['some', 'words', 'before', 'and', 'after'],
        )

        # THEN
        self.assertEqual(iso_dt, expected_dt.isoformat())

    def test_parse_date_with_large_day(self):
        # GIVEN
        expected_dt = datetime.datetime(2017, 3, 13, 0, 0)

        # WHEN
        iso_dt = parser.parse_date(
            'some words 13/03/2017 before and after',
            exclude=['some', 'words', 'before', 'and', 'after'],
        )

        # THEN
        self.assertEqual(iso_dt, expected_dt.isoformat())

    def test_parse_date_none(self):
        # WHEN
        iso_dt = parser.parse_date(None)

        # THEN
        self.assertEqual(iso_dt, '')

    @patch('kp_scrapers.spiders.registries.equasis.parser._parse_base_vessel')
    @patch('kp_scrapers.spiders.registries.equasis.parser._parse_table')
    @patch('kp_scrapers.spiders.registries.equasis.parser._parse_classification')
    def test_parse_vessel_details(self, classification, _, base_vessel):
        # GIVEN
        # all the required fields
        base_vessel.return_value = {'imo': '1111111', 'type': 'Gaz Tanker', 'name': 'base_item'}
        classification.return_value = {'classification_statuses': [], 'classification_surveys': []}

        # WHEN
        item = parser.parse_vessel_details(Mock())

        # THEN
        # meaning it was validated
        self.assertIsNotNone(item)
        self.assertTrue('classification_statuses' in item)
        self.assertTrue('classification_surveys' in item)
        self.assertTrue('companies' in item)
        # TODO commented out in spider, to discuss with analysts on value
        # self.assertTrue('safety_certificates' in item)

    @patch('kp_scrapers.spiders.registries.equasis.parser._parse_base_vessel')
    @patch('kp_scrapers.spiders.registries.equasis.parser._parse_table')
    @patch('kp_scrapers.spiders.registries.equasis.parser._parse_classification')
    def test_parse_invalid_vessel_details(self, classification, _, base_vessel):
        # GIVEN
        # none of the required field
        base_vessel.return_value = {}

        # WHEN
        item = parser.parse_vessel_details(Mock())

        # THEN
        # meaning it was validated
        self.assertIsNone(item)


class TestEquasisParserIntegration(unittest.TestCase):
    @selector_source('equasis/search/base.htm')
    def test_parse_imos_from_search_results(self, selector):
        # GIVEN
        expected_imos = {
            '9806639',
            '9735531',
            '9775983',
            '9369576',
            '9778703',
            '9745251',
            '9756248',
            '9745263',
            '9645437',
            '9464429',
            '9756224',
            '9589279',
            '9808663',
            '9820362',
            '9804564',
        }

        # WHEN
        imos = parser.parse_imos_from_search_results(selector)

        # THEN
        self.assertSetEqual(imos, expected_imos)

    @selector_source('equasis/search/base.htm')
    def test_parse_page_links_from_search(self, selector):
        # GIVEN
        expected_page_links = ['1', '2', '3', '4', '5', '6', '7', '>', '\xbb']

        # WHEN
        page_links = parser.parse_page_links(selector)

        # THEN
        self.assertEqual(page_links, expected_page_links)

    @selector_source('equasis/search/last_page.htm')
    def test_parse_page_links_from_search_last_page(self, selector):
        # GIVEN
        expected_page_links = ['\xab', '<', '49', '50', '51', '52', '53', '54', '55']

        # WHEN
        page_links = parser.parse_page_links(selector)

        # THEN
        self.assertEqual(page_links, expected_page_links)

    @selector_source('equasis/vessel/base.htm')
    def test_parse_base_vessel(self, selector):
        # GIVEN
        expected_vessel = Vessel(
            {
                'build_year': 2017,
                'call_sign': 'SVCO6',
                'dead_weight': 158871,
                'flag_name': 'Greece',
                'gross_tonnage': 81349,
                'imo': '9745263',
                'mmsi': '241492000',
                'name': 'AEGEAN FIGHTER',
                'status': 'Launched',
                # TODO not required in VesselRegistry model,
                # clarify with analysts on criticality
                # 'status_date': '2016-12-31T00:00:00',
                'type': 'Crude Oil Tanker',
                'reported_date': '2017-03-28T00:00:00',
            }
        )

        # WHEN
        vessel = parser._parse_base_vessel(selector)

        # THEN
        self.assertEqual(strip_meta_fields(vessel), strip_meta_fields(expected_vessel))

    @selector_source('equasis/vessel/no_mmsi.htm')
    def test_parse_base_vessel_no_mmsi(self, selector):
        # GIVEN
        expected_vessel = Vessel(
            {
                'build_year': 1983,
                'call_sign': 'LAMW4',
                'dead_weight': 56174,
                'flag_name': 'Norway NIS',
                'gross_tonnage': 50699,
                'imo': '8016809',
                'name': 'BERGE FROST',
                'status': 'Broken Up',
                # TODO not required in VesselRegistry model,
                # clarify with analysts on criticality
                # 'status_date': '2011-03-04T00:00:00',
                'type': 'LPG Tanker',
                'reported_date': '2016-11-04T00:00:00',
            }
        )

        # WHEN
        vessel = parser._parse_base_vessel(selector)

        # THEN
        self.assertEqual(strip_meta_fields(vessel), strip_meta_fields(expected_vessel))
