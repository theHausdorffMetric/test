# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from collections import Iterable
import datetime as dt
import unittest
from unittest.mock import patch

from scrapy import Selector

from kp_scrapers.spiders.tropical_storm.jtwc.parser import CycloneKMLParser
from tests._helpers.date import DateTimeWithChosenNow
from tests._helpers.mocks import FakeXmlResponse, fixtures_path


MODULE = 'kp_scrapers.spiders.tropical_storm.jtwc'


class JTWCParserTestCase(unittest.TestCase):
    def setUp(self):
        fake_file = fixtures_path('weather', 'jtwc_doc.kml')
        self.mock_cyclones_index = FakeXmlResponse(fake_file)
        DateTimeWithChosenNow.choosen_now = dt.datetime(2017, 6, 12, 0, 0)

    def tearDown(self):
        DateTimeWithChosenNow.chosen_now = None

    def _new_kml_parser(self):
        return CycloneKMLParser(Selector(text=self.mock_cyclones_index.body))

    def _assert_is_forecast(self, forecast):
        for key in ['date', 'position', 'wind']:
            # TODO better check the data consistency
            self.assertTrue(forecast.get(key))

    @patch('{}.parser.dt.datetime'.format(MODULE), new=DateTimeWithChosenNow)
    def test_cyclone_name(self):
        # name depends on the report year
        DateTimeWithChosenNow.chosen_now = dt.datetime(2017, 6, 12, 0, 0)

        doc = self._new_kml_parser()
        self.assertEqual(doc.cyclone_name, '201705E')

    def test_parse_forecast(self):
        doc = self._new_kml_parser()
        data = doc.forecast_track
        self.assertTrue(isinstance(data, Iterable))

        for forecast in data:
            self._assert_is_forecast(forecast)
