# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch, PropertyMock

from kp_scrapers.models.base import strip_meta_fields
from kp_scrapers.spiders.ais.vesselfinder import KEY_MAP, VesselFinderApi
from tests._helpers.date import DateTimeWithChosenNow
from tests._helpers.mocks import fixtures_path, inject_fixture, response_factory


FIXTURE_PATH = fixtures_path('ais', 'vesselfinder')


class VesselFinderResponseTestCase(TestCase):
    def test_key_map_haeding_remove_bad_value(self):
        _, transformer = KEY_MAP['HEADING']
        # we're good, give us as is
        self.assertEqual(transformer(200), 200)
        # we're good, give as an int
        self.assertEqual(transformer('200'), 200)
        # above 360°, remove that
        self.assertIsNone(transformer(4000))


class VesselFinderApiTestCase(TestCase):
    def setUp(self):
        self.response = {
            'provider_id': 'VF API',
            'aisType': 'T-AIS',
            'nextDestination_aisType': 'T-AIS',
            'position_aisType': 'T-AIS',
            'position_timeReceived': '2015-06-12T10:46:46',
            'position_lon': 5.342667,
            'position_lat': 43.332330,
            'position_course': 245,
            'position_speed': 0.1,
            'position_heading': None,
            'master_imo': '7357452',
            'master_name': 'METHANIA',
            'master_callsign': 'ONAE',
            'master_shipType': 81,
            'master_dimA': 191,
            'master_dimB': 52,
            'master_dimC': 17,
            'master_dimD': 17,
            'position_draught': 9.5,
            'master_mmsi': '205194000',
            'nextDestination_destination': 'MARSEILLE',
            'nextDestination_eta': '2015-06-16T10:00:00',
            'position_navState': 0,
        }
        self.responses = [self.response]

    def tearDown(self):
        DateTimeWithChosenNow.chosen_now = None

    def _remove_eta(self):
        for response in self.responses:
            for k in response.copy():
                if k.startswith('nextDestination'):
                    del response[k]

    def _check_ais_msg(self, spider, response):
        idx = None
        for idx, item in enumerate(spider.retrieve_positions(response)):
            for k, v in strip_meta_fields(item).items():
                self.assertEqual(v, self.responses[idx][k])
        self.assertIsNotNone(idx)  # Ensures the loop iterated at least once

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    @inject_fixture('ais/vesselfinder/position-and-eta.json', loader=response_factory)
    def test_response_data(self, response):
        DateTimeWithChosenNow.chosen_now = datetime(2015, 6, 12, 11, 0, 0)
        self._check_ais_msg(VesselFinderApi(apikey='key'), response)

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    @inject_fixture('ais/vesselfinder/position-no-destination.json', loader=response_factory)
    def test_response_data_without_destination(self, response):
        DateTimeWithChosenNow.chosen_now = datetime(2015, 6, 12, 11, 0, 0)

        self._remove_eta()  # Prune fixtures from the ETA part.

        self._check_ais_msg(VesselFinderApi(apikey='key'), response)

    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    @inject_fixture('ais/vesselfinder/position-invalid-eta.json', loader=response_factory)
    def test_response_data_without_eta(self, response):
        DateTimeWithChosenNow.chosen_now = datetime(2015, 6, 12, 11, 0, 0)

        self._remove_eta()

        self._check_ais_msg(VesselFinderApi(apikey='key'), response)

    @inject_fixture(
        'ais/vesselfinder/position-and-eta-with-unknown-key.json', loader=response_factory
    )  # noqa
    @patch('kp_scrapers.spiders.ais.vesselfinder.VesselFinderApi.logger', new=PropertyMock())
    def test_response_data_with_unknown_key(self, response):
        DateTimeWithChosenNow.chosen_now = datetime(2015, 6, 12, 11, 0, 0)
        spider = VesselFinderApi(apikey='key')

        for idx, item in enumerate(spider.retrieve_positions(response)):
            pass
        self.assertIsNotNone(idx)  # Ensures the loop iterated at least once

    @inject_fixture('ais/vesselfinder/error-response.json', loader=response_factory)  # noqa
    def test_response_data_when_error(self, response):
        spider = VesselFinderApi(apikey='key')

        with self.assertRaisesRegexp(RuntimeError, 'an error occurred'):
            for x in spider.retrieve_positions(response):
                pass  # To force consumption of the generator.

    @patch('kp_scrapers.spiders.ais.vesselfinder.VesselFinderApi.logger', new=PropertyMock())
    @patch('kp_scrapers.lib.date.dt.datetime', new=DateTimeWithChosenNow)
    @inject_fixture(
        'ais/vesselfinder/position-and-eta-without-mmsi.json', loader=response_factory
    )  # noqa
    def test_response_data_when_missing_key(self, response):
        DateTimeWithChosenNow.chosen_now = datetime(2015, 6, 12, 11, 0, 0)
        spider = VesselFinderApi(apikey='key')

        idx = None
        for idx, item in enumerate(spider.retrieve_positions(response)):
            pass  # To force consumption of the generator.
        self.assertEqual(idx, 0)

        expected_response = self.responses[0]
        expected_response.pop('master_mmsi')
        self.assertListEqual(list(strip_meta_fields(item).keys()), list(expected_response.keys()))

    def test_start_requests(self):
        spider = VesselFinderApi(apikey='key')

        for request in spider.start_requests():
            pass

        self.assertNotIn('{', request.url)
