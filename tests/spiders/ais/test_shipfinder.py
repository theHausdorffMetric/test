# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime
import os
from unittest import skip, TestCase
from unittest.mock import patch, PropertyMock

import pytz

from kp_scrapers.models.base import strip_meta_fields
from kp_scrapers.spiders.ais.shipfinder import ShipFinderSpider
from tests._helpers.mocks import FakeTextResponse, fixtures_path, ForcedCollection, response_factory


FIXTURE_PATH = fixtures_path('ais', 'shipfinder')
VESSELS_MODULE = 'kp_scrapers.lib.static_data.vessels'


@skip('shipfinder source is deprecated')
class ShipFinderTestCase(TestCase):
    def setUp(self):
        self._vessel_list = [
            {'name': 'KIKYO', 'imo': '9415703', 'mmsi': '477744900', 'callsign': 'VRGR7'}
        ]
        self._vessel_dict = {v['mmsi']: v for v in self._vessel_list}
        # The content of the item expected to be yielded by the parse function.
        self._item = {
            'master_name': 'KIKYO',
            'master_imo': '9415703',
            'master_mmsi': '477744900',
            'master_callsign': 'VRGR7',
            'position_aisType': 'T-AIS',
            'position_course': 90.2,
            'position_draught': 11.8,
            'position_heading': 91,
            'position_lat': 5.702933,
            'position_lon': 80.408952,
            'position_navState': 0,
            'position_speed': 8694 / (514.444444),
            'position_timeReceived': datetime(
                2015, 6, 18, 7, 1, 59, 0, tzinfo=pytz.utc
            ).isoformat(),
            'aisType': 'T-AIS',
            'provider_id': ShipFinderSpider.PROVIDER_SHORTNAME,
            'nextDestination_destination': "IND_VIZAG           ",
            'nextDestination_eta': datetime(2015, 6, 20, 18, 0, 0, 0).isoformat(),
        }

        with open(os.path.join(FIXTURE_PATH, 'spiders_data_mock.two-vessels.json'), 'r') as f:
            self._list_of_two_vessels = f.read()

        with open(os.path.join(FIXTURE_PATH, 'spiders_data_mock.one-vessel.json'), 'r') as f:
            self._list_of_one_vessel = f.read()

    def test_ship_finder_valid_response(self):
        resp = response_factory(
            os.path.join(FIXTURE_PATH, 'response_mmsi_477744900.json'),
            klass=FakeTextResponse,
            meta={'vessels': self._vessel_dict},
        )
        spider = ShipFinderSpider()
        with patch(VESSELS_MODULE, ForcedCollection(self._list_of_one_vessel)):
            spider.parse(resp)

    def test_ship_finder_valid_response_with_missing_vessel(self):
        resp = response_factory(
            os.path.join(FIXTURE_PATH, 'response_mmsi_477744900.json'),
            klass=FakeTextResponse,
            meta={'vessels': self._vessel_dict},
        )
        with patch(
            'kp_scrapers.spiders.ais.shipfinder.ShipFinderSpider.logger', new_callable=PropertyMock
        ):
            spider = ShipFinderSpider()
            with patch(VESSELS_MODULE, ForcedCollection(self._list_of_two_vessels)):
                spider.parse(resp)

    def test_ship_finder_valid_response_with_vessel_twice(self):
        resp = response_factory(
            os.path.join(FIXTURE_PATH, 'response_twice_mmsi_477744900.json'),
            klass=FakeTextResponse,
            meta={'vessels': self._vessel_dict},
        )
        spider = ShipFinderSpider()
        with patch(VESSELS_MODULE, ForcedCollection(self._list_of_two_vessels)):
            for item in spider.parse(resp):
                for key in strip_meta_fields(item):
                    self.assertEqual(item[key], self._item[key])

    def test_ship_finder_valid_response_no_eta(self):
        resp = response_factory(
            os.path.join(FIXTURE_PATH, 'response_mmsi_477744900.no-eta.json'),
            klass=FakeTextResponse,
            meta={'vessels': self._vessel_dict},
        )

        for key in ['nextDestination_destination', 'nextDestination_eta']:
            del self._item[key]
        spider = ShipFinderSpider()
        with patch('kp_scrapers.spiders.ais.shipfinder.ShipFinderSpider.logger') as logger_mock:
            with patch(VESSELS_MODULE, ForcedCollection(self._list_of_two_vessels)):
                for item in spider.parse(resp):
                    for key in self._item:
                        self.assertEqual(
                            item[key],
                            self._item[key],
                            'Key {}: {} != {}'.format(key, item[key], self._item[key]),
                        )

        logger_mock.warning.assert_called_with(
            "ShipFinder's response for vessel {} contains no ETA.".format(
                self._vessel_list[0]['imo']
            )
        )

    def test_ship_finder_invalid_http_response(self):
        resp = response_factory(
            os.path.join(FIXTURE_PATH, 'response_mmsi_477744900.json'),
            klass=FakeTextResponse,
            meta={'vessels': self._vessel_dict},
        )
        resp.status = 404

        with patch('kp_scrapers.spiders.ais.shipfinder.ShipFinderSpider.logger') as logger_mock:
            spider = ShipFinderSpider()
            with patch(VESSELS_MODULE, ForcedCollection(self._list_of_two_vessels)):
                for item in spider.parse(resp):
                    pass

        logger_mock.error.assert_called_with(
            'Bad HTTP response from shipfinder API: {}'.format(resp.body)
        )

    def test_ship_finder_invalid_json_response(self):
        self._vessel_dict['533189000'] = self._vessel_dict['477744900']
        resp = response_factory(
            os.path.join(FIXTURE_PATH, 'response_mmsi_533189000-invalid.json'),
            klass=FakeTextResponse,
            # Content of meta does not really matters here.
            meta={'vessels': self._vessel_dict},
        )
        spider = ShipFinderSpider()
        count = 0
        for item in spider.parse(resp):
            count += 1

        self.assertEqual(count, 0)

    def test_ship_finder_valid_response_bad_eta(self):
        # Ship Finder may give bad eta "02-30 00:28" for 30th of February.
        # Check that "ValueError: day is out of range for month" does not raise
        # when trying to parse date
        resp = response_factory(
            os.path.join(FIXTURE_PATH, 'response_mmsi_477744900.bad-eta.json'),
            klass=FakeTextResponse,
            meta={'vessels': self._vessel_dict},
        )
        spider = ShipFinderSpider()
        for item in spider.parse(resp):
            pass
