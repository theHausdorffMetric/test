# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
import os
from unittest import TestCase
from unittest.mock import patch

from scrapy import Selector

from kp_scrapers.models.base import strip_meta_fields
from kp_scrapers.spiders.ais.marinetraffic import MarineTrafficSpider
from tests._helpers.mocks import (
    FakeResponse,
    FakeXmlResponse,
    fixtures_path,
    ForcedCollection,
    response_factory,
)


FIXTURE_PATH = fixtures_path('ais', 'marinetraffic')
VESSELS_MODULE = 'kp_scrapers.lib.static_data.vessels'


class MarineTrafficTestCase(TestCase):
    def setUp(self):
        self.vessel = {
            'imo': '0000000',
            'name': 'Some Vessel',
            'mmsi': '111111111',
            'status': 'Active',
            'status_detail': 'In Service/Commission',
        }
        self.api_key = '123456'

    def test_parse_response(self):
        response = FakeXmlResponse(os.path.join(FIXTURE_PATH, 'position-and-eta.xml'))
        result = Selector(response)
        spider = MarineTrafficSpider(fleet_name='MT_API', msgtype='extended')
        responses = [
            {
                'aisType': 'T-AIS',
                'master_imo': '7357452',
                'master_mmsi': '205194000',
                'master_callsign': 'ONAE',
                'master_name': 'METHANIA',
                # 'master_shipType': 'tankships',
                'nextDestination_aisType': 'T-AIS',
                'nextDestination_destination': 'MARSEILLE',
                'nextDestination_eta': '2015-12-16T10:00:00',
                'position_aisType': 'T-AIS',
                'position_course': '245',
                'position_draught': '9.5',
                'position_lat': '43.332330',
                'position_lon': '5.342667',
                'position_navState': None,
                'position_speed': 0.1,
                'position_timeReceived': '2015-06-12T10:46:46',
                'provider_id': 'MT_API',
            }
        ]

        idx = None
        for idx, item in enumerate(spider._parse_response(result)):
            relevant_fields = list(strip_meta_fields(item).keys())
            for key in relevant_fields:
                self.assertEqual(
                    item.get(key),
                    responses[idx].get(key),
                    "Key: '{}': {} != {}".format(
                        key, repr(item.get(key)), repr(responses[idx].get(key))
                    ),
                )

            # Ensure the item and our test dict have the same number of keys.
            self.assertEqual(len(relevant_fields), len(responses[idx]))
        self.assertIsNotNone(idx)  # Ensures the loop iterated at least once

    def test_get_vessel_fleet(self):
        api_key = '123456'
        spider = MarineTrafficSpider(
            show_fleet='', getkey=api_key, fleet_name='FLEET', fleet_id='xyz'
        )
        idx = -1
        for idx, item in enumerate(spider.get_vessel_fleet()):
            self.assertIn('/api/getfleet/{}/fleet_id:xyz'.format(self.api_key), item.url)
            self.assertIs(item.callback, spider.show_fleet)

        self.assertEqual(idx, 0)

    def test_get_vessel_fleet_with_callback(self):
        def callback(a, b):
            pass

        spider = MarineTrafficSpider(show_fleet='', getkey=self.api_key, fleet_name='FLEET')
        idx = -1
        for idx, item in enumerate(spider.get_vessel_fleet(cb=callback)):
            self.assertIn('/api/getfleet/{}'.format(self.api_key), item.url)
            self.assertIs(item.callback, callback)

        self.assertEqual(idx, 0)

    def test_start_requests_when_fleet_id_is_none(self):
        spider = MarineTrafficSpider(poskey=self.api_key, fleet_name='MT_API', fleet_id=None)

        idx = None
        for idx, item in enumerate(spider.start_requests()):
            self.assertFalse(True)  # Unreachable

        self.assertIsNone(idx)

    def test_start_requests_positions(self):
        spider = MarineTrafficSpider(poskey=self.api_key, fleet_name='MT_API', fleet_id='xyz')

        idx = -1
        for idx, item in enumerate(spider.start_requests()):
            self.assertIn(
                '/exportvessels/{}/v:8/timespan:{}/msgtype:{}'.format(
                    self.api_key, spider.TIMESPAN, spider.MSGTYPE
                ),
                item.url,
            )
            self.assertEqual(item.callback, spider.parse)

        self.assertEqual(idx, 0)

    def test_start_requests_extended_positions(self):
        msgtype = 'EXTENDED'
        spider = MarineTrafficSpider(
            poskey=self.api_key, fleet_name='MT_API', msgtype=msgtype, fleet_id='xyz'
        )

        idx = -1
        for idx, item in enumerate(spider.start_requests()):
            self.assertIn(
                '/exportvessels/{}/v:8/timespan:{}/msgtype:{}'.format(
                    self.api_key, spider.TIMESPAN, msgtype.lower()
                ),
                item.url,
            )
            self.assertEqual(item.callback, spider.parse)

        self.assertEqual(idx, 0)

    def test_spider_init_wrong_msg_type(self):
        msgtype = 'WRONG'

        with patch('kp_scrapers.spiders.ais.marinetraffic.MarineTrafficSpider.logger'):
            spider = MarineTrafficSpider(
                poskey=self.api_key, fleet_name='MT_API', msgtype=msgtype, fleet_id='xyz'
            )
            idx = -1
            for idx, item in enumerate(spider.start_requests()):
                self.assertIn(
                    '/exportvessels/{}/v:8/timespan:{}/msgtype:{}'.format(
                        self.api_key, spider.TIMESPAN, spider.MSGTYPE
                    ),
                    item.url,
                )
                self.assertEqual(item.callback, spider.parse)

            self.assertEqual(idx, 0)

            spider.logger.warning.assert_called_once_with(
                'Unknown message type "{}", will use default instead: "{}"'.format(
                    msgtype.lower(), spider.MSGTYPE
                )
            )

    def test_spider_init_mutually_exclusive_actions(self):
        with patch('kp_scrapers.spiders.ais.marinetraffic.MarineTrafficSpider.logger'):
            spider = MarineTrafficSpider(
                getkey=self.api_key,
                fleet_name='MT_API',
                fleet='true',
                showfleet='true',
                fleet_id='xyz',
            )
            idx = -1
            for idx, item in enumerate(spider.start_requests()):
                self.assertIn('/api/getfleet/{}/fleet_id:xyz'.format(self.api_key), item.url)
                self.assertEqual(item.callback, spider.show_fleet)

            self.assertEqual(idx, 0)

            spider.logger.warning.assert_called_once_with(
                'Contradictory arguments: cannot update and '
                'show fleet at once. Will assume command is '
                'show fleet.'
            )

    def test_start_requests_show_fleet(self):
        spider = MarineTrafficSpider(
            showfleet='true', getkey=self.api_key, fleet_name='MT_API', fleet_id='xyz'
        )

        idx = -1
        for idx, item in enumerate(spider.start_requests()):
            self.assertIn('/api/getfleet/{}/fleet_id:xyz'.format(self.api_key), item.url)
            self.assertEqual(item.callback, spider.show_fleet)

        self.assertEqual(idx, 0)

    def test_start_requests_update_fleet(self):
        spider = MarineTrafficSpider(
            fleet='true', getkey=self.api_key, fleet_name='MT_API', fleet_id='xyz'
        )

        idx = -1
        for idx, item in enumerate(spider.start_requests()):
            self.assertIn('/api/getfleet/{}/fleet_id:xyz'.format(self.api_key), item.url)
            self.assertEqual(item.callback, spider.update_fleet)

        self.assertEqual(idx, 0)

    def test_start_requests_no_fleet_name(self):
        spider = MarineTrafficSpider(getkey=self.api_key, fleet_id='xyz')

        with patch('kp_scrapers.spiders.ais.marinetraffic.MarineTrafficSpider.logger'):
            idx = -1
            for idx, item in enumerate(spider.start_requests()):
                self.assertIn('/api/getfleet/{}/fleet_id:xyz'.format(self.api_key), item.url)
                self.assertIs(item.callback, spider.show_fleet)

            spider.logger.error.assert_called_once_with("invalid fleet name specified")

        self.assertEqual(idx, -1)

    def test_start_requests_wrong_fleet_name(self):
        spider = MarineTrafficSpider(getkey=self.api_key, fleet_name='FLEET', fleet_id='xyz')

        with patch('kp_scrapers.spiders.ais.marinetraffic.MarineTrafficSpider.logger'):
            idx = -1
            for idx, item in enumerate(spider.start_requests()):
                self.assertIn('/api/getfleet/{}/fleet_id:xyz'.format(self.api_key), item.url)
                self.assertIs(item.callback, spider.show_fleet)

            spider.logger.error.assert_called_once_with("invalid fleet name specified")

        self.assertEqual(idx, -1)

    def test_error_response(self):
        response = FakeResponse(os.path.join(FIXTURE_PATH, 'error-response.xml'))
        spider = MarineTrafficSpider()
        with patch('kp_scrapers.spiders.ais.marinetraffic.MarineTrafficSpider.logger'):
            for item in spider.parse(response):
                pass

            spider.logger.error.assert_called_once_with(
                "Data retrieval error: {message}".format(message="ABOVE SERVICE CALL LIMIT")
            )

    def test_vessel_added_error(self):
        response = response_factory(
            os.path.join(FIXTURE_PATH, 'imo-not-found.xml'), meta={'vessel': self.vessel}
        )
        with patch('kp_scrapers.spiders.ais.marinetraffic.MarineTrafficSpider.logger'):
            spider = MarineTrafficSpider(fleet='True')

            spider.check_vessel_was_added(response)

            spider.logger.error.assert_called_once_with(
                "Could not add vessel {name} (IMO: {imo}, MMSI: {mmsi}): "
                "{message}".format(message="VESSEL IMO NOT FOUND IN DATABASE", **self.vessel)
            )

    def test_vessel_successfully_added(self):
        response = response_factory(
            os.path.join(FIXTURE_PATH, 'vessel-added.xml'), meta={'vessel': self.vessel}
        )
        with patch('kp_scrapers.spiders.ais.marinetraffic.MarineTrafficSpider.logger') as logger:
            spider = MarineTrafficSpider(fleet='True')

            spider.check_vessel_was_added(response)

        self.assertEqual(logger.info.call_count, 1)
        self.assertEqual(
            logger.info.call_args_list[-1][0][0],
            "Vessel {name} (IMO: {imo}, MMSI: {mmsi}) added to MT"
            " fleet.".format(message="FLEET ITEM UPDATED", **self.vessel),
        )

    def test_vessel_action_error(self):
        response = response_factory(
            os.path.join(FIXTURE_PATH, 'imo-not-found.xml'),
            meta={'vessel': self.vessel, 'action': 'add'},
        )
        with patch('kp_scrapers.spiders.ais.marinetraffic.MarineTrafficSpider.logger'):
            spider = MarineTrafficSpider(fleet='True')

            spider.check_vessel_action_status(response)

            spider.logger.error.assert_called_once_with(
                "Could not add vessel {name} (IMO: {imo}, MMSI: {mmsi}): "
                "{message}".format(message="VESSEL IMO NOT FOUND IN DATABASE", **self.vessel)
            )

    def test_vessel_action_successful(self):
        response = response_factory(
            os.path.join(FIXTURE_PATH, 'vessel-added.xml'),
            meta={'vessel': self.vessel, 'action': 'add'},
        )
        with patch('kp_scrapers.spiders.ais.marinetraffic.MarineTrafficSpider.logger') as logger:
            spider = MarineTrafficSpider(fleet='True')

            spider.check_vessel_action_status(response)

        self.assertEqual(logger.info.call_count, 1)
        self.assertEqual(
            logger.info.call_args_list[-1][0][0],
            "Vessel {name} (IMO: {imo}, MMSI: {mmsi}) added to MT fleet.".format(
                message="FLEET ITEM UPDATED", **self.vessel
            ),
        )

    def test_no_crash_when_both_fleet_and_vessel_list_are_empty(self):
        with patch(VESSELS_MODULE, new=ForcedCollection()):
            spider = MarineTrafficSpider(
                fleet='True',
                fleet_name='MT',
                setkey='abcdef',
                getkey='123456',
                allow_removal='true',
            )
            response = response_factory(os.path.join(FIXTURE_PATH, 'fleet-empty.xml'))
            requests = []
            for request in spider.update_fleet(response):
                requests.append(request)

        self.assertEqual(len(requests), 0)

    def test_vessel_removal_when_fleet_empty_vessel_list_contains_one_vessel_other_provider(self):
        self.vessel['providers'] = ['Other Provider']
        with patch(VESSELS_MODULE, new=ForcedCollection([self.vessel])):
            spider = MarineTrafficSpider(
                fleet='True',
                fleet_name='MT',
                setkey='abcdef',
                getkey='123456',
                allow_removal='true',
            )
            response = response_factory(os.path.join(FIXTURE_PATH, 'fleet-empty.xml'))
            requests = []
            for request in spider.update_fleet(response):
                requests.append(request)

        self.assertEqual(len(requests), 0)

    def test_vessel_add_when_fleet_empty_and_vessel_list_contains_one_vessel(self):
        self.vessel['providers'] = ['MT']
        with patch(VESSELS_MODULE, new=ForcedCollection([self.vessel])):
            spider = MarineTrafficSpider(
                fleet='True',
                fleet_name='MT',
                setkey='abcdef',
                getkey='123456',
                allow_removal='true',
            )
            response = response_factory(os.path.join(FIXTURE_PATH, 'fleet-empty.xml'))
            requests = []
            for request in spider.update_fleet(response):
                requests.append(request)

        self.assertEqual(len(requests), 1)
        request = requests[0]
        self.assertIn('action', request.meta)
        self.assertEqual(request.meta['action'], 'add')
        self.assertIn('imo:{}'.format(self.vessel['imo']), request.url)
        self.assertIn('active:1', request.url)
        self.assertNotIn('delete:1', request.url)

    def test_vessel_removal_when_fleet_contains_one_vessel_and_vessel_list_empty(self):
        with patch(VESSELS_MODULE, new=ForcedCollection()):
            spider = MarineTrafficSpider(
                fleet='True', fleet_name='MT', setkey='abcdef', getkey='123456', removal='true'
            )
            response = response_factory(
                os.path.join(FIXTURE_PATH, 'fleet-one-vessel.xml'), klass=FakeXmlResponse
            )
            requests = []
            for request in spider.update_fleet(response):
                requests.append(request)

        self.assertEqual(len(requests), 1)
        request = requests[0]
        self.assertIn('action', request.meta)
        self.assertEqual(request.meta['action'], 'delete')
        self.assertIn('imo:{}'.format(self.vessel['imo']), request.url)
        self.assertNotIn('active:1', request.url)
        self.assertIn('delete:1', request.url)

    def test_parse(self):
        response = FakeXmlResponse(os.path.join(FIXTURE_PATH, 'position-and-eta.xml'))
        # result = etree.fromstring(response.body)
        spider = MarineTrafficSpider(fleet_name='MT_API', msgtype='extended')
        responses = [
            {
                'aisType': 'T-AIS',
                'master_imo': '7357452',
                'master_mmsi': '205194000',
                'master_callsign': 'ONAE',
                'master_name': 'METHANIA',
                # 'master_shipType': 'tankships',
                'nextDestination_aisType': 'T-AIS',
                'nextDestination_destination': 'MARSEILLE',
                'nextDestination_eta': '2015-12-16T10:00:00',
                'position_aisType': 'T-AIS',
                'position_course': '245',
                'position_draught': '9.5',
                'position_lat': '43.332330',
                'position_lon': '5.342667',
                'position_navState': None,
                'position_speed': 0.1,
                'position_timeReceived': '2015-06-12T10:46:46',
                'provider_id': 'MT_API',
            }
        ]

        idx = None
        for idx, item in enumerate(spider.parse(response)):
            relevant_fields = list(strip_meta_fields(item).keys())
            for key in relevant_fields:
                self.assertEqual(item.get(key), responses[idx].get(key))

            # Ensure the item and our test dict have the same number of keys.
            self.assertEqual(len(relevant_fields), len(responses[idx]))

        self.assertIsNotNone(idx)  # Ensures the loop iterated at least once

    def test_retrieve_positions(self):
        response = FakeXmlResponse(os.path.join(FIXTURE_PATH, 'position-and-eta.xml'))
        # result = etree.fromstring(response.body)
        spider = MarineTrafficSpider(fleet_name='MT_API', msgtype='extended')
        responses = [
            {
                'aisType': 'T-AIS',
                'master_imo': '7357452',
                'master_mmsi': '205194000',
                'master_callsign': 'ONAE',
                'master_name': 'METHANIA',
                # 'master_shipType': 'tankships',
                'nextDestination_aisType': 'T-AIS',
                'nextDestination_destination': 'MARSEILLE',
                'nextDestination_eta': '2015-12-16T10:00:00',
                'position_aisType': 'T-AIS',
                'position_course': '245',
                'position_draught': '9.5',
                'position_lat': '43.332330',
                'position_lon': '5.342667',
                'position_navState': None,
                'position_speed': 0.1,
                'position_timeReceived': '2015-06-12T10:46:46',
                'provider_id': 'MT_API',
            }
        ]

        idx = None
        for idx, item in enumerate(spider.parse(response)):
            relevant_fields = list(strip_meta_fields(item).keys())
            for key in relevant_fields:
                self.assertEqual(item.get(key), responses[idx].get(key))

            # Ensure the item and our test dict have the same number of keys.
            self.assertEqual(len(relevant_fields), len(responses[idx]))
        self.assertIsNotNone(idx)  # Ensures the loop iterated at least once

    def test_retrieve_terrestrial_position(self):
        response = FakeXmlResponse(os.path.join(FIXTURE_PATH, 'position-v5-terrestrial.xml'))
        # result = etree.fromstring(response.body)
        spider = MarineTrafficSpider(fleet_name='MT_API', msgtype='simple')
        responses = [
            {
                'aisType': 'T-AIS',
                'master_mmsi': '205194000',
                # 'master_shipType': 'tankships',
                'position_aisType': 'T-AIS',
                'position_course': '11',
                'position_heading': 329,
                'position_lat': '43.332330',
                'position_lon': '5.342667',
                'position_navState': '5',
                'position_speed': 0.0,
                'position_timeReceived': '2015-09-17T06:49:55',
                'provider_id': 'MT_API',
            }
        ]

        idx = None
        for idx, item in enumerate(spider.parse(response)):
            relevant_fields = list(strip_meta_fields(item).keys())
            for key in relevant_fields:
                self.assertEqual(item.get(key), responses[idx].get(key))

            # Ensure the item and our test dict have the same number of keys.
            ikeys = set(item.keys())
            dkeys = set(responses[0])
            self.assertEqual(
                len(relevant_fields),
                len(responses[idx]),
                'Missing keys: {}, Extraneous keys: {}'.format(
                    ', '.join(list(dkeys - ikeys)), ', '.join(list(ikeys - dkeys))
                ),
            )
        self.assertIsNotNone(idx)  # Ensures the loop iterated at least once

    def test_retrieve_sattelite_position(self):
        response = FakeXmlResponse(os.path.join(FIXTURE_PATH, 'position-v5-sattelite.xml'))
        # result = etree.fromstring(response.body)
        spider = MarineTrafficSpider(fleet_name='MT_API', msgtype='simple')
        responses = [
            {
                'aisType': 'S-AIS',
                'master_mmsi': '205194000',
                # 'master_shipType': 'tankships',
                'position_aisType': 'S-AIS',
                'position_course': '11',
                'position_heading': 329,
                'position_lat': '43.332330',
                'position_lon': '5.342667',
                'position_navState': '5',
                'position_speed': 0.0,
                'position_timeReceived': '2015-09-17T06:49:55',
                'provider_id': 'MT_API',
            }
        ]

        idx = None
        for idx, item in enumerate(spider.parse(response)):
            relevant_fields = list(strip_meta_fields(item).keys())
            for key in relevant_fields:
                self.assertEqual(item.get(key), responses[idx].get(key))

            # Ensure the item and our test dict have the same number of keys.
            ikeys = set(item.keys())
            dkeys = set(responses[0])
            self.assertEqual(
                len(relevant_fields),
                len(responses[idx]),
                'Missing keys: {}, Extraneous keys: {}'.format(
                    ', '.join(list(dkeys - ikeys)), ', '.join(list(ikeys - dkeys))
                ),
            )
        self.assertIsNotNone(idx)  # Ensures the loop iterated at least once
