# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
import os
import unittest
from unittest.mock import patch

from kp_scrapers.models.base import strip_meta_fields
from kp_scrapers.spiders.ais.vesseltracker import VesselTrackerSpider
from tests._helpers.mocks import fixtures_path


FIXTURE_PATH = fixtures_path('ais', 'vesseltracker')


class FakeSoapResponse(object):
    def __init__(self, filename):
        """Mocks soap method call response.

        Parameters
        ----------

           filename (str): a file from which load the XML data to use
               as the response from a SOAP method call.

        """
        with open(os.path.join(FIXTURE_PATH, filename), 'r') as f:
            self._xml_response = f.read()

    def as_xml(self):
        return self._xml_response

    def __getattr__(self, attr):
        if attr.endswith('Response'):
            return self
        return getattr(self, attr)


class VesselTrackerTestCase(unittest.TestCase):
    def setUp(self):
        self.vessel = {
            'imo': '0000000',
            'name': 'Some Vessel',
            'mmsi': '111111111',
            'status': 'Active',
            'status_detail': 'In Service/Commission',
        }

    @unittest.skip('VesselTracker has been deprecated')
    def test_response_with_voyage_and_position(self):
        response = FakeSoapResponse('position-and-eta.xml')
        spider = VesselTrackerSpider(username='U', password='P')
        responses = [
            {
                'aisType': 'S-AIS & T-AIS',
                'master_imo': '7400704',
                'master_mmsi': '605106030',
                'master_callsign': '7TJC',
                'master_name': 'MOURAD DIDOUCHE',
                'master_dimA': '224',
                'master_dimB': '50',
                'master_dimC': '11',
                'master_dimD': '31',
                'master_shipType': 'tankships',
                'nextDestination_aisType': 'VT',
                'nextDestination_destination': 'ARZEW',
                'nextDestination_eta': '2015-06-10T08:00:00+02:00',
                'nextDestination_timeUpdated': '2015-06-10T03:41:32.047+02:00',
                'position_aisType': 'SAT',
                'position_course': '206.0',
                'position_draught': '10.8',
                'position_heading': None,
                'position_lat': '36.26815',
                'position_lon': '0.03751666666666666',
                'position_navState': None,
                'position_speed': None,
                'position_timeReceived': '2015-06-10T03:54:40.778+02:00',
                'provider_id': 'VT',
            }
        ]

        for idx, item in enumerate(spider._parse_response(response)):
            item_bones = strip_meta_fields(item)
            for key in item_bones.keys():
                self.assertEqual(item_bones.get(key), responses[idx][key])

            # Ensure the item and our test dict have the same number of keys.
            self.assertEqual(len(item_bones), len(responses[idx]))

    @unittest.skip('VesselTracker has been deprecated')
    def test_response_with_master_data_only(self):
        response = FakeSoapResponse('master-data-only.xml')
        spider = VesselTrackerSpider(username='U', password='P')

        with self.assertRaises(StopIteration):
            next(spider._parse_response(response))

    @unittest.skip('VesselTracker has been deprecated')
    def test_response_with_voyage_only(self):
        response = FakeSoapResponse('eta-only.xml')
        spider = VesselTrackerSpider(username='U', password='P')
        responses = [
            {
                'aisType': 'VT',
                'master_imo': '7400704',
                'master_mmsi': '605106030',
                'master_callsign': '7TJC',
                'master_name': 'MOURAD DIDOUCHE',
                'master_dimA': '224',
                'master_dimB': '50',
                'master_dimC': '11',
                'master_dimD': '31',
                'master_shipType': 'tankships',
                'nextDestination_aisType': 'VT',
                'nextDestination_destination': 'ARZEW',
                'nextDestination_eta': '2015-06-10T08:00:00+02:00',
                'nextDestination_timeUpdated': '2015-06-10T03:41:32.047+02:00',
                'position_draught': '10.8',
                'provider_id': 'VT',
            }
        ]

        for idx, item in enumerate(spider._parse_response(response)):
            item_bones = strip_meta_fields(item)
            for key in item_bones.keys():
                self.assertEqual(item_bones.get(key), responses[idx][key])

            # Ensure the item and our test dict have the same number of keys.
            self.assertEqual(len(item_bones), len(responses[idx]))

    @unittest.skip('VesselTracker has been deprecated')
    def test_response_with_position_only(self):
        response = FakeSoapResponse('position-only.xml')
        spider = VesselTrackerSpider(username='U', password='P')
        responses = [
            {
                'aisType': 'SAT',
                'master_imo': '7400704',
                'master_mmsi': '605106030',
                'master_callsign': '7TJC',
                'master_name': 'MOURAD DIDOUCHE',
                'master_dimA': '224',
                'master_dimB': '50',
                'master_dimC': '11',
                'master_dimD': '31',
                'master_shipType': 'tankships',
                'position_aisType': 'SAT',
                'position_course': '206.0',
                'position_heading': None,
                'position_lat': '36.26815',
                'position_lon': '0.03751666666666666',
                'position_navState': None,
                'position_speed': None,
                'position_timeReceived': '2015-06-10T03:54:40.778+02:00',
                'provider_id': 'VT',
            }
        ]

        for idx, item in enumerate(spider._parse_response(response)):
            item_bones = strip_meta_fields(item)
            for key in item_bones.keys():
                self.assertEqual(item_bones.get(key), responses[idx][key])

            # Ensure the item and our test dict have the same number of keys.
            self.assertEqual(len(item_bones), len(responses[idx]))

    @unittest.skip('VesselTracker has been deprecated')
    def test_spider_init_mutually_exclusive_actions(self):
        with patch('kp_scrapers.spiders.ais.vesseltracker.VesselTrackerSpider.logger'):
            spider = VesselTrackerSpider(
                username='bcd', password='abc', fleet='true', showfleet='true'
            )
            idx = -1
            for idx, item in enumerate(spider.start_requests()):
                self.assertEqual(item.callback, spider.show_vessel_fleet)

            spider.logger.warning.assert_called_once_with(
                'Contradictory arguments: cannot update and '
                'show fleet at once. Will assume command is '
                'show fleet.'
            )
            self.assertEqual(idx, 0)
            self.assertTrue(spider._show_fleet)

    @unittest.skip('VesselTracker has been deprecated')
    def test_vessel_tracker_update_fleet_true_when_fleet_arg_is_true(self):
        def get_vessel_fleet(spider):
            yield

        with patch('kp_scrapers.spiders.ais.vesseltracker.VesselTrackerSpider.logger'):
            with patch(
                'kp_scrapers.spiders.ais.vesseltracker.VesselTrackerSpider.get_vessel_fleet',
                new=get_vessel_fleet,
            ):
                spider = VesselTrackerSpider(username='bcd', password='abc', fleet='true')
                self.assertTrue(spider._update_fleet)

    @unittest.skip('VesselTracker has been deprecated')
    def test_vessel_tracker_show_fleet_is_true_when_show_fleet_args_is_true(self):
        def get_vessel_fleet(spider):
            yield

        with patch('kp_scrapers.spiders.ais.vesseltracker.VesselTrackerSpider.logger'):
            with patch(
                'kp_scrapers.spiders.ais.vesseltracker.VesselTrackerSpider.get_vessel_fleet',
                new=get_vessel_fleet,
            ):
                spider = VesselTrackerSpider(username='bcd', password='abc', showfleet='true')
                self.assertTrue(spider._show_fleet)

    @unittest.skip('VesselTracker has been deprecated')
    def test_vessel_tracker_calls_get_positions_when_no_fleet_args(self):
        def get_vessel_fleet(spider):
            yield

        with patch('kp_scrapers.spiders.ais.vesseltracker.VesselTrackerSpider.logger'):
            with patch(
                'kp_scrapers.spiders.ais.vesseltracker.VesselTrackerSpider.get_vessel_fleet',
                new=get_vessel_fleet,
            ):
                spider = VesselTrackerSpider(username='bcd', password='abc')
                self.assertFalse(spider._show_fleet)
                self.assertFalse(spider._update_fleet)
