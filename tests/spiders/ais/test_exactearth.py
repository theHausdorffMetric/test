import datetime as dt
import os
from unittest import TestCase

from scrapy import Selector
import six

from kp_scrapers.spiders.ais.exactearth import api, constants, parser, spider
from tests._helpers.mocks import FakeXmlResponse, fixtures_path, MockStaticData


MODULE_PATH = os.path.dirname(os.path.dirname(__file__))
FIXTURES_PATH = fixtures_path('ais', 'exactearth')
FIXTURE_RESPONSE_PATH = os.path.join(FIXTURES_PATH, 'latest-vessel-info.xml')
FIXTURE_VESSEL = {
    "ais_type": "T-AIS",
    "message_type": "1",
    "next_destination_eta": '{y}-06-17T02:00:00'.format(y=dt.datetime.utcnow().year),
    "next_destination_ais_type": "T-AIS",
    "next_destination_destination": "SINGAPORE",
    "position": {
        "ais_type": "T-AIS",
        "course": 207.4,
        "heading": 206.0,
        "lat": 29.678498333333334,
        "lon": 126.184325,
        "nav_state": 0,
        "received_time": "2019-06-10T03:44:04",
        "speed": 15.0,
        "draught_raw": 11.3,
        "draught": None,
    },
    "provider_name": "EE",
    "reported_date": "2019-06-10T01:21:16",
    "vessel": {
        "beam": None,
        "build_year": None,
        "call_sign": 'C6DP6',
        "dead_weight": None,
        "dwt": None,
        "flag_code": None,
        "flag_name": None,
        "gross_tonnage": None,
        "imo": '9834454',
        "length": None,
        "mmsi": "311000743",
        "name": "LONDON VOYAGER",
        'type': None,
        "vessel_type": "80",
    },
}

if not os.environ.get('KP_DISABLE_MOCK'):
    # monkey patch production vessel list with ligthweight/network-free fake
    spider.static_data = MockStaticData  # noqa


class ExactEarthRequestTestCase(TestCase):
    def setUp(self):
        self.eer = api.XMLRequestFactory(1234)

    def test_initialization(self):
        self.assertEqual(self.eer.filters, [])
        self.assertEqual(self.eer.token, 1234)


class ExactEarthUtilsTestCase(TestCase):
    def setUp(self):
        self.mock_api_result = FakeXmlResponse(FIXTURE_RESPONSE_PATH)

    def test_eta_parsing(self):
        eta_dt = parser.parse_eta_fmt(2015, '11032330')
        self.assertEqual(eta_dt, dt.datetime(2015, 11, 3, 23, 30))

    def test_xml_node_extraction(self):
        tree = Selector(self.mock_api_result).root
        for node in tree.findall('{gml}featureMembers/'.format(**constants.XMLNS)):
            self.assertEqual(parser.extract(node, 'source'), FIXTURE_VESSEL['ais_type'])

    def test_parse_response(self):
        for vessel in parser.parse_response(self.mock_api_result):
            for k, v in six.iteritems(FIXTURE_VESSEL):
                self.assertEqual(vessel[k], v)


class ExactEarthSpiderTestCase(TestCase):
    def setUp(self):
        self.api_key = '123456'
