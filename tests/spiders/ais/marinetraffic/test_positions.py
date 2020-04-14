# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from unittest import TestCase

from nose.tools import raises

# from tests._helpers.mocks import FakeResponse, FakeXmlResponse, inject_fixture
from kp_scrapers.lib.errors import InvalidCliRun
from kp_scrapers.spiders.ais.marinetraffic.spider import ais_key_map, MarineTrafficSpider
from tests._helpers import TestIO, with_test_cases


class MarineTrafficSpiderNormalisationTestCase(TestCase):
    @with_test_cases(
        TestIO('missing speed', given=None, then=0.0),
        TestIO('normal speed', given='124.0', then=12.4),
    )
    def test_speed_parsing(self, given):
        return ais_key_map['SPEED'][1](given)

    @with_test_cases(
        TestIO('missing draft', given=None, then='0.0'),
        TestIO('normal draft', given='724.0', then='72.4'),
    )
    def test_draught_parsing(self, given):
        return ais_key_map['DRAUGHT'][1](given)

    @with_test_cases(
        TestIO('missing heading', given=4000, then=None),
        TestIO('normal draft', given='234.0', then=234.0),
    )
    def test_heading_parsing(self, given):
        return ais_key_map['HEADING'][1](given)

    @with_test_cases(
        TestIO('wrong ais type', given='FOO', then='T-AIS'),
        TestIO('missing ais type', given=None, then='T-AIS'),
        TestIO('known terrestrial type', given='TER', then='T-AIS'),
        TestIO('known satellite type', given='SAT', then='S-AIS'),
    )
    def test_aistype_parsing(self, given):
        return ais_key_map['DSRC'][1](given)


class MarineTrafficApiSpiderTestCase(TestCase):
    @raises(InvalidCliRun)
    def test_spider_invalid_fleet_name(self):
        MarineTrafficSpider(fleet_name='FOO', poskey='1234')

    def test_spider_msgtype_fallback(self):
        spider = MarineTrafficSpider(fleet_name='MT_API', msgtype='foo', poskey='1234')
        self.assertEquals(spider._msgtype, MarineTrafficSpider.DEFAULT_MSGTYPE)

    def test_spider_defaults(self):
        spider = MarineTrafficSpider(fleet_name='MT_API', poskey='1234')
        self.assertEquals(spider._msgtype, MarineTrafficSpider.DEFAULT_MSGTYPE)
        self.assertEquals(spider._timespan, MarineTrafficSpider.DEFAULT_TIMESPAN)

        self.assertEquals(spider.client.fleet_name, 'MT_API')
        self.assertEquals(spider.client.poskey, '1234')
