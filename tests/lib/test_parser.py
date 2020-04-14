# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from unittest import TestCase

from nose.tools import raises

from kp_scrapers.lib import parser
from tests._helpers.mocks import FakeResponse, FakeXmlResponse, inject_fixture


class ParserSerializeTestCase(TestCase):
    @inject_fixture('ais/marinetraffic/simple-ais-messages.json', loader=FakeResponse)
    def test_response_json_serialization(self, response):
        @parser.serialize_response('json')
        def _callback(klass, blob):
            # return data as is so we can test it
            return blob

        self.assertTrue(isinstance(response.body, bytes))
        ais = _callback(None, response)
        self.assertTrue(isinstance(ais, list))
        self.assertTrue(isinstance(ais[0], dict))
        self.assertIn('MMSI', ais[0])

    @inject_fixture('ais/marinetraffic/position-and-eta.xml', loader=FakeXmlResponse)
    def test_response_xml_serialization(self, response):
        @parser.serialize_response('xml')
        def _callback(klass, blob):
            # return data as is so we can test it
            return blob

        self.assertTrue(isinstance(response.body, bytes))
        ais = _callback(None, response)
        self.assertEquals(ais.type, 'xml')
        self.assertTrue(len(ais.xpath('//row').extract()))

    @raises(NotImplementedError)
    def test_response_unknown_serialization(self):
        @parser.serialize_response('foo')
        def _callback(klass, blob):
            pass

        _callback(None, None)
