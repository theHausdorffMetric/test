# -*- coding: utf-8 -*-
# vim:fenc=utf-8

from __future__ import absolute_import, unicode_literals
from unittest import TestCase

from nose.plugins.attrib import attr
from nose.tools import raises

from kp_scrapers.spiders.ais.spire import api
from tests._helpers.mocks import FakeTextResponse, inject_fixture, TextResponse


@attr('unit')
class APIUtilsTestCase(TestCase):
    def test_header_format(self):
        header = api.headers('secret')
        self.assertEquals(header['Accept'], 'application/json')
        self.assertEquals(header['Authorization'], 'Bearer secret')

    def test_request_builder_resource(self):
        url = api.make_request_url('vessels')
        self.assertEquals(url, 'https://ais.spire.com/vessels?fields=decoded&')

    def test_request_builder_params(self):
        url = api.make_request_url('vessels', foo='bar')
        self.assertEquals(url, 'https://ais.spire.com/vessels?fields=decoded&foo=bar')


class APIResponseTestCase(TestCase):
    @inject_fixture('ais/spire/vessels-api.json', loader=FakeTextResponse)
    def setUp(self, raw_response):
        self.res = api.ResponseFromScrapy(raw_response)

    def test_bad_response(self):
        res = api.ResponseFromScrapy(TextResponse('not found'))
        self.assertEquals(res.status, 404)
        self.assertEquals(res.data, [])
        self.assertTrue(res.failed)

    def test_response_factory_status(self):
        self.assertEquals(self.res.status, 200)
        self.assertFalse(self.res.failed)

    def test_response_paging(self):
        self.assertEquals(self.res.paging['total'], 1)
        self.assertEquals(self.res.paging['limit'], 100)
        # not sure what requirements we have for this
        self.assertTrue(self.res.paging['next'])

    def test_response_next_page(self):
        self.assertFalse(self.res.has_next_page)

    @raises(ValueError)
    def test_response_next_page_doesnt_exists(self):
        self.assertEquals(self.res.next_page())

    def test_response_data(self):
        self.assertEquals(len(self.res.data), 1)
        self.assertEquals(self.res.data[0].get('flag'), 'PA')
