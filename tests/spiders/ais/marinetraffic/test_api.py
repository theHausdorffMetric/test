# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from unittest import TestCase

from nose.tools import raises
from six.moves.urllib.parse import urlparse

from kp_scrapers.spiders.ais.marinetraffic import api


class MarineTrafficClientTestCase(TestCase):
    def test_client_url_builder(self):
        client = api.MTClient('MT_API')
        url = client.build_url('a_method', 'a_token')
        parsed = urlparse(url)

        self.assertEquals(parsed.scheme, 'https')
        self.assertEquals(parsed.netloc, 'services.marinetraffic.com')
        self.assertEquals(parsed.path, '/api/a_method/a_token/protocol:jsono')

    def test_client_url_builder_options(self):
        client = api.MTClient('MT_API')
        url = client.build_url('a_method', 'a_token', foo='bar')
        parsed = urlparse(url)

        self.assertIn('/foo:bar/', parsed.path)

    @raises(KeyError)
    def test_client_wrong_fleet_name(self):
        api.MTClient(fleet_name='FOO')
