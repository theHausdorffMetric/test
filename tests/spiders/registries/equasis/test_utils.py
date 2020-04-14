# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import datetime
import unittest

import six

from kp_scrapers.spiders.registries.equasis import utils


class TestEquasisUtil(unittest.TestCase):
    def test_date_to_timestamp(self):
        # GIVEN
        # We need to remove the microseconds because timestamps truncate after seconds
        dt = datetime.datetime.utcnow().replace(microsecond=0)

        # WHEN
        timestamp = utils.to_timestamp(dt)

        # THEN
        self.assertEqual(datetime.datetime.utcfromtimestamp(timestamp), dt)

    def test_session_id(self):
        # WHEN
        id1 = utils.session_id()
        id2 = utils.session_id()

        # THEN
        self.assertTrue(type(id1) is six.text_type)
        self.assertTrue(type(id2) is six.text_type)
        self.assertNotEqual(id1, id2)
