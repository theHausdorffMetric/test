# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
import os
import unittest

import kp_scrapers.settings as settings


class UtilsTestCase(unittest.TestCase):
    def test_local_env_detection(self):
        # locally and on circleci it is not set
        self.assertFalse(settings.is_shub_env())

    def test_shub_env_detection(self):
        os.environ['SHUB_JOBKEY'] = '1234'
        self.assertTrue(settings.is_shub_env())
        os.environ.pop('SHUB_JOBKEY')
