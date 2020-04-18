# -*- coding: utf-8; -*-

from __future__ import absolute_import, print_function, unicode_literals
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

from kp_scrapers.spiders.port_authorities.ferrol import FerrolTable
from tests._helpers.mocks import fixtures_path


class BenlinesTestCase(TestCase):
    def _get_table(self, name):
        filename = fixtures_path('port_authorities', 'benlines', name)
        return filename


    def test_0(self):
        f = self._get_table("VPR-08-04-2020.PDF")
        self.assertIsNotNone(f)
        pass