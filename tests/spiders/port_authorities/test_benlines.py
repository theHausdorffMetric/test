# -*- coding: utf-8; -*-

from __future__ import absolute_import, print_function, unicode_literals
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock
from tests._helpers.mocks import FakeResponse, FakeXmlResponse, inject_fixture
import logging
from pathlib import Path

from kp_scrapers.spiders.port_authorities.benlines.spider import BenlinesSpider
from kp_scrapers.spiders.port_authorities.benlines import schema

from tests._helpers.mocks import fixtures_path
logger = logging.getLogger(__name__)


class BenlinesTestCase(TestCase):

    def test_0(self):
        logger.setLevel("INFO")
        name = "VPR-08-04-2020.PDF"
        filename = fixtures_path('port_authorities', 'benlines', name)
        content = Path(filename).read_bytes()
        logger.info(content)
        spider = BenlinesSpider()
        port_names = spider.extract_port_names(content)
        self.assertEqual(40,len(port_names))
        self.assertEqual(port_names[12],"HAZIRA(ADANI)")
        self.assertEqual(port_names[28],"MUMBAI (JNPT)")
        self.assertEqual(port_names[38],"VADINAR")


    def test_1(self):
        name = "VPR-08-04-2020.PDF"
        filename = fixtures_path('port_authorities', 'benlines', name)
        content = Path(filename).read_bytes()
        spider = BenlinesSpider()

        port_names = spider.extract_port_names(content)
        data = spider.extract_port_data(content)
        keys = sorted(data.keys())
        self.assertEqual(keys[38],"VADINAR")
        self.assertIsNotNone(data.get("VADINAR",None))
        self.assertListEqual(sorted(port_names),sorted(list(data.keys())))
        self.assertEqual(40,len(data.keys()))

        self.assertEqual(data["BEDI(INCL. ROZI-JAMNAGAR)"][schema.BenlineTableEnum.ARRIVE][0][0],"CLIPPER BAROLO")
        self.assertEqual(data["MUMBAI (JNPT)"][schema.BenlineTableEnum.LOADING][0][0],"JAG VIJAYA")

        self.assertEqual(data["GANGAVARAM"][schema.BenlineTableEnum.DISCHARGE][0][0],"AQUAEXPLORER")


