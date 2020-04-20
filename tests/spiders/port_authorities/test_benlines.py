# -*- coding: utf-8; -*-

from __future__ import absolute_import, print_function, unicode_literals
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock
from tests._helpers.mocks import FakeResponse, FakeXmlResponse, inject_fixture
import logging
from pathlib import Path

from kp_scrapers.spiders.port_authorities.benlines.spider import BenlinesSpider

from tests._helpers.mocks import fixtures_path
logger = logging.getLogger(__name__)


class BenlinesTestCase(TestCase):

    def test_first_pass_port_names(self):
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
        rows = spider.extract_port_data(content)

        data={}
        for row in rows:
            data[row["vessel"]["name"]] = row

        print(data)

        #     port_name=row[0]
        #     table_id=row[1]
        #     info=row[2]
        #     if data.get(port_name,None) is None:
        #         data[port_name] = {}
        #     if data[port_name].get(table_id,None) is None:

        #self.assertListEqual(sorted(port_names),sorted(list(data.keys())))
        self.assertEqual(566,len(data.keys()))

        d=data["CERVIA"]
        self.assertEqual("SANDHEAD ANCHORAGE",d["berth"])

        d=data["WEI LUN JU LONG"]
        self.assertEqual("8",d["berth"])
        self.assertEqual("discharge",d["cargoes"][0]["movement"])
        self.assertEqual("COKING COAL",d["cargoes"][0]["product"])
        self.assertEqual(26803,float(d["cargoes"][0]["volume"]))

        d=data["JAG PRABHA"]
        self.assertEqual("Q4",d["berth"])
        self.assertEqual(2,len(d["cargoes"]))
        self.assertEqual("discharge",d["cargoes"][1]["movement"])
        self.assertEqual("HIGH SPEED DIESEL (HSD )",d["cargoes"][1]["product"])
        self.assertEqual(8000,float(d["cargoes"][1]["volume"]))

        d=data["FRANCESCO CORRADO"]
        self.assertEqual("discharge",d["cargoes"][0]["movement"])
        self.assertEqual("STEAM COAL",d["cargoes"][0]["product"])
        self.assertEqual(71619.540,float(d["cargoes"][0]["volume"]))

        d=data["JAOHAR RANIM"]
        self.assertEqual("CJ-2",d["berth"])
        self.assertEqual(2,len(d["cargoes"]))
        self.assertEqual("load",d["cargoes"][1]["movement"])
        self.assertEqual("SUGAR",d["cargoes"][1]["product"])
        self.assertEqual(0,float(d["cargoes"][1]["volume"]))