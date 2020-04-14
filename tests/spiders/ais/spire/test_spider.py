# -*- coding: utf-8 -*-
# vim:fenc=utf-8

from __future__ import absolute_import, unicode_literals
import logging
from unittest import TestCase

from nose.plugins.attrib import attr

from kp_scrapers.spiders.ais.spire import spider


logger = logging.getLogger(__name__)


@attr('unit')
class SpiderUtilsTestCase(TestCase):
    def test_unknown_dest(self):
        self.assertTrue(spider.dest_is_unknown({}))
        self.assertTrue(spider.dest_is_unknown({'nextDestination_destination': ''}))

    def test_similar_names(self):
        threshold = spider.MIN_MESSAGE_SIMILARITY
        names = {
            ('C. Vision', 'C.VISION'): True,
            ('Christophe De Margerie', 'CHRIS. DE MARGERIE'): True,
            ('Nordocean', 'NORDNCEAN'): True,
            ('Daebo Newcastle', 'DAEBO NMWCAWTLE'): True,
            ('Athenian Glory', 'ADHENIAN CLORY'): True,
            ('Transformer Ol', 'TBANSFORMER OL'): True,
            ('Evergreen State', 'EVARREEN STATE'): True,
            ('Tr Infinity', 'TP ANFINIVY'): False,
            ('Desh Vishal', 'DESH VISHAL P'): True,
            ('Golar Tundra', 'LNG GOLAR TUNDRA'): True,
            ('Sinar Busan', 'MT.SINAR BHSAN'): True,
            ('Dorsch', 'MT DORSH'): True,
            ('Pelita Bangsa', 'M/T PELITA BNGSA'): True,
            ('Aegea', 'M.V.AEGEA'): True,
            ('Mtm Hamburg', 'MTM HAMBURG'): True,
            ('Adriano Knutsen', 'ADRIANO KNUTSEN S/T'): True,
            ('Pacific M.', None): False,
        }

        for pair, result in names.items():
            logger.info("Names: %s", pair)
            self.assertEqual(spider.name_is_similar(*pair, threshold), result)
