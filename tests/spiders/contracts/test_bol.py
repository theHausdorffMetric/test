# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import unittest

from kp_scrapers.spiders.contracts import bill_of_lading


class BOLUtilsTestCase(unittest.TestCase):
    def test_auth_body_factory(self):
        body = bill_of_lading.BoLSpider._auth_body()
        for keyword in ['nusername', 'action', 'password']:
            self.assertTrue(keyword in body)

        self.assertEqual(len(body.split('&')), 3)


# TODO check this is an 'OtherSpider' (ie)
class BOLSpiderTestCase(unittest.TestCase):
    def setUp(self):
        self.spider = bill_of_lading.BoLSpider()

    def test_BoLSpider_is_contract_spider(self):
        dd_tags = self.spider.category_settings.get('DATADOG_CUSTOM_TAGS')
        self.assertTrue('category:contract' in dd_tags)
        self.assertEqual(self.spider.category(), 'contract')
