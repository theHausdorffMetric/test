# -*- coding: utf-8 -*-
"""Scrape booked and available slots for panama canal

Web app of panama canal can be found here:
https://bookingwp.panama-canal.com/

We scrape and yield raw json
We scrape 2 years of data at each spider run

Normalisation of raw data is done on the etl for now
It should be move to Incoming Data API once ready

For more information, see ct-pipeline doc and
https://kpler1.atlassian.net/browse/LNG-154
"""
from __future__ import absolute_import, unicode_literals
import json

from scrapy import Spider

from kp_scrapers.spiders.canals import CanalSpider


class PanamaSlot(CanalSpider, Spider):

    name = 'PanamaSlot'
    version = '1.0.0'
    provider = 'PanamaCanal'

    start_urls = ['https://bookingwp.panama-canal.com/Conditions.json']

    def parse(self, response):
        raw_str = response.body.decode("utf-8-sig").encode("utf-8")
        raw_json = json.loads(raw_str)
        for raw_item in raw_json:
            yield raw_item
