# -*- coding: utf8 -*-

"""Grain send out data.

The spider can run in two modes:
- standard mode without argument scrapes current month
- specifying `-a all=Whatever` scrapes historical data, to scrape data from 17 July 2005

"""

from __future__ import absolute_import, unicode_literals
import datetime as dt
import json

from scrapy import Request
from scrapy.spiders import Spider

from kp_scrapers.models.items_inventory import IOItem

from . import OperatorSpider


HISTORICAL_ROUTES = ['getGrainCurrentYearData', 'getGrainArchivalData']
MONTH_ROUTES = ['getGrainCurrentMonthData']


def _extract_current_month(raw_data):
    return dt.datetime.strptime(raw_data.get('Gas_Day'), '%d/%m/%Y').isoformat(' ')


def _extract_historical_date(raw_data):
    return dt.datetime(
        year=raw_data.get('GasYear') or dt.datetime.now().year,
        month=dt.datetime.strptime(raw_data.get('GasMonth'), '%B').month,
        day=raw_data.get('GasDay'),
    ).isoformat(' ')


def parse_ioitem(raw_data, _extract_date):
    item = IOItem(unit='KWH')
    item['date'] = _extract_date(raw_data)
    item['output_o'] = raw_data.get('AggregateSendOut')

    return item


class GrainSendoutOperatorSpider(OperatorSpider, Spider):

    name = 'GrainSendoutOperator'
    base_url = 'https://extranet.nationalgrid.com/grain/api/grain/'

    def __init__(self, all='false', *args, **kwargs):
        is_historical = all.lower() == 'true'
        self.routes = HISTORICAL_ROUTES if is_historical else MONTH_ROUTES
        self._extract_date = _extract_historical_date if is_historical else _extract_current_month

    def start_requests(self):
        return (Request(self.base_url + route) for route in self.routes)

    def parse(self, response):
        json_response = json.loads(response.body)
        data = json_response.get('aaData')
        return (parse_ioitem(raw, self._extract_date) for raw in data)
