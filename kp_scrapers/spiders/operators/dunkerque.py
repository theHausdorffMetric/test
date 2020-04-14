# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
import csv
from datetime import datetime

from scrapy import Request

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.models.items_inventory import IOItem

from . import OperatorSpider
from .base import BaseOperatorSpider


class DunkerqueSpider(OperatorSpider, BaseOperatorSpider):
    name = 'DunkerqueOperator'

    date_format = '%Y-%m-%d'

    min_date = datetime(2016, 6, 25)
    lag_past = 10

    FIELDNAMES = (
        'day',
        'level_m3',
        'level_gwh',
        'storage_capacity',
        'send_out',
        'delivery_capacity',
    )

    def start_requests(self):
        # fyi, start and end dates are set in base.__init__
        start_url = (
            'https://www.ebb.dlng-sico.com/HistoricalData/ExportTerminalDataAsync'
            '?DisplayUnitSelection.Selected=0'
            '&Filter.GasDayRangeFilter.FromGasDayDateTime={start_date}'
            '&Filter.GasDayRangeFilter.ToGasDayDateTime={end_date}'
            '&Filter.FileExportFormat=csv'
        ).format(start_date=self.start_date, end_date=self.end_date)

        yield Request(start_url, self.parse)

    def parse(self, response):
        clean_response = (
            response.text.replace(u'\xb3', u'3').replace(u'\xa0', u' ').replace(u'\r\n', u'\n')
        )

        reader = csv.DictReader(
            clean_response.splitlines(), delimiter=';', fieldnames=self.FIELDNAMES
        )

        next(reader)  # Pop header

        for row in reader:
            item = {}
            try:
                item['date'] = datetime.strptime(row['day'], '%d/%m/%Y').isoformat(' ')
            except ValueError as e:
                self.logger.error(e)
            item['stock_level'] = try_apply(row['level_gwh'].replace(',', '.'), float)
            item['send_out'] = try_apply(row['send_out'].replace(',', '.'), float)
            item['unit'] = 'GWH'

            # Temporary while using old port_operator loader
            mapped_item = IOItem()
            mapped_item['date'] = item['date']
            mapped_item['level_o'] = item['stock_level']
            mapped_item['output_o'] = item['send_out']
            mapped_item['unit'] = item['unit']

            # Drop item if both stock_level and send_out are not given by the website
            if item['stock_level'] in (0.0, None) and item['send_out'] in (0.0, None):
                yield
            else:
                yield mapped_item
