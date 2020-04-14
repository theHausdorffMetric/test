# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
import csv
from datetime import datetime

from scrapy import FormRequest, Request

from kp_scrapers.models.items import USAModuleSendIn

from . import OperatorSpider
from .base import BaseOperatorSpider


class USASabinePassTransco(OperatorSpider, BaseOperatorSpider):

    name = 'SabinePassTransco'

    date_format = '%m/%d/%Y'
    min_date = datetime(2017, 1, 22)
    lag_past = 2

    def start_requests(self):
        start_url = (
            'http://www.1line.williams.com/ebbCode/IframePrinter.jsp'
            '?URL=http://www.1line.williams.com/Transco/info-postings/capacity'
            '/Operationally-Available.html'
        )
        yield Request(start_url, self.post)

    def post(self, response):
        formdata = {'tbGasFlowBeginDate': self.start_date, 'tbGasFlowEndDate': self.end_date}
        yield FormRequest.from_response(
            response=response, formname='myform', formdata=formdata, callback=self.get_data
        )

    def get_data(self, response):
        yield Request('http://www.1line.williams.com/ebbCode/OACreport.jsp', self.get_csv)

    def get_csv(self, response):
        yield Request('http://www.1line.williams.com/ebbCode/OACreportCSV.jsp', self.parse)

    def parse(self, response):
        reader = csv.DictReader(response.text.splitlines(), delimiter=str(','))

        transco_rows = [
            row
            for row in reader
            if row['Loc Name'].lower() == 'Lighthouse Road M4662 MP 13'.lower()
        ]

        for row in transco_rows:
            date = datetime.strptime(row['Effective Gas Day'], '%m/%d/%Y').isoformat(' ')
            send_in = row['Total Scheduled Quantity']

            item = USAModuleSendIn()
            item['pipeline'] = 'Transco'
            item['installation_id'] = 217
            item['unit'] = 'MMBTU'
            item['date'] = date
            item['value'] = send_in

            yield item
