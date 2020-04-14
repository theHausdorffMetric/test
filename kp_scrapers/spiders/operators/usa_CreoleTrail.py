# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime
import json

from scrapy.http import Request

from kp_scrapers.models.items import USAModuleSendIn
from kp_scrapers.spiders.bases.persist import PersistSpider

from . import OperatorSpider


class USASabinePassCreoleTrail(OperatorSpider, PersistSpider):

    name = 'SabinePassCreoleTrail'
    allowed_domaine = ['lngconnection.cheniere.com']  # Main Page
    start_urls = [
        'http://lngconnection.cheniere.com/#/report/2/4001/Operationally%2520Available%2520/operationally-available',  # Operationally available capacity page  # noqa
        'http://lngconnectionapi.cheniere.com/api/Capacity/GetCapacity?tspNo=200&beginDate={}&cycleId=null&locationId=0',  # API contains daily values directly  # noqa
    ]

    def start_requests(self):
        today = datetime.now()
        today_str = today.strftime('%m/%d/%Y')

        # Request which hit directly the API containing data jsons
        yield Request(url=self.start_urls[1].format(today_str), callback=self.post)

    def post(self, response):
        data = json.loads(response.body)
        data_list = data['report']

        for row in data_list:
            if row['loc'] == u'CT200111':
                item = USAModuleSendIn()
                item['url'] = self.start_urls[0]
                item['pipeline'] = 'Creole Trail'
                item['installation_id'] = 217
                item['date'] = row['avaiL_CAP_EFF_DT_TIME']
                item['value'] = row['scheD_QTY']
                item['unit'] = 'MMBTU'

                yield item
