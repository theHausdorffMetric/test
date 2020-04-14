# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from scrapy import Spider
from scrapy.http import Request
from six.moves.urllib import parse

from kp_scrapers.lib.date import create_str_from_time, may_parse_date_str
from kp_scrapers.models.items_inventory import IOItem

from . import OperatorSpider
from .utils import kwh_from_km3lng


DATE_FORMAT = '%d/%m/%Y'
DEFAULT_UNIT = 'KWH'
EMPTY_VALUE = '-'
DEFAULT_HISTORIC = 10
MONTH_AHEAD = 2  # Website is displaying at most data for 2 month ahead

HEADERS = {
    'Host': 'www.fosmax-lng.com',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': (
        'https://www.fosmax-lng.com/fr/nos-services/donnees-operationnelles'
        '/donnees-d-utilisation/recherches.html?article1=&article2='
    ),
    'Content-Type': 'application/x-www-form-urlencoded',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}


class FosCavaouSpider(OperatorSpider, Spider):

    name = 'FosCavaouOperator'
    start_url = (
        'https://www.fosmax-lng.com/fr/nos-services/donnees-operationnelles'
        '/donnees-d-utilisation/recherches.html?article1=&article2='
    )
    page = None

    def __init__(self, start_date=None, end_date=None, historic=None):
        """Fos Cavaou Spider

        We scrappe data 10 days in the past as the operator may update past data.
        Website is displaying data for the whole next month.

        Args:
            start_date (str, optional): Format YYYY-mm-dd, default (today - 10 days).
            end_date (str, optional): Format YYYY-mm-dd,
                                      default last day of start_date's next month.
        """
        historic = int(historic) if historic else DEFAULT_HISTORIC
        if start_date:
            self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            self.start_date = datetime.now() - timedelta(days=historic)
        if end_date:
            self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            # Last day of the next month
            self.end_date = (self.start_date + relativedelta(months=MONTH_AHEAD)).replace(
                day=1
            ) - timedelta(days=1)

    def _build_request(self):
        post_data = [
            ('jform[terminal]', '1'),
            ('jform[type]', '1'),
            ('jform[jour1]', self.start_date.day),
            ('jform[mois1]', self.start_date.month),
            ('jform[annee1]', self.start_date.year),
            ('jform[jour2]', self.end_date.day),
            ('jform[mois2]', self.end_date.month),
            ('jform[annee2]', self.end_date.year),
            ('jform[start]', self.page),
            ('jform[export]', '0'),
            ('submit', 'Visualiser'),
            ('option', 'com_transparence'),
            ('view', 'recherches'),
            ('a2af421b6895e6fa35eaff8ac7fcd198', '1'),
        ]

        return Request(
            method='POST',
            url=self.start_url,
            body=parse.urlencode(post_data),
            headers=HEADERS,
            callback=self.parse_simple_page,
        )

    def start_requests(self):
        self.page = 1
        yield self._build_request()

    @staticmethod
    def level_o_check(level, date):
        # LNG inventory at the beginning of the gas day is expressed in
        # kilowatt-hour until 30 September 2014
        # and in thousand cubic meters of LNG since 1st October 2014.
        if date < datetime(2014, 10, 1):
            return level
        else:
            return kwh_from_km3lng(float(level))

    def parse_simple_page(self, response):
        table = response.xpath('//table[@class="flux"]//tr')
        # Skip header
        for row in table[1:-1]:
            item = IOItem(unit=DEFAULT_UNIT)
            extracted_row = row.xpath('td/text()').extract()
            io_datetime = may_parse_date_str(extracted_row[0], DATE_FORMAT)
            item['date'] = create_str_from_time(io_datetime + timedelta(hours=18))
            lvl = extracted_row[1].replace(' ', '')
            if lvl != EMPTY_VALUE:
                item['level_o'] = self.level_o_check(lvl, io_datetime)
            else:
                # If the stock level is not set,
                # scrapping output is meaningless as website display 0
                continue

            # Scraping Nominated quantities as output_forcast and
            # Allocated quan. as output (SENDOUT)
            out = extracted_row[3].replace(' ', '')
            if out != EMPTY_VALUE:
                item['output_o'] = out

            out_forecast = extracted_row[2].replace(' ', '')
            if out_forecast != EMPTY_VALUE:
                item['output_forecast_o'] = out_forecast

            # if '-', take the output_forecast value into output
            if item.get('output_o') is None:
                item['output_o'] = item.get('output_forecast_o')

            yield item

        pagination_bloc = response.xpath('//div[@class="pagination-bloc"]/a/text()').extract()
        if 'Suivante >' in pagination_bloc:
            self.page += 1
            yield self._build_request()
