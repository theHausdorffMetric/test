# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import copy
from datetime import date
import re

from scrapy.http import FormRequest, Request
import six
from six.moves import range

from kp_scrapers.lib.date import rewind_time
from kp_scrapers.models.items import Customs
from kp_scrapers.spiders.bases.markers import LngMarker, LpgMarker, OilMarker
from kp_scrapers.spiders.customs.base import CustomsBaseSpider


DEFAULT_PRICE_CURRENCY = 'EUROS'
DEFAULT_WEIGHT_CURRENCY = 'quintal'


class EurostatCustomsSpider(LngMarker, LpgMarker, OilMarker, CustomsBaseSpider):
    """
    For each year, for each commodity a bookmark is specified to be used in URL
    Divided into many bookmarks to avoid reaching the max num of rows
    """

    name = 'EurostatCustoms'
    version = '1.0.0'
    provider = 'EurostatCustoms'

    url = 'http://appsso.eurostat.ec.europa.eu/nui/show.do'

    bookmarks = {
        2014: {
            '271111': '-285D8AAC',  # LNG HS code
            '271112': '-250431B0',  # Propane HS code
            '271113': '-2CFD326E',  # Butane HS code
            '270900': '5FD554E4',
        },  # Oil HS code
        2015: {
            '271111': '-6745EB3',
            '271112': '-6014332E',
            '271113': 'B8764B4',
            '270900': '588AF00',
        },
        2016: {
            '271111': '-212C2578',
            '271112': '49F47775',
            '271113': '65DF8155',
            '270900': '29988814',
        },
    }
    flows = list(range(1, 3))  # Import: 1 or Export: 2

    def commodity_mapping(self):
        return {
            'lng': {'271111': None},
            'lpg': {'271112': 'propane', '271113': 'butane'},
            'oil': {'270900': None},
        }

    # Preparing the URL with the different parameter
    def start_requests(self):
        # This one request is used to get a cookie
        yield Request(url=self.url, callback=self.post)

    def post(self, response):
        self.logger.info('Rewinding {} months before current month'.format(self.months_look_back))

        # get the previous date to scrape
        past_dates = rewind_time(current_date=date.today(), months=self.months_look_back)
        years = set([past_date.year for past_date in past_dates])

        for product in self.products:
            for year in years:
                months = [past_date.month for past_date in past_dates if past_date.year == year]
                for month in months:
                    # format date to put in the url to request
                    period = str(year) + '%02d' % (month,)
                    for flow in self.flows:
                        formdata = {
                            "query": "BOOKMARK_DS-016893_QID_{bookmark}_UID_-3F171EB0".format(
                                bookmark=self.bookmarks[year][product]
                            ),
                            "layout": "PARTNER,L,X,0;"
                            "INDICATORS,C,X,1;"
                            "REPORTER,L,Y,0;"
                            "PRODUCT,L,Z,0;FLOW,L,Z,1;"
                            "PERIOD,L,Z,2;",
                            "zSelection": "DS-016893FLOW,{flow};"
                            "DS-016893PERIOD,{period};"
                            "DS-016893PRODUCT,{code};".format(
                                flow=flow, period=period, code=product
                            ),
                            "rankName1": "FLOW_1_2_-1_2",
                            "rankName2": "PRODUCT_1_2_-1_2",
                            "rankName3": "PERIOD_1_0_1_1",
                            "rankName4": "PARTNER_1_2_0_0",
                            "rankName5": "INDICATORS_1_2_1_0",
                            "rankName6": "REPORTER_1_2_0_1",
                            "rStp": "",
                            "cStp": "",
                            "rDCh": "",
                            "cDCh": "",
                            "rDM": "true",
                            "cDM": "true",
                            "empty": "false",
                            "footnes": "false",
                            "wai": "false",
                            "time_mode": 'NONE',
                            "time_most_recent": "false",
                            "lang": "EN",
                            "cfo": "%23%23%23%2C%23%23%23.%23%23%23",
                        }

                        yield FormRequest(
                            formdata=formdata,
                            url='http://appsso.eurostat.ec.europa.eu/nui/show.do',
                            callback=self.parse_values,
                            meta={'flow': flow, 'year': year, 'month': month, 'product': product},
                        )

    # Parsing the results
    def parse_values(self, response):

        if not response.body:
            self.logger.error('No response body to be parsed')
            return
        error_check = ['Bookmark parsing error', 'Unexpected error']
        if any(i in response.body for i in error_check):
            self.logger.warning('Invalid format page')
            return
        flow = 'Import' if response.meta['flow'] == 1 else 'Export'
        month = int(response.meta['month'])
        year = int(response.meta['year'])
        product_code = response.meta['product']

        self.logger.debug('Getting items for month %s, year %s, and flow %s' % (month, year, flow))

        col_index = re.search(r'var xIndex="([0-9a-f]*)";', response.body).group(1)
        row_index = re.search(r'var yIndex="([0-9a-f]*)";', response.body).group(1)
        data_index = re.search(r'var dataIndex="([0-9a-f]*)";', response.body).group(1)
        col_value = re.search(r'var xValues="(.*)";', response.body).group(1)
        row_value = re.search(r'var yValues="(.*)";', response.body).group(1)
        data_value = re.search(r'var dataValues="(.*)";', response.body).group(1)

        row_dimension = Dimension(row_index, row_value)
        col_dimension = Dimension(col_index, col_value)
        data_cells = Dimension(data_index, data_value)
        items = self.aggregate_fields(row_dimension, col_dimension, data_cells)
        for subcommo, commodity in six.iteritems(self.get_subcommodities(product_code)):
            default_item = Customs()
            default_item['url'] = response.url
            default_item['type'] = flow
            default_item['month'] = month
            default_item['year'] = year
            default_item['raw_price_currency'] = DEFAULT_PRICE_CURRENCY
            default_item['raw_weight_units'] = DEFAULT_WEIGHT_CURRENCY
            default_item['commodity'] = subcommo or commodity
            for item_key, item_values in six.iteritems(items):
                new_item = EurostatCustomsSpider.fill_in(default_item, item_key, item_values)
                yield new_item

    @staticmethod
    def fill_in(item_base, players, values):
        item = copy.copy(item_base)
        item['country_name'] = players[0]
        item['source_country'] = players[1]
        item['raw_price'] = values.get(DEFAULT_PRICE_CURRENCY)
        item['raw_weight'] = values.get(DEFAULT_WEIGHT_CURRENCY)
        return item

    def aggregate_fields(self, row_dimension, column_dimension, cells):
        items = {}
        for row_idx in range(0, row_dimension.items_count):
            row_item = row_dimension.items[row_idx].split('|')
            for column_idx in range(0, column_dimension.items_count):
                column_item = column_dimension.items[column_idx].split('|')
                data_cell = cells.items[column_idx + row_idx * column_dimension.items_count]
                value_cell = EurostatCustomsSpider.format_value(data_cell.split('|')[0])
                if value_cell:
                    players = (
                        EurostatCustomsSpider.format_player_names(row_item[7]),
                        EurostatCustomsSpider.format_player_names(column_item[7]),
                    )
                    if players in items:
                        items[players][self.check_unit(column_item[11])] = value_cell
                    else:
                        items[players] = {self.check_unit(column_item[11]): value_cell}
        return items

    def check_unit(self, unit):
        if unit == "VALUE_IN_EUROS":
            return DEFAULT_PRICE_CURRENCY
        elif unit == "QUANTITY_IN_100KG":
            return DEFAULT_WEIGHT_CURRENCY
        else:
            self.logger.error("Value unknown %s" % unit)

    @staticmethod
    def format_value(value):
        return re.sub(r'[,:]', '', value)

    @staticmethod
    def format_player_names(name):
        return re.sub(r'\([^)]*\)', '', name).strip()


class Dimension(object):
    def __init__(self, index_str, field_str):
        # indexes define the index of each new item in the item string.
        # the indexes is a concatenation of hexadecimal index.
        self.indexes = self.get_indexes(index_str)
        self.items = self.get_values(field_str)
        self.items_count = len(self.items)

    def get_indexes(self, index_str):
        return [int(index_str[i : i + 8], 16) for i in range(0, len(index_str), 8)]

    def get_values(self, value_str):
        res = []
        for idx in range(0, len(self.indexes) - 1):
            curr_idx = self.indexes[idx]
            next_idx = self.indexes[idx + 1]
            res.append(value_str[curr_idx:next_idx])
        return res
