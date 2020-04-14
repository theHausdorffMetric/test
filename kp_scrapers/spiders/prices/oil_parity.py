# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime
import json

from scrapy.http import Request

from kp_scrapers.models.items import StockExchangeIndex
from kp_scrapers.spiders.prices import PriceSpider


class OilParitySpider(PriceSpider):
    name = 'OilParity'

    GET_LIST_URL = (
        'https://www.theice.com/marketdata/DelayedMarkets.shtml'
        '?getContractsAsJson&productId=254&hubId=403'
    )
    GET_BARS_URL = (
        'https://www.theice.com/marketdata/DelayedMarkets.shtml'
        '?getIntradayChartDataAsJson=&marketId={market_id}'
    )

    def start_requests(self):
        return [Request(url=self.GET_LIST_URL, method='GET', callback=self.get_market_id)]

    def get_market_id(self, response):
        r = json.loads(response.body)

        yield Request(
            url=self.GET_BARS_URL.format(market_id=r[0]['marketId']),
            method='GET',
            callback=self.get_last_price,
        )

    @staticmethod
    def get_last_price(response):
        r = json.loads(response.body)

        month = str(datetime.strptime(r['stripDescription'], '%b%y').date())
        day = str(datetime.now())
        yield StockExchangeIndex(
            raw_unit='usd/bbl',
            raw_value=r['settlementPrice'],
            converted_value=r['settlementPrice'] * 0.1724,
            difference_value=r['change'],
            index='Oil Parity',
            zone='North Sea',
            commodity='lng',
            provider='ICE',
            month=month,
            day=day,
            source_power=1,
        )
