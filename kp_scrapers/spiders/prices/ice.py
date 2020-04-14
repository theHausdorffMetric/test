# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
import datetime as dt
import logging

from scrapy.http import FormRequest, Request
from scrapy.selector import Selector
from six.moves import range

from kp_scrapers.models.items import StockExchangeIndex
from kp_scrapers.spiders.prices import PriceSpider


logger = logging.getLogger(__name__)


class IceSpider(PriceSpider):
    LOGIN_URL = (
        'http://data.theice.com/MyAccount/Login.aspx?'
        'ReturnUrl=%2fViewData%2fEndOfDay%2fFuturesReport.aspx'
    )
    TABLE_URL = 'http://data.theice.com/ViewData/EndOfDay/FuturesReport.aspx'

    spider_settings = {'DUPEFILTER_CLASS': 'kp_scrapers.settings.NoFilter'}

    def __init__(
        self, futures_id, raw_unit, index, zone, commodity, cf=1, month_ahead=5, source_power=1
    ):
        self.credentials = ('iceicebb', 'PKZ/238-b')
        self.futures_id = futures_id
        self.index = index
        self.zone = zone
        self.commodity = commodity
        self.raw_unit = raw_unit
        self.cf = cf
        self.ahead = month_ahead
        self.power = source_power

    def start_requests(self):
        return [Request(url=self.LOGIN_URL, callback=self.login)]

    def login(self, response):
        sel = Selector(response)

        validation = sel.xpath('//input[@id="__EVENTVALIDATION"]/@value').extract_first()
        state = sel.xpath('//input[@id="__VIEWSTATE"]/@value').extract_first()

        formdata = {
            'ctl00$ContentPlaceHolder1$LoginControl$m_userName': self.credentials[0],
            'ctl00$ContentPlaceHolder1$LoginControl$m_password': self.credentials[1],
            '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$LoginControl$LoginButton',
            '__EVENTVALIDATION': validation,
            '__VIEWSTATE': state,
        }

        yield FormRequest(url=self.LOGIN_URL, formdata=formdata, callback=self.ajax)

    def ajax(self, response):
        sel = Selector(response)

        date = sel.xpath('//input[@id="ctl00_ContentPlaceHolder1_tbDate"]/@value').extract_first()
        validation = sel.xpath('//input[@id="__EVENTVALIDATION"]/@value').extract_first()
        state = sel.xpath('//input[@id="__VIEWSTATE"]/@value').extract_first()

        formdata = {
            'ctl00$ContentPlaceHolder1$ddlFutures': self.futures_id,
            'ctl00$ContentPlaceHolder1$tbDate': date,
            'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1'
            '$UpdatePanel3|ctl00'
            '$ContentPlaceHolder1$ddlFutures',
            '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlFutures',
            '__EVENTVALIDATION': validation,
            '__VIEWSTATE': state,
            '__VIEWSTATEENCRYPTED': '',  # Must remain present && empty
        }

        # Don't ask me why.
        if self.futures_id != '254':
            yield FormRequest(url=self.TABLE_URL, formdata=formdata, callback=self.extract)
        else:
            yield Request(self.TABLE_URL, callback=self.extract)

    def extract(self, response):
        sel = Selector(response)

        t = sel.xpath('//table[@id="ctl00_ContentPlaceHolder1_tblReport_Month_gvReport"]')
        day = sel.xpath('//input[@id="ctl00_ContentPlaceHolder1_tbDate"]/@value').extract_first()
        day = str(dt.datetime.strptime(day, '%m/%d/%Y').date())

        for elt in range(0, self.ahead):
            sett = t.xpath('.//tr[{}]/td[5]/nobr/text()'.format(elt + 2)).extract_first()
            month = t.xpath('.//tr[{}]/td[1]/nobr/text()'.format(elt + 2)).extract_first()
            month = str(dt.datetime.strptime(month, '%b%y').date())
            ticker = self.index + 'N' + str(elt + 1)

            if sett:
                if self.name == 'BrentIce':
                    yield StockExchangeIndex(
                        raw_unit=self.raw_unit,
                        raw_value=float(sett),
                        converted_value=float(sett) * self.cf,
                        source_power=self.power,
                        index=self.index,
                        ticker=ticker,
                        commodity=self.commodity,
                        zone=self.zone,
                        provider='ICE',
                        month=month,
                        day=day,
                    )
                else:
                    yield StockExchangeIndex(
                        raw_unit=self.raw_unit,
                        raw_value=float(sett) * self.cf,
                        source_power=self.power,
                        index=self.index,
                        ticker=ticker,
                        commodity=self.commodity,
                        zone=self.zone,
                        provider='ICE',
                        month=month,
                        day=day,
                    )
            else:
                # handle cases where sett is empty due to market closure
                logger.error(f'Settlement Value (sett) is Empty for {day}')


class UKIceSpider(IceSpider):
    name = 'UKIce'

    def __init__(self):
        super(UKIceSpider, self).__init__(
            futures_id='236',  # Asked by the provider
            raw_unit='gbp/therm',
            index='NBP',
            zone='United Kingdom',
            commodity='lng',
            cf=0.01,  # idk why
            month_ahead=5,  # We scrape the 5 values ahead
        )


class NLIceSpider(IceSpider):
    name = 'NLIce'

    def __init__(self):
        super(NLIceSpider, self).__init__(
            futures_id='4331',  # Asked by the provider
            raw_unit='eur/mwh',
            index='TTF',
            zone='Netherlands',
            commodity='lng',
            month_ahead=5,  # We scrape the 5 values ahead
        )


class BrentIceSpider(IceSpider):
    name = 'BrentIce'

    def __init__(self):
        super(BrentIceSpider, self).__init__(
            futures_id='254',  # Asked by the provider
            raw_unit='usd/bbl',
            index='Brent',
            zone='North Sea',
            commodity='lng',
            cf=0.115,  # idk why
            month_ahead=5,  # We scrape the 5 values ahead
        )
