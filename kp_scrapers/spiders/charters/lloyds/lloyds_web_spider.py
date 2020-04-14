# -*- coding: utf-8 -*-

"""Lloyds Spider

This spider crawls Clarksons charters available on the Lloyds website:
https://www.lloydslist.com/ll/login.htm.
It crawls the last page available, and the date is recorded in the
'reported_date' field of the items.

!!! This website has to be accessed from Singapore IPs because the credentials
are from a Singapore University Account. !!!

Glossary
~~~~~~~~

- {R,C,D}NR: {Rate, Charterer, Date} Not Reported (will be ignored and set to None)

"""

from __future__ import absolute_import

import dateutil.parser
from scrapy.http import FormRequest, Request
from scrapy.selector import Selector
from scrapy.spiders import Spider
from six.moves import zip

from kp_scrapers.settings import HTTP_PROXY
from kp_scrapers.spiders.bases.markers import DeprecatedMixin
from kp_scrapers.spiders.charters import CharterSpider

from .common import extract_item


LLOYDS_CREDENTIALS = {
    'j_username': 'tanz0106@e.ntu.edu.sg',
    'j_password': 'merlion2017',
    # This 'token' key is there because it is required by the website
    'token': '',
}

# Access the website not directly but through Singapore proxy
LLOYDS_URL = HTTP_PROXY + '/lloyds/ll/login.htm'
LLOYDS_FIXTURES_URL = HTTP_PROXY + '/lloyds/ll/marketdata/tankers/tankerFixturesPage.htm'


def parse_reported_date(sel):
    # NOTE we might need to return None if 'DNR' is found
    xpath_ = './/form[contains(@id, "marketDataFixturesForm")]//option/text()'
    raw_date = sel.xpath(xpath_).extract()
    return dateutil.parser.parse(raw_date[0]).strftime('%Y-%m-%d') if len(raw_date) else None


def parse_html_table_rows(table, **kwargs):
    """Extract the table rows from a table selector as a dict with the
       headers of the HTML table as keys.

    """
    headers = table.xpath('.//tr/th/text()').extract()
    rows = table.xpath('.//tr')
    rows = [row.xpath('.//td') for row in rows][1:]
    for row_raw in rows:
        row = {
            header: ''.join(col.xpath('./text()').extract())
            for header, col in zip(headers, row_raw)
        }
        row.update(kwargs)
        yield row


def extract_rows(response):
    def commodity_parser(i):
        return {'commodity': 'other'} if i == 0 else {'commodity': 'None'}

    sel = Selector(response)
    added_fields = {'reported_date': parse_reported_date(sel)}
    tables = sel.xpath('//table[contains(@class, "ll-index-table")]')
    for i, table in enumerate(tables):
        added_fields.update(commodity_parser(i))
        for row in parse_html_table_rows(table, **added_fields):
            yield row


class LloydsSpider(DeprecatedMixin, CharterSpider, Spider):

    name = 'Lloyds'

    version = '1.0.0'
    provider = 'lloyds'

    start_urls = [LLOYDS_URL]

    def __init__(self, history=False):
        super(LloydsSpider, self).__init__()
        self.history_mode = history
        self.callback = self.get_history if self.history_mode else self.parse_items

    def parse(self, response):
        yield self.login(response)

    def login(self, response):
        return FormRequest.from_response(
            response,
            formdata=LLOYDS_CREDENTIALS,
            formxpath='//form[contains(@class, "log-in")]',
            # needed to be forced because FormRequest with
            # empty formdata sent a GET
            method='POST',
            callback=self.after_login,
        )

    def after_login(self, response):
        yield Request(LLOYDS_FIXTURES_URL, callback=self.callback)

    def get_history(self, response):
        sel = Selector(response)
        dates = sel.xpath(
            './/form[contains(@id, "marketDataFixturesForm")]' '//option/text()'
        ).extract()
        for date in dates:
            yield FormRequest.from_response(
                response,
                method='POST',
                formdata={'selectedDate': date, 'isSubmit': 'true'},
                formxpath='.//form[contains(@id, "marketDataFixturesForm")]',
                callback=self.parse_items,
            )

    def parse_items(self, response):
        for row in extract_rows(response):
            item = extract_item(row)
            if item is None:
                self.logger.warning('failed to parse item: {}'.format(row))
                continue

            yield item
