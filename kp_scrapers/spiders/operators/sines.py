# -*- coding: utf-8 -*-

"""SinesOperator:

Default behavior retrieves yesterday's information History mode: if provided
with a start_date argument, retrieves the data for all the days between
start_date and yesterday (inclusive).  If provided with end_date argument,
between start_date and end_date (inclusive) Retrieves an XML file containing a
table from the mercado website (porto de Sines)

site: http://www.mercado.ren.pt/EN/Gas/MarketInfo/TGNL/Infrastructure/Pages/default.aspx

Example:

    DAILY MODE:
    $ scrapy crawl SinesOperator

    HISTORY MODE:
    $ scrapy crawl SinesOperator -a start_date=<START_DATE>
    $ scrapy crawl SinesOperator -a start_date=<START_DATE> \
                                 -a end_date=<END_DATE>

Attributes:

    UNIT (string): The unit of the extracted information
    (the same for all table columns).

    DATE_FORMAT (string): The date format argumant used to parse
    dt.date object properties from string

    BASE_URL (string): The URL template used to build the start_urls
    used by the spider's start_requests function

"""

from __future__ import absolute_import, unicode_literals
import datetime as dt

from dateutil.relativedelta import relativedelta
from scrapy.http import Request
from scrapy.selector import Selector

from kp_scrapers.models.items_inventory import IOItem

from . import OperatorSpider
from .base import BaseOperatorSpider
from .utils import convert_to_unique_unit


UNIT = 'MWH'
DATE_FORMAT = '%Y-%m-%d'
BASE_URL = 'http://www.mercado.ren.pt/UserPagesGas/Dados_download.aspx?Dia1={}&Dia2={}&Nome=BalancoTGNL'  # noqa


class SinesSpider(OperatorSpider, BaseOperatorSpider):
    """The url dates have to be the start date and the end date of a full month.
    The available file urls are formed with delimiters
    (first day of month, last day of month)
    If self.end_date is in the future

    Attributes:
        name (string): The spider's name for scrapy

    """

    name = 'SinesOperator'

    def __init__(self, start_date=None, end_date=None):
        """Parses optional arguments given through scrapy -a.

        Args:
            start_date (string or NoneType): The date from which
            to start retrieving items.
            end_date (string or NoneType): The date on which
            to stop retrieving items.

        """
        if start_date:
            self.start_date = dt.datetime.strptime(start_date, DATE_FORMAT).date()
            if end_date:
                self.end_date = dt.datetime.strptime(end_date, DATE_FORMAT).date()
            else:
                self.end_date = dt.date.today()
            self.args_validity_check()
        else:
            self.end_date = dt.date.today() - relativedelta(days=1)
            self.start_date = self.end_date

    def args_validity_check(self):
        """Check to make sure that date parameters make sense.

        Raises:
            ValueError: If start_date posterior to end_date,
            or if start_date posterior to today's date

        """
        yesterday = dt.date.today() - relativedelta(days=1)
        if (self.start_date - yesterday).days > 0:
            raise ValueError("start_date: Start earlier" " than yesterday's date")
        if (self.start_date - self.end_date).days > 0:
            raise ValueError("end_date: End date earlier" " than start date")

    def start_requests(self):
        """Create scrapy Requests

        Retrieves:
        - all data between start and end date (history mode)
        - yesterday's data (daily mode)

        Returns:
            Generator of IOItems
        """
        months_to_scrape = get_full_months(self.start_date, self.end_date)
        start_urls = [
            BASE_URL.format(month_delimiters[0], month_delimiters[1])
            for month_delimiters in months_to_scrape
        ]
        for url in start_urls:
            yield Request(url, self.parse_item)

    def parse_item(self, response):
        sel = Selector(response)
        table = sel.xpath('//table')
        trs = table.xpath('.//tr')
        for line in trs[1:]:
            tr_raw = line.xpath('.//text()').extract()
            tr = format_tr(tr_raw)
            try:
                yield self.item_from_tr(tr, response)
            except ValueError:
                self.logger.warning('problem in this row *** %s ***' % tr)

    def item_from_tr(self, tr, response):
        inflow = float(tr[2].replace(',', '.'))
        outflow = float(tr[6].replace(',', '.'))
        level = float(tr[9].replace(',', '.'))
        item = IOItem()
        item["date"] = tr[0]
        item_date = dt.datetime.strptime(tr[0], "%d-%m-%Y").date()
        item["inflow_o"], item["unit"] = convert_to_unique_unit(inflow, UNIT)
        item["outflow_o"] = convert_to_unique_unit(outflow, UNIT)[0]
        item["level_o"] = convert_to_unique_unit(level, UNIT)[0]
        item["src_file"] = response.url
        wanted_date = is_wanted_date(item_date, self.start_date, self.end_date)
        if wanted_date:
            return item


def format_tr(tr_raw):
    tr_r = [i.strip('\n\r\t') for i in tr_raw if i]
    return [i.strip('') for i in tr_r if i]


def get_full_months(start_date, end_date):
    months_to_scrape = []
    month_start = dt.date(start_date.year, start_date.month, 1)
    month_end = get_month_end(month_start)
    while (month_end - true_end(end_date)).days <= 0:
        months_to_scrape.append(
            (month_start.strftime(DATE_FORMAT), month_end.strftime(DATE_FORMAT))
        )
        month_start = month_end + relativedelta(days=1)
        month_end = get_month_end(month_start)
    return months_to_scrape


def is_wanted_date(item_date, start_date, end_date):
    """Check if the item's date is between start an end dates

    Args:
        item_date (dt.date): the date of an item.
        start_date (dt.date): the start date or None
        end_date (dt.date): the end date or None


    Returns:
        True if item_date is between start_date and end_date,
        True if start_date is None and ite_date is yesterday's date,
        False otherwise

    """
    if (item_date - end_date).days <= 0 and (item_date - start_date).days >= 0:
        return True
    return False


def true_end(end_date):
    """Last day of the end_date current month
    if end_date in the past, else last day of today's month

    Returns:
        dt.date

    """
    end_before_today = (dt.date.today() - end_date).days >= 0
    end_date = end_date if end_before_today else dt.date.today()
    curr_month_first_day = dt.date(end_date.year, end_date.month, 1)
    return get_month_end(curr_month_first_day)


def get_month_end(month_start):
    """Util that gets month end form month start

    Returns:
        dt.date

    """
    return month_start + relativedelta(months=1) - relativedelta(days=1)
