# -*- coding: utf-8; -*-

"""Pareto Spot Charter source parsing.

Pareto is a free source for spot charters.
website: http://paretoship.com/services/tanker-chartering

"""

from __future__ import absolute_import
import datetime as dt
import re

from scrapy.spiders import Spider

from kp_scrapers.models.items import SpotCharter, VesselIdentification
from kp_scrapers.spiders.bases.markers import DeprecatedMixin
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.utils import (
    create_voyage_raw_text,
    parse_arrival_zones,
    parse_rate,
)


SINGLE_DATE_RGX = re.compile(r'(\d{2})\.(\d{2})')
PATH_TO_TANKER_TAB = '//table[contains(@class, "table-tanker")]'
PATH_TO_MAIN = '//main'
PATH_TO_REPORTED_DATE = PATH_TO_MAIN + '//div[contains(@class, "col-md-10")]/text()'
PATH_TO_XML_DATA = PATH_TO_TANKER_TAB + '/tbody/tr'
VESSEL_TYPE = 'Crude Oil Tanker'
VESSEL_STATUS = 'Fully Fixed'
DATE_PATTERN = r'\d+.\d+.\d+'


def to_date(month, day):
    # The year could be wrong around the 1st of January if this Japanese
    # website computes the date based on a JST datetime (UTC+9).
    return dt.date(dt.datetime.utcnow().year, int(month), int(day))


def parse_lay_day(loading_date):
    """Parse date formated as 'dd.mm'."""
    lay_date = None

    single_date = SINGLE_DATE_RGX.match(loading_date)
    if single_date:
        day, month = single_date.groups()
        lay_date = to_date(month, day)

    return lay_date


def _strip_new_lines(some_str):
    return some_str.replace('\n', '').replace('\r', '')


class ParetoSpider(DeprecatedMixin, CharterSpider, Spider):
    """Pareto Shipbrokers.

    """

    name = 'Pareto'
    start_urls = ['http://paretoship.com/services/tanker-chartering']

    def parse(self, response):
        reported_date = [
            re.search(DATE_PATTERN, el).group(0)
            for el in response.xpath(PATH_TO_REPORTED_DATE).extract()
            if re.search(DATE_PATTERN, el)
        ][0]
        reported_date = dt.datetime.strptime(reported_date, '%d.%m.%Y').date()
        return [
            self._charter_factory(row, reported_date)
            for row in response.xpath(PATH_TO_XML_DATA)[1:]
        ]

    def _charter_factory(self, table_row, reported_date):
        (vessel_name, quantity, lay_day, departure_zone, arrival_zone, rate, charterer) = [
            i.extract() for i in table_row.xpath('.//td/text()')
        ]
        # this is painful and error-prone to work with quantity as a string further down
        quantity = int(quantity) if quantity else None

        vessel = VesselIdentification(name=vessel_name, type=VESSEL_TYPE, dwt=int(quantity))
        lay_can_start = lay_can_end = rate_value = None
        # New rate formats appear often.
        # We want to continue scraping in case of parsing failure.
        try:
            lay_can_start = lay_can_end = parse_lay_day(lay_day)
            rate_value = parse_rate(rate, quantity)
        except (TypeError, ValueError) as err:
            self.logger.warning('failed to parse line: {err}'.format(err=err))
            # we might still have interesting data to report (`rate_value`
            # could be set to RNR) so let's go on

        departure_zone = _strip_new_lines(departure_zone)
        arrival_zone = parse_arrival_zones(_strip_new_lines(arrival_zone))
        voyage_raw_text = create_voyage_raw_text(departure_zone, arrival_zone)
        return SpotCharter(
            vessel=vessel,
            lay_can_start=lay_can_start,
            lay_can_end=lay_can_end,
            departure_zone=departure_zone,
            arrival_zone=arrival_zone,
            charterer=_strip_new_lines(charterer),
            status=VESSEL_STATUS,
            rate_raw_value=rate,
            rate_value=rate_value,
            reported_date=reported_date,
            voyage_raw_text=voyage_raw_text,
        )
