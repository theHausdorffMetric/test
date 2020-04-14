# -*- coding: utf-8 -*-

"""Lloyds utilities also useful for scraping the website directly."""

from __future__ import absolute_import
import datetime as dt
import logging

from kp_scrapers.lib import parser
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.items import Cargo, SpotCharter, VesselIdentification
from kp_scrapers.models.utils import filter_item_fields
from kp_scrapers.spiders.charters.utils import create_voyage_raw_text, parse_rate


logger = logging.getLogger(__name__)

MONTH = {
    'Jan': 1,
    'Feb': 2,
    'Mar': 3,
    'Apr': 4,
    'May': 5,
    'Jun': 6,
    'Jul': 7,
    'Aug': 8,
    'Sep': 9,
    'Oct': 10,
    'Nov': 11,
    'Dec': 12,
}


def parse_charterer(x):
    """Replace not reported data by python equivalent: None.

    Example:
        >>> parse_charterer('Charterer Not Reported')

        >>> parse_charterer('whatever')
        'whatever'

    """
    return None if x == 'Charterer Not Reported' else x


def parse_lay_day(raw_date):
    """Convert custom format to datetime type.

    The function supports DNR (Date Not Reported) although it wasn't seen in production for now.

    Example:
        >>> parse_lay_day('Jul 06')
        datetime.date(2020, 7, 6)
        >>> parse_lay_day('Dec 24')
        datetime.date(2020, 12, 24)

        >>> parse_lay_day('DNR')
        >>> parse_lay_day('Date Not Reported')

    """
    if raw_date == 'DNR' or raw_date == 'Date Not Reported':
        # return the equivalent Python unknown value
        return None

    this_year = dt.datetime.utcnow().year
    return dt.date(
        this_year, MONTH[raw_date[0:3]], int(raw_date[4:])  # 3-letters date format
    )  # day as int after a space


def key_map():
    return {
        'Voyage From': ('departure_zone', parser.may_strip),
        'Voyage To': ('arrival_zone', parser.may_strip),
        'Tonnes': ('dwt', lambda x: parser.may_apply(x, int)),
        # nothing is casted for those two values since they only are raw
        # strings used for parsing the real rate value
        'Rate Amount': ('rate', None),
        'Rate Unit': ('unit', None),
        'Lay Date': ('lay_can_start', parse_lay_day),
        'Charterer Name': ('charterer', parse_charterer),
        # support both web and api response
        'Vessel Name': ('vessel_name', None),
        'Vessel': ('vessel_name', None),
    }


def extract_item(raw_extract):
    """Method to parse an item from a row using the parsings and the
    transformations created in the parsers and transformations properties.

    """
    row = map_keys(raw_extract, key_map(), skip_missing=False)

    dwt = int(row['dwt']) if row['dwt'] else None
    # although Lloyds don't have this notion, our database and the other sources do.
    # so we stick to the same da structure (which is relevant here, although less accurate)
    row['lay_can_end'] = row['lay_can_start']

    row['vessel'] = VesselIdentification(dwt=dwt, name=row['vessel_name'])
    row['cargo'] = Cargo(ton=dwt, commodity=row['commodity'])

    try:
        row['rate_value'] = parse_rate(row['rate'] + row['unit'], dwt)
        row['rate_raw_value'] = row['rate'] + row['unit']
    except ValueError as error:
        logger.warning('parsing failed on "{}": {}'.format(row['rate'] + row['unit'], error))
        return None

    row['departure_zone'] = row['departure_zone']
    row['arrival_zone'] = [row['arrival_zone']]
    row['voyage_raw_text'] = create_voyage_raw_text(row['departure_zone'], row['arrival_zone'])
    row = filter_item_fields(SpotCharter, row)

    return SpotCharter(row)
