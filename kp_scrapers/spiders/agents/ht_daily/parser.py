# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import re

from kp_scrapers.lib.parser import try_apply


def parse_quantity(qty_str):
    """Parse qty field

    Examples:
        >>> parse_quantity('3 450')
        '3450'
        >>> parse_quantity('')

    """
    # strip spaces since qty can have spaces in between, e.g. 25 320
    return try_apply(qty_str.replace(' ', '') or None, float, int, str)


def parse_date(row_date, reported_date):
    date = extract_date(row_date, r'\d+/\d+/\d+')
    if date:  # if field is date instead of 'ANCHORED' or 'ON ROADS'
        return date
    if row_date == 'ON ROADS':  # do not use vessels still on roads
        return None
    return reported_date


def extract_date(raw_str, regex):
    """Extract a single date from string given a regular exp

    Examples:
        >>> extract_date('Daily Update 25-12-2018.pdf', r'\d+-\d+-\d+')
        '25-12-2018'
        >>> extract_date('23/05/18', r'\d+/\d+/\d+')
        '23/05/18'
        >>> extract_date('ON ROADS', r'\d+/\d+/\d+')

    """
    match = re.search(regex, raw_str)
    return match.group(0) if match else None
