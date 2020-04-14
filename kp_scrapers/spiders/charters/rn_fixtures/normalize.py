# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import datetime as dt

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_remove_substring, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.utils import validate_item


ZONE_SUBSTR_BLACKLIST = ['STS', '+1', '+ 1']

ZONE_MAPPING = {'CPC': 'NOVOROSSIYSK', 'EAST': 'FAR EAST'}

PERSIAN_GULF = 'Persian Gulf'
PERSIAN_GULF_ZONES = [
    'jebel',
    'dhana',
    'mubarraz',
    'zirku',
    'halul',
    'fateh',
    'sirri',
    'ras laffan',
    'al shaheen',
    'al rayyan',
    'ryrus',
    'lavan',
    'ras tanura',
    'das is',
    'juaymah',
    'r.tan',
    'basrah',
    'khafiji',
    'mina saud',
    'barhegan',
    'assaluyeh',
    'lavan',
    'fujairah',
    'muscat',
    'cyrus',
    'barhegan',
    'mina al fahal',
    'bashayer',
    'muda',
    'ruwais',
    'meg',
    'fuj',
    'maa',
    'maf',
    'bot',
]


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

        Args:
            raw_item (Dict[str, str]):

        Returns:
            Dict[str, str]:

        """
    item = map_keys(raw_item, spot_charter_mapping())
    # `lay_can` dates do not contain year, so we need to obtain it from `reported_date`
    _year = item['reported_date'].split(' ')[-1]
    _raw_laycan = item['lay_can_start'].replace(' ', '') + _year
    item['lay_can_start'] = dt.datetime.strptime(_raw_laycan, '%d%b%Y').isoformat()

    return item


def spot_charter_mapping():
    return {
        'arrival_zone': ('arrival_zone', normalize_arrival_zone),
        'cargo': ('cargo', lambda x: {'product': may_strip(x)}),
        'charterer': ('charterer', parse_charterer),
        'departure_zone': ('departure_zone', normalize_departure_zone),
        'lay_can_start': ('lay_can_start', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'vessel_name': ('vessel', lambda x: {'name': may_strip(x)}),
    }


def normalize_departure_zone(raw_zone):
    """Normalize departure zones.

    Args:
        raw_zone (str):

    Returns:
        str:

    Examples:
        >>> normalize_departure_zone('BASRAH-KAA')
        'Persian Gulf'
        >>> normalize_departure_zone('BASRAH')
        'BASRAH'
        >>> normalize_departure_zone('BASRAH+1')
        'Persian Gulf'
        >>> normalize_departure_zone('BUKIT TUA+1')
        'BUKIT TUA'
        >>> normalize_departure_zone('STS YOSU')
        'YOSU'
    """
    if is_persian_gulf_zone(raw_zone):
        return PERSIAN_GULF

    zone = may_strip(may_remove_substring(raw_zone, ZONE_SUBSTR_BLACKLIST))
    for alias in ZONE_MAPPING:
        if alias in zone.lower():
            return ZONE_MAPPING[alias]

    return zone


def normalize_arrival_zone(raw_zone):
    """Normalize arrival zones.

    We don't care about persian gulf macro zones when it is an arrival zone.

    Args:
        raw_zone (str):

    Returns:
        List[str]:

    Examples:
        >>> normalize_arrival_zone('BASRAH-KAA')
        ['BASRAH', 'KAA']
        >>> normalize_arrival_zone('BASRAH')
        ['BASRAH']
        >>> normalize_arrival_zone('BASRAH+1')
        ['BASRAH']
        >>> normalize_arrival_zone('BUKIT TUA+1')
        ['BUKIT TUA']
        >>> normalize_arrival_zone('STS YOSU')
        ['YOSU']
    """
    arrival_zone = []
    for single_zone in raw_zone.split('-'):
        zone = may_strip(may_remove_substring(single_zone, ZONE_SUBSTR_BLACKLIST))
        for alias in ZONE_MAPPING:
            if alias in zone.lower():
                arrival_zone.append(ZONE_MAPPING[alias])
                break

        arrival_zone.append(zone)

    return arrival_zone if arrival_zone else [raw_zone]


def is_persian_gulf_zone(raw_zone):
    """Check if a raw zone is within Persian Gulf and contains multiple zones.

    Examples:
        >>> is_persian_gulf_zone('BASRAH-KAA')
        True
        >>> is_persian_gulf_zone('BASRAH')
        False
        >>> is_persian_gulf_zone('BASRAH+1')
        True
        >>> is_persian_gulf_zone('BUKIT TUA+1')
        False
        >>> is_persian_gulf_zone('STS YOSU')
        False
    """
    return any(z in raw_zone.lower() for z in PERSIAN_GULF_ZONES) and (
        '+' in raw_zone or '-' in raw_zone
    )


def parse_charterer(charterer):
    """Removes dirty substrings from charterer

    As the regex does not match rows perfectly due to inconsistent spacing,
    the charterer regex group will tend to include useless info, often numeric.

    Examples:
        >>> parse_charterer('5 BP')
        'BP'
        >>> parse_charterer('4.675M SK ENERGY')
        'SK ENERGY'
        >>> parse_charterer('CSSSSA')
        'CSSSSA'

    """
    if len(charterer.split()) > 1:
        return ' '.join(word for word in charterer.split() if word.isalpha())
    else:
        return charterer


def parse_reported_date(date):
    """Converts fuzzy reported date into serialized_format defined by spot_charter.py

    Examples:
        >>> parse_reported_date('29 TH JUNE 2018')
        '29 Jun 2018'
        >>> parse_reported_date('30TH MAY 2018')
        '30 May 2018'

    """
    return parse_date(date).strftime('%d %b %Y')
