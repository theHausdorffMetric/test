import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


VESSEL_TYPE_RELEVANT = ['bulk carrier', 'cargo ship', 'gas tanker', 'oil tanker', 'tanker']


ZONE_SUBSTRING_BLACKLIST = ['coast', 'of', 'off', 'the']


FLAGS = {
    'JPN': 'JP',
    'PAN': 'PA',
    'BHS': 'BS',
    'MHL': 'MH',
    'KOR': 'KR',
    'LBR': 'LR',
    'GBR': 'GB',
    'HKG': 'HK',
    'CYP': 'CY',
    'MLT': 'MT',
    'PHL': 'PH',
    'SGP': 'SG',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    # map/normalize individual raw fields to something resembling a valid event
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard vessels with irrelevant types
    if item['vessel_type'].lower() not in VESSEL_TYPE_RELEVANT:
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'length': item.pop('vessel_length'),
        'gross_tonnage': item.pop('vessel_gt'),
        'flag_code': item.pop('vessel_flagcode'),
        'vessel_type': item.pop('vessel_type'),
    }

    # build ETA
    item['eta'] = normalize_eta_date(item['eta'], item['reported_date'])

    return item


def portcall_mapping():
    return {
        '0': ('eta', None),
        '1': ('vessel_name', None),
        '2': ('vessel_type', lambda x: x.title()),
        '3': ('vessel_gt', None),
        '4': ('vessel_length', None),
        '5': ignore_key('type'),
        '6': ('vessel_flagcode', lambda x: FLAGS.get(x)),
        # TODO could use the following URL for greater zone mapping precision
        # http://www.kaiho.mlit.go.jp/03kanku/h22houkaisei/sozai/guide3_e.pdf
        '7': ('port_name', clean_zone_name),
        '8': ignore_key('nakanose route'),
        '9': ignore_key('pilot'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_eta_date(raw_date, raw_reported_date):
    """eta dates do not contain the year

    Format:
    1. mm/dd hh:ss

    Args:
        raw_date (str):
        raw_reported_date (str):

    Examples:
        >>> normalize_eta_date('04/07 04:35', '2019-04-03T10:01:00')
        '2019-04-07T04:35:00'
        >>> normalize_eta_date('12/30 01:00', '2019-12-30T10:01:00')
        '2019-12-30T01:00:00'
        >>> normalize_eta_date('12/30 01:00', '2020-01-01T10:01:00')
        '2019-12-30T01:00:00'
        >>> normalize_eta_date('01/03 08:45', '2019-12-30T10:01:00')
        '2020-01-03T08:45:00'

    Returns:
        str:
    """
    year = parse_date(raw_reported_date).year
    _match = re.match(r'(\d{2})\/(\d{2})\s(\d{2})\:\s?(\d{2})', raw_date)

    if _match:
        _month, _day, _hour, _min = _match.groups()

        eta = dt.datetime(
            year=int(year), month=int(_month), day=int(_day), hour=int(_hour), minute=int(_min)
        )

        # to accomodate end of year parsing, prevent dates too old or far into
        # the future. 100 days was chosen as a gauge
        if (eta - parse_date(raw_reported_date)).days < -100:
            eta = dt.datetime(
                year=int(year) + 1,
                month=int(_month),
                day=int(_day),
                hour=int(_hour),
                minute=int(_min),
            )

        if (eta - parse_date(raw_reported_date)).days > 100:
            eta = dt.datetime(
                year=int(year) - 1,
                month=int(_month),
                day=int(_day),
                hour=int(_hour),
                minute=int(_min),
            )

        return eta.isoformat()

    logger.error('unable to parse %s', raw_date)

    return None


def clean_zone_name(raw_name):
    """Clean a translated zone string.

    Args:
        str (raw_name):

    Returns:
        str: cleaned zone name

    Examples:
        >>> clean_zone_name('Tokyo (Tokyo west route)')
        'Tokyo'
        >>> clean_zone_name('Minami Honmoku')
        'Minami Honmoku'
        >>> clean_zone_name('Off the coast of chiba')
        'Chiba'
        >>> clean_zone_name('Cosmo Seabass')
        'Chiba'

    """
    # remove parantheses
    raw_name = raw_name.split('(')[0].strip()

    # tokenise name and remove irrelevant substrings
    cleaned_name = []
    for _token in [sub.lower() for sub in raw_name.split()]:
        if _token not in ZONE_SUBSTRING_BLACKLIST:
            cleaned_name.append(_token.title())

    # special case; take care of cosmo/keiyo sea-berth (mapped to Chiba)
    if 'Cosmo' in cleaned_name or 'Keiyo' in cleaned_name:
        return 'Chiba'

    return ' '.join(cleaned_name)
