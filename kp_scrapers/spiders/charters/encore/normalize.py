import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import get_last_day_of_current_month, to_isoformat
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


STATUS_FAILED = ['FLD', 'FAILED']

STATUS_FLAG = ['FXD', 'RPTD', 'RPLC', 'EX', 'LVLS', 'BOT', 'STILL', '2ND']

NOT_KNOWN_RATE = 'R N R'

VAGUE_DAY = ['ELY', 'MID', 'END']

DAY_MAPPING = {'ELY': ('1', '7'), 'MID': ('14', '21')}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        SpotCharter | None

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if item['rate_value'] == 'FAIL' or not item['vessel']:
        return

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )
    item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage'))

    # build cargo sub-model
    item['cargo'] = normalize_cargo(item.pop('volume'), item.pop('product'))

    return item


def field_mapping():
    return {
        '0': ('charterer', None),
        '1': ('volume', None),
        '2': ('product', None),
        '3': ('voyage', None),
        '4': ('lay_can', None),
        '5': ('vessel', normalize_vessel),
        '6': ('rate_value', normalize_rate_value),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_cargo(volume, product):
    """Normalize cargo.

    Args:
        volume (str):
        product (str):

    Returns:
        Dict[str, str]:

    """
    return {'product': product, 'movement': 'load', 'volume': volume, 'volume_unit': Unit.kilotons}


def normalize_vessel(raw_vessel_name):
    """Filter irrelevant vessel name.

    Args:
        raw_vessel_name (str):

    Returns:
        Dict[str, str]:

    """
    return None if 'TBN' in raw_vessel_name.split() else {'name': raw_vessel_name}


def normalize_lay_can(raw_lay_can, reported):
    """Append year to lay can date.

    Lay can start date format:
        1. 19/8, 24-25/8 (normal case)
        2. END/9, EAY/1 (vague case)
        3. DNR, PPT -> empty
        4. 31-1/4 (Rollover dates)

    Steps of the algorithm:
        1. deal with year shift issue
        2. END -> 24-30, ELY -> 1-7
        3. DNR, PPT -> empty

    Examples:
        >>> normalize_lay_can('23-25/8', '10 Aug 2018')
        ('2018-08-23T00:00:00', '2018-08-25T00:00:00')
        >>> normalize_lay_can('FLD', '10 Aug 2018')
        (None, None)
        >>> normalize_lay_can('1-4/1', '29 Dec 2018')
        ('2019-01-01T00:00:00', '2019-01-04T00:00:00')
        >>> normalize_lay_can('END/12', '1 Jan 2019')
        ('2018-12-25T00:00:00', '2018-12-31T00:00:00')
        >>> normalize_lay_can('31-1/4', '1 MAR 2019')
        ('2019-03-31T00:00:00', '2019-04-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        str:

    """
    # normalize day and month
    day, _, month = raw_lay_can.partition('/')
    if not month:
        return None, None

    year = parse_date(reported).year
    # Year shift Dec case
    if month == '12' and 'Jan' in reported:
        year -= 1
    # Year shift Jan case
    if (month == '1' or month == '01') and 'Dec' in reported:
        year += 1

    # assemble lay can end
    if day in VAGUE_DAY:
        start_day, end_day = _day_mapping(day, month, year)
    else:
        start_day, _, end_day = day.partition('-')
    try:
        lay_can_start = to_isoformat(f'{start_day} {month} {year}')
    except ValueError:
        lay_can_start = to_isoformat(f'{start_day} {int(month) - 1} {year}')

    lay_can_end = to_isoformat(f'{end_day} {month} {year}') if end_day else lay_can_start

    if parse_date(lay_can_start) > parse_date(lay_can_end):
        lay_can_start = to_isoformat(f'{start_day} {int(month) - 1} {year}')

    return lay_can_start, lay_can_end


def _day_mapping(flag, month, year):
    """Extract the start day and end day.
     Args:
        flag (str): ELY | MID | END
        month (str):
        year (int):
     Returns:
     """
    if flag in DAY_MAPPING:
        return DAY_MAPPING[flag]
    last_day = get_last_day_of_current_month(month, year, '%m', '%Y')
    return str(last_day - 6), str(last_day)


def normalize_voyage(raw_voyage):
    """Get departure zone and arrival zones.


    Examples:
        >>> normalize_voyage('AIN SUKHNA/MED – SPORE')
        ('AIN SUKHNA', ['MED ', ' SPORE'])
        >>> normalize_voyage('AIN SUKHNA/OPTS')
        ('AIN SUKHNA', None)

    Args:
        raw_voyage (str):

    Returns:
        Tuple(str, List[str]): departure_zone, arrival_zone

    """
    departure_zones, _, arrival_zone = raw_voyage.partition('/')
    departure_zone, _, another = departure_zones.partition('+')

    if not arrival_zone or 'OPTS' in arrival_zone:
        return departure_zone, None

    return departure_zone, arrival_zone.replace('–', '-').split('-')


def normalize_rate_value(raw_rate):
    """Rate value may contain status info, remove status and identify failed status.

    Examples:
        >>> normalize_rate_value('R N R ( FAILED )')
        'FAIL'
        >>> normalize_rate_value('R N R STILL ON SUB')
        >>> normalize_rate_value('WS89 RPLC DS MELODY WS83')
        'WS89'
        >>> normalize_rate_value('(P/C) 280K')
        '280K'

    Args:
        raw_rate (str):

    Returns:
        str | None

    """
    # failed status
    if any(True for x in STATUS_FAILED if x in raw_rate):
        return 'FAIL'

    # empty rate
    if NOT_KNOWN_RATE in raw_rate:
        return

    raw_rate = _remove_brackets(raw_rate)

    if ')' in raw_rate:
        raw_rate = raw_rate[raw_rate.index(')') :]

    # identify possible status
    for status in STATUS_FLAG:
        if status in raw_rate:
            return raw_rate[: raw_rate.index(status)].strip()

    return raw_rate


def _remove_brackets(raw_str):
    """Remove the brackets and the characters inside from a string.

    Args:
        raw_str (str):

    Returns:
        str:

    """
    return re.sub(r'\([^\(\)]+\)', '', raw_str).strip()
