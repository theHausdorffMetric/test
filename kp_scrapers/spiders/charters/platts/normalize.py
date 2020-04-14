from calendar import month_abbr, month_name
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to SpotCharter model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    # items with vessel name 'TBN' need to be discarded
    if 'TBN' in raw_item.get('vessel'):
        return

    item = map_keys(raw_item, field_mapping())
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can', ''), item.get('reported_date')
    )

    # departure and arrival_zone
    zone_info = item.pop('origin/destination', None)
    if zone_info:
        item['departure_zone'], _, item['arrival_zone'] = zone_info.partition('/')
        item['arrival_zone'] = [item.pop('arrival_zone')]

    # build cargo
    item['cargo'] = {
        # the source provide information about only coal
        'product': 'coal',
        'volume': int(item.pop('volume', None).replace(',', '')),
        'volume_unit': Unit.tons,
    }

    return item


def field_mapping():
    return {
        'vessel': ('vessel', lambda x: {'name': x}),
        'origin/destination': ('origin/destination', None),
        'loading dates': ('lay_can', None),
        'rate': ('rate_value', None),
        'quantity (mt)': ('volume', None),
        'charterer': ('charterer', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(lay_can, reported_date):
    """ Normalize lay can date with reported year as reference.

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    Examples:
        >>> normalize_lay_can('Feb 12 - April 17', '17 Feb 2019')
        ('2019-02-12T00:00:00', '2019-04-17T00:00:00')
        >>> normalize_lay_can('March 12 - 17', '17 Feb 2019')
        ('2019-03-12T00:00:00', '2019-03-17T00:00:00')
    """
    if '-' not in lay_can:
        return None, None

    start, _, end = lay_can.partition('-')
    lay_can_start = None
    lay_can_end = None

    match_start = re.search(r'[a-zA-Z]*', start.strip())
    if _filter_match_conditon(match_start):
        lay_can_start = _process_date_info(match_start, start, reported_date)

    match_end = re.search(r'[a-zA-Z]*', end.strip())
    if _filter_match_conditon(match_end):
        lay_can_end = _process_date_info(match_end, end, reported_date)
    elif _filter_match_conditon(match_start):
        lay_can_end = _process_date_info(match_start, end, reported_date)

    if lay_can_start > lay_can_end:
        lay_can_end = lay_can_end + relativedelta(years=+1)

    return (
        lay_can_start.isoformat() if lay_can_start else None,
        lay_can_end.isoformat() if lay_can_end else None,
    )


def _filter_match_conditon(match):
    if (
        match
        and match.group()
        and (match.group().title() in month_abbr or match.group().title() in month_name)
    ):
        return True
    else:
        return False


def _process_date_info(matched_string, raw_string, reported_date):
    month = _month_index(matched_string.group())
    year = _get_lay_can_year(month, reported_date)
    day = [int(s) for s in raw_string.split() if s.isdigit()]
    return _build_lay_can_date(day[0], month, year)


def _build_lay_can_date(day, month, year):
    """Build lay can date.

    Args:
        day (int | str):
        month (str):
        year (int):

    Returns:
        datetime.datetime: time in ISO 8601 format

    """
    return parse_date(f'{day} {month} {year}', dayfirst=True)


def _get_lay_can_year(month, reported):
    """Get lay can year.

    Args:
        month (str):
        reported (str):

    Returns:
        int:

    """
    year = parse_date(reported).year
    if ('12' == month or '11' == month) and 'Jan' in reported:
        year -= 1
    if ('01' == month or '1' == month) and 'Dec' in reported:
        year += 1
    return year


def _month_index(month):
    """Return the month number

    Args:
        str

    Return:
        int

    Examples:
        >>> _month_index('Mar')
        3
        >>> _month_index('April')
        4
        >>> _month_index('ApilEnd')
    """
    try:
        # to identify whether month or its abbrevation passed
        if len(month) > 3:
            return list(month_name).index(month.title())
        else:
            return list(month_abbr).index(month.title())
    except ValueError:
        return None
