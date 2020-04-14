from calendar import month_abbr, month_name
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        Dict[str, str]:

    Returns:
        Dict[str, Any]:

    """
    # vessel containing 'TBN' to be ignored
    if 'TBN' in raw_item.get('vsl name'):
        return

    item = map_keys(raw_item, field_mapping())
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('laycan', ''), item.get('reported_date')
    )

    item['reported_date'] = parse_date(
        item.pop('reported_date') if item.get('reported_date', None) else None
    )

    # cargo model
    item['cargo'] = {
        'product': 'coal',
        'volume': item.pop('volume', '').replace(',', '').split('/')[0],
        'volume_unit': Unit.tons,
        'movement': 'load',
    }

    return item


def field_mapping():
    return {
        'vsl name': ('vessel', lambda x: {'name': x}),
        'l/port': ('departure_zone', None),
        "cargo q'ty": ('volume', None),
        'd/port': ('arrival_zone', lambda x: [x]),
        'laycan': ('laycan', None),
        'frt(usd/ton)': ('rate_value', None),
        'terms': ('rate_raw_value', None),
        'charterer': ('charterer', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(lay_can, reported_date):
    """ Build lay can date

    Args:
        lay_can (str):
        reported_date(str):

    Returns:
        Tuple[str, str]

    Examples:
        >>> normalize_lay_can('3/8 Apr', '2019-03-21')
        ('2019-04-03T00:00:00', '2019-04-08T00:00:00')
        >>> normalize_lay_can('23 March /8 Apr', '2019-03-21')
        ('2019-03-23T00:00:00', '2019-04-08T00:00:00')

    """

    if '/' not in lay_can:
        return None, None

    start, _, end = lay_can.partition('/')
    lay_can_start = None
    lay_can_end = None

    match_start = re.search(r'([0-9]*)(?P<month>\D*)', start.strip())
    match_end = re.search(r'([0-9]*)(?P<month>\D*)', end.strip())

    if _filter_match_conditon(match_start):
        lay_can_start = _process_date_info(match_start, start, reported_date)

    if _filter_match_conditon(match_end):
        lay_can_end = _process_date_info(match_end, end, reported_date)
        if lay_can_start is None:
            lay_can_start = _process_date_info(match_end, start, reported_date)

    return (
        lay_can_start.isoformat() if lay_can_start else None,
        lay_can_end.isoformat() if lay_can_end else None,
    )


def _filter_match_conditon(match):
    if (
        match
        and match.group('month')
        and (
            match.group('month').strip().title() in month_abbr
            or match.group('month').strip().title() in month_name
        )
    ):
        return True
    else:
        return False


def _process_date_info(matched_string, raw_string, reported_date):
    month = _month_index(matched_string.group('month').strip())
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
    """
    try:
        # to identify whether month or its abbrevation passed
        if len(month) > 3:
            return list(month_name).index(month.title())
        else:
            return list(month_abbr).index(month.title())
    except ValueError:
        return None
