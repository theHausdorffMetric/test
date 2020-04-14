import datetime as dt

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.utils import validate_item


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, charters_mapping())

    # normalize laycan dates
    item['lay_can_start'], item['lay_can_end'] = normalize_laycan(
        item.pop('laycan'), d_year=item['date_checker'].year, d_month=item['date_checker'].month
    )

    item.pop('date_checker')

    return item


def charters_mapping():
    return {
        'charterer': ('charterer', normalize_charterer),
        'destination': ('arrival_zone', lambda x: [may_strip(x)] if may_strip(x) else []),
        'laycan': ('laycan', None),
        'origin': ('departure_zone', None),
        'provider_name': ('provider_name', None),
        'quantity': ignore_key('not required for now'),
        'rate': ('rate_value', lambda x: x if x else None),
        'date_checker': ('date_checker', None),
        'reported_date': (
            'reported_date',
            # strip timezone
            # NOTE charter loader needs a dayfirst date string; ISO8601 not recommended
            lambda x: parse_date(x[:-6]).strftime('%d %b %Y'),
        ),
        'vessel_name': ('vessel', lambda x: {'name': may_strip(x)}),
    }


def normalize_charterer(raw_charterer):
    """Cleanup and normalize raw charterer name.

    This function will remove blacklisted charterer strings, and strip empty spaces.

    Args:
        raw_charterer (str):

    Returns:
        str:

    Examples:
        >>> normalize_charterer('CNOOC')
        'CNOOC'
        >>> normalize_charterer('VITOL?')
        'VITOL'
        >>> normalize_charterer('?')
        ''
    """
    if raw_charterer.endswith('?'):
        raw_charterer = raw_charterer.replace('?', '')
    return raw_charterer if raw_charterer else ''


def normalize_laycan(raw_laycan, d_year, d_month):
    """Normalize raw laycan date.

    Args:
        raw_laycan (str):
        d_year (int): integer of date checker
        d_month (int): integer of date checker

    Returns:
        Tuple[str]: tuple of laycan period

    Examples:
        >>> normalize_laycan('26-28 MAR', 2019, 3)
        ('2019-03-26T00:00:00', '2019-03-28T00:00:00')
        >>> normalize_laycan('31-03 JAN', 2018, 12)
        ('2018-12-31T00:00:00', '2019-01-03T00:00:00')
        >>> normalize_laycan('30-02 JAN', 2019, 1)
        ('2018-12-30T00:00:00', '2019-01-02T00:00:00')
        >>> normalize_laycan('12 MAR', 2018, 3)
        ('2018-03-12T00:00:00', '2018-03-12T00:00:00')
    """
    # extract the month and date range first
    month = parse_date(raw_laycan.split()[1]).month
    date_range = raw_laycan.split()[0].split('-')

    # laycan dates may/may not be a range
    if len(date_range) == 1:
        start = end = date_range[0]
    elif len(date_range) == 2:
        start, end = date_range
    else:
        raise ValueError(f'Unknown raw laycan format: {raw_laycan}')

    # normalize yearss
    d_year = d_year + 1 if (d_month == 12 and month == 1) else d_year

    # init laycan period
    # sometimes, we may be presented with dates like `31-03 MAR`, which contains a month rollover
    # hence, we need to use try/except
    lay_can_end = dt.datetime(year=d_year, month=month, day=int(end))
    try:
        lay_can_start = dt.datetime(year=d_year, month=month, day=int(start))
    except ValueError:
        lay_can_start = dt.datetime(year=d_year, month=month - 1, day=int(start))
    if lay_can_start > lay_can_end:
        lay_can_start -= relativedelta(months=1)

    return lay_can_start.isoformat(), lay_can_end.isoformat()
