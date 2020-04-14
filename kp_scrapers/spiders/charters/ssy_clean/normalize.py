import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MISSING_ROWS = []


STATUS_MAPPING = {
    'SUBS': SpotCharterStatus.on_subs,
    'O/P': SpotCharterStatus.fully_fixed,
    'FXD': SpotCharterStatus.fully_fixed,
    'FLD': SpotCharterStatus.failed,
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, charters_mapping())

    # build cargo item
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    # enrich laycan dates with year
    if item.get('lay_can'):
        item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
            item.pop('lay_can', None), item['reported_date'], raw_item
        )

    return item


def charters_mapping():
    return {
        'ship': ('vessel', lambda x: {'name': x}),
        'status': ('status', lambda x: STATUS_MAPPING.get(x, None)),
        'charterer': ('charterer', None),
        'qty': ('cargo_volume', None),
        'grade': ('cargo_product', None),
        'loadport': ('departure_zone', None),
        'load': ('departure_zone', None),
        'discharge options': ('arrival_zone', lambda x: x.split('/') if x else None),
        'discharge': ('arrival_zone', lambda x: x.split('/') if x else None),
        'rate': ('rate_value', None),
        'laycan': ('lay_can', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported, raw_string_item):
    """Normalize raw lay can date with reference of reported year.

    Raw laycan inputs can be of the following formats:
        1) OFF 04-05 MAY
        2) 4/5 MAY
        3) 4-5 MAY
        4) 31ST MAR
        5) 31-01 APR (31 mar - 01 apr)
        6) PPT

    Examples:
        >>> normalize_lay_can('OFF 04-05 MAY','1 Sep 2018', 'vessel...')
        ('2018-05-04T00:00:00', '2018-05-05T00:00:00')
        >>> normalize_lay_can('29-01 FEB','1 Sep 2018', 'vessel...')
        ('2018-01-29T00:00:00', '2018-02-01T00:00:00')
        >>> normalize_lay_can('31-01 APR', '10 Oct 2018', 'vessel...')
        ('2018-03-31T00:00:00', '2018-04-01T00:00:00')
        >>> normalize_lay_can('31 DEC', '10 Jan 2019', 'vessel...')
        ('2018-12-31T00:00:00', '2018-12-31T00:00:00')
        >>> normalize_lay_can('01 JAN', '10 Dec 2018', 'vessel...')
        ('2019-01-01T00:00:00', '2019-01-01T00:00:00')
        >>> normalize_lay_can('PPT ON', '25 Dec 2018', 'vessel...')
        (None, None)

    Args:
        raw_lay_can (str):
        reported (str): reported date

    Returns:
        Tuple[str, str]: tuple of lay can period, (lay can start, lay can end)

    """
    raw_lay_can = may_strip(raw_lay_can.upper().replace('OFF', ''))

    # format 1, 2, 3, 5
    _match = re.match(r'(\d{1,2})(?:[\-\/])(\d{1,2})\s([A-z]+)', raw_lay_can)

    if _match:
        start_day, end_day, month = _match.groups()
        year = _get_year(month, reported)
        # months can be spelt incorrectly
        try:
            month = parse_date(f'01 {month} {year}', dayfirst=True).month
        except Exception:
            MISSING_ROWS.append(str(raw_string_item))
            return None, None

        end = parse_date(f'{end_day} {month} {year}', dayfirst=True)

        # in the event the rollover has february dates (30-01 FEB), first result would
        # be 30th february which does not exist, hence try and except is needed
        try:
            start = parse_date(f'{start_day} {month} {year}', dayfirst=True)
        except Exception:
            start = parse_date(f'{start_day} {month-1} {year}', dayfirst=True)

        # if it's rollover case
        if start > end:
            start = start + relativedelta(months=-1)

        return start.isoformat(), end.isoformat()

    # format 4
    _match_1 = re.match(r'(\d{1,2})(?:[A-z]+)?\s([A-z]+)', raw_lay_can)
    if _match_1:
        start_day, month = _match_1.groups()
        year = _get_year(month, reported)
        start = parse_date(f'{start_day} {month} {year}', dayfirst=True)

        return start.isoformat(), start.isoformat()

    # format 6
    if 'PPT' in raw_lay_can:
        return None, None

    # unknown formats
    MISSING_ROWS.append(str(raw_string_item))
    logger.error(f'Invalid or unknown lay can format: {raw_lay_can}')

    return None, None


def _get_year(lay_can_str, reported):
    """Get lay can year with reference of reported date.

    Args:
        lay_can_str (str):
        reported (str):

    Returns:
        str:

    """
    year = parse_date(reported).year
    if 'Dec' in reported and 'JAN' in lay_can_str.upper():
        year += 1
    if 'Jan' in reported and 'DEC' in lay_can_str.upper():
        year -= 1

    return year
