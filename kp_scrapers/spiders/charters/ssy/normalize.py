import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


STATUS_MAPPING = {
    'FAILED': SpotCharterStatus.failed,
    'FIXED': SpotCharterStatus.fully_fixed,
    'ON SUBS': SpotCharterStatus.on_subs,
}

UNIT_MAPPING = {'KT': Unit.kilotons, 'KY': Unit.kilotons}  # sic

logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=True)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, charters_mapping())

    # build cargo sub-model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': item.pop('cargo_unit', None),
    }

    # enrich laycan dates with year and month
    item['lay_can_start'], item['lay_can_end'] = normalize_laycan(
        item.pop('laycan'), year=parse_date(item['reported_date']).year
    )

    return item


def charters_mapping():
    return {
        '0': ('vessel', lambda x: {'name': x}),
        '1': ('status', lambda x: STATUS_MAPPING.get(x.partition(' ')[0])),
        '2': ('cargo_volume', None),
        '3': ('cargo_unit', lambda x: UNIT_MAPPING.get(x, x)),
        '4': ('cargo_product', None),
        '5': ('departure_zone', None),
        '6': ('arrival_zone', lambda x: x.split('-')),
        '7': ('laycan', None),
        '8': ('rate_value', None),
        '9': ('charterer', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: normalize_reported_date(x)),
    }


def normalize_laycan(raw_laycan, year):
    """Normalize raw laycan date.

    Raw laycan inputs can be of the following formats:
        1) range: '02-03 SEP'
        2) range with month rollover: '30-01 AUG'
        3) single day: '28 AUG'
        4) month only: 'AUG'

    Args:
        raw_laycan (str):
        year (str | int): string numeric of report's year

    Returns:
        Tuple[str]: tuple of laycan period

    Examples:
        >>> normalize_laycan('02-03 SEP', '2018')
        ('2018-09-02T00:00:00', '2018-09-03T00:00:00')
        >>> normalize_laycan('30-01 AUG', '2018')
        ('2018-08-30T00:00:00', '2018-09-01T00:00:00')
        >>> normalize_laycan('28 AUG', '2018')
        ('2018-08-28T00:00:00', '2018-08-28T00:00:00')
        >>> normalize_laycan('30-01 DEC', '2018')
        ('2018-12-30T00:00:00', '2019-01-01T00:00:00')
        >>> normalize_laycan('AUG', '2018')
        (None, None)
        >>> normalize_laycan('ABC', '2018')
        (None, None)
    """
    # first, account for scenario #4 above:
    if len(raw_laycan.split(' ')) == 1:
        return None, None

    # extract the month and date range first
    date_range, month = raw_laycan.split(' ')
    # in case data extracted not in right format
    try:
        month = dt.datetime.strptime(month, '%b').month
    except ValueError:
        logger.error(f'Unknown raw month format: {month}')
        return None, None

    # laycan dates may/may not be a range
    if len(date_range.split('-')) == 1:
        start = end = date_range
    elif len(date_range.split('-')) == 2:
        start, end = date_range.split('-')
    else:
        logger.error(f'Unknown raw laycan format: {raw_laycan}')
        return None, None
    # init laycan period
    # sometimes, we may be presented with dates like `31-3/6`, which contains a month rollover
    # hence, we need to use try/except
    # try except is also used to take into account end dates for feb
    try:
        lay_can_start = dt.datetime(year=int(year), month=int(month), day=int(start))
    except ValueError:
        lay_can_start = dt.datetime(year=int(year), month=int(month - 1), day=int(start))

    try:
        lay_can_end = dt.datetime(year=int(year), month=int(month), day=int(end))
    except ValueError:
        lay_can_end = dt.datetime(year=int(year), month=int(month) + 1, day=int(end))

    if lay_can_start > lay_can_end:
        lay_can_end += relativedelta(months=1)

    return lay_can_start.isoformat(), lay_can_end.isoformat()


def normalize_reported_date(reported_date):
    """Normalize reported date

    Args:
        reported_date (str):

    Returns:
        str:

    Examples:
        >>> normalize_reported_date('FW: SIMPSON SPENCE YOUNG EAST OF SUEZ REPORT - WEDNESDAY 26TH FEBRUARY 2020') # noqa
        '26 Feb 2020'
        >>> normalize_reported_date('EAST OF SUEZ REPORT - TUESDAY 25TH FEBRUARY 2020') # noqa
        '25 Feb 2020'
    """
    _match = re.match(r'.*?\s+(\d{1,2}.*\d{2,4})', reported_date)
    if _match:
        return parse_date(_match.group(1)).strftime('%d %b %Y')
