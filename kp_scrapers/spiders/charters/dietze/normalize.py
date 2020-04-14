import calendar
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.utils import validate_item


VAGUE_DAY_REPLACE = {'EARLY': '1-7', 'MID': '14-21'}

LAY_CAN_PATTERN = r'(\d{1,2}).(\d{1,2}|END).?(\d{1,2})?'

MONTH = {
    'JAN': '01',
    'FEB': '02',
    'MAR': '03',
    'APR': '04',
    'MAY': '05',
    'JUN': '06',
    'JUL': '07',
    'AUG': '08',
    'SEPT': '09',
    'SEP': '09',
    'OCT': '10',
    'NOV': '11',
    'DEC': '12',
}

STATUS_MAPPING = {
    'FLD': SpotCharterStatus.failed,
    'DENIED': SpotCharterStatus.failed,
    'OLD': SpotCharterStatus.fully_fixed,
    'RPLC': SpotCharterStatus.fully_fixed,
    'RCNT': SpotCharterStatus.on_subs,
    'RPTD': SpotCharterStatus.on_subs,
}

STATUS_SIGN = ['-', '(', ',']

logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if not item['vessel']:
        return

    item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage'))
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )
    item['charterer'], item['status'] = normalize_charterer(item.pop('charterer_status'))
    if item['status'] == SpotCharterStatus.failed:
        return

    return item


def field_mapping():
    return {
        '0': ('vessel', lambda x: {'name': x} if 'TBN' not in x.split() else None),
        '1': ignore_key('size'),
        '2': ignore_key('cargo'),
        '3': ('voyage', None),
        '4': ('lay_can', None),
        '5': ('rate_value', None),
        '6': ('charterer_status', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can date may comes in different patterns:
    1. normal: 8/22-24, 9/7
    2. vague: 08/END, EARLY SEPT
    3. empty: 9/--

    As you see, there are two formats of representing the date:
    1. 08/12-13, 08/END
    2. 12-13 AUG, END AUG

    In this function, we deal with it in the following steps:
    1. change the date format into 08/21 format
    2. map vague day to actual day, for EARLY and MID
    3. get lay can start year
    4. map vague day to actual day, for END
    5. parse lay can date

    In this function, we don't handle cross month situation for now, as the report hasn't got one,
    if there is month cross situation, validation will fail, and then we deal with month rollover.


    Examples:
        >>> normalize_lay_can('8/22-24', '08 August 2018')
        ('2018-08-22T00:00:00', '2018-08-24T00:00:00')
        >>> normalize_lay_can('08/END', '08 August 2018')
        ('2018-08-25T00:00:00', '2018-08-31T00:00:00')
        >>> normalize_lay_can('EARLY SEPT', '08 August 2018')
        ('2018-09-01T00:00:00', '2018-09-07T00:00:00')
        >>> normalize_lay_can('9/--', '08 August 2018')
        (None, None)

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    # unify date format
    lay_can = raw_lay_can
    for month in MONTH:
        if month in lay_can:
            lay_can = month_first(lay_can, month)
            break

    # replace vague day as actual day if can
    for pattern, days in VAGUE_DAY_REPLACE.items():
        if pattern in lay_can:
            lay_can = lay_can.replace(pattern, days)

    match = re.match(LAY_CAN_PATTERN, lay_can)
    if not match:
        logger.warning(
            f'Invalid lay can, before transform: {raw_lay_can}, after transform: {lay_can}'
        )
        return None, None

    month, start_day, end_day = match.groups()
    year = parse_date(reported).year

    # year rollover for lay can start
    if month == '12' and 'Jan' in reported:
        year -= 1
    if (month == '01' or month == '1') and 'Dec' in reported:
        year += 1

    # replace vague day of `END`
    if 'END' in start_day:
        start_day, end_day = get_end_day_range(month, year)

    lay_can_start = to_isoformat(f'{start_day} {month} {year}', dayfirst=True)
    lay_can_end = (
        to_isoformat(f'{end_day} {month} {year}', dayfirst=True) if end_day else lay_can_start
    )

    return lay_can_start, lay_can_end


def month_first(raw_date, month):
    """Month abbreviation to number and month first.

    Examples:
        >>> month_first('EARLY SEPT', 'SEPT')
        '09/EARLY'

    Args:
        raw_date (str):
        month (str):

    Returns:
        str:

    """
    return '/'.join([MONTH[month], raw_date.replace(month, '')]).strip()


def get_end_day_range(month, year):
    """Get end day range of specific month and year.

    Examples:
        >>> get_end_day_range('08', 2018)
        (25, 31)

    Args:
        month (str):
        year (int):

    Returns:
        Tuple[str, str]:

    """
    last_day = calendar.monthrange(year, int(month))[1]
    return last_day - 6, last_day


def normalize_voyage(raw_voyage):
    """Normalize departure zone and arrival zone from voyage field.

    Args:
        raw_voyage (str):

    Returns:
        Tuple[str, List[str]]: departure zone, arrival zone

    """
    departure, _, arrivals = raw_voyage.partition('/')
    return departure, arrivals.replace('–', ',').split(',')


def normalize_charterer(raw_charterer):
    """Normalize charterer and extract status field if exists.

    Examples:
        >>> normalize_charterer('VALERO – OLD')
        ('VALERO', 'Fully Fixed')
        >>> normalize_charterer('DAY HARVEST (DEM: $29K)')
        ('DAY HARVEST', None)
        >>> normalize_charterer('PETROBRAS,RPLC')
        ('PETROBRAS', 'Fully Fixed')
        >>> normalize_charterer('BOSTON O/O')
        ('BOSTON', None)

    Args:
        raw_charterer (str):

    Returns:
        Tuple[str, str]:

    """
    raw_charterer = raw_charterer.replace('–', '-')

    # fully fixed status by default, update status if there's any
    status = None
    for sign in STATUS_MAPPING:
        if sign in raw_charterer:
            status = STATUS_MAPPING[sign]

    # remove everything after status signs: - , (
    # if they all appear, will return - 's idx first
    for sign in STATUS_SIGN:
        idx = raw_charterer.find(sign)
        if idx != -1:
            raw_charterer = raw_charterer[:idx]
            break

    charterer = raw_charterer.replace('O/O', '').replace('P/C', '').replace('S/S', '').strip()

    return charterer, status
