import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import get_last_day_of_current_month
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


VAGUE_DAY_MAPPING = {'BEG': (1, 7), 'ELY': (1, 7), 'EARLY': (1, 7), 'MID': (14, 21)}

MONTH_MAPPING = {'SEPT': 'SEP'}

STATUS_MAPPING = {
    'UPDT': SpotCharterStatus.fully_fixed,
    'FXD': SpotCharterStatus.fully_fixed,
    'REPORTED': SpotCharterStatus.fully_fixed,
    'RPLC': SpotCharterStatus.fully_fixed,
    'SUBS': SpotCharterStatus.on_subs,
}

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
    if not item['vessel'] or not item.get('vessel').get('name'):
        return

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        'VESSEL': ('vessel', lambda x: {'name': x} if 'TBN' not in x.split() else None),
        'YEAR BUILT': ignore_key('build_year'),
        'FLEET': ignore_key('fleet'),
        'CARGO': ('cargo', normalize_cargo),
        'LOADPORT': ('departure_zone', None),
        'DISPORT': ('arrival_zone', lambda x: [alias.strip() for alias in x.split('/')]),
        'LAYCAN': ('lay_can', None),
        'RATE': ('rate_value', None),
        'CHARTERER': ('charterer', None),
        'NOTES': ('status', normalize_status),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_cargo(raw_cargo):
    """Normalize cargo.

    Examples:
        >>> normalize_cargo('100fo')
        {'product': 'fo', 'movement': 'load', 'volume': '100', 'volume_unit': 'kilotons'}
        >>> normalize_cargo('80')

    Args:
        raw_cargo:

    Returns:

    """
    _find_cargo = re.search(r'(\d{2,3})([A-Za-z]+)', raw_cargo)
    if _find_cargo:
        volume, product = _find_cargo.groups()
        return {
            'product': product,
            'movement': 'load',
            'volume': volume,
            'volume_unit': Unit.kilotons,
        }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can pattern:
        - Normal case: 6-Nov, 5-7-Sept
        - Vague case: end Sept

    Month rollover case not spotted yet.

    Examples:
        >>> normalize_lay_can('6-Nov', '5 Nov 2018')
        ('2018-11-06T00:00:00', '2018-11-06T00:00:00')
        >>> normalize_lay_can('5-7-Sept', '5 Nov 2018')
        ('2018-09-05T00:00:00', '2018-09-07T00:00:00')
        >>> normalize_lay_can('end Sept', '5 Nov 2018')
        ('2018-09-23T00:00:00', '2018-09-30T00:00:00')
        >>> normalize_lay_can('28 Dec', '1 Jan 2019')
        ('2018-12-28T00:00:00', '2018-12-28T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    raw_lay_can = raw_lay_can.upper()

    _normal_match = re.match(r'(\d{1,2}).(\d{1,2}.)?([A-Za-z]{3,4})', raw_lay_can)
    if _normal_match:
        start_day, end_day, month = _normal_match.groups()
        year = _get_lay_can_year(month, reported)

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    _vague_match = re.match(r'(ELY|EARLY|BEG|MID|END).([A-Za-z]{3,4})', raw_lay_can)
    if _vague_match:
        vague, month = _vague_match.groups()
        month = MONTH_MAPPING.get(month, month)
        year = _get_lay_can_year(month, reported)
        start_day, end_day = VAGUE_DAY_MAPPING.get(vague, (None, None))
        if vague == 'END':
            end_day = get_last_day_of_current_month(month, year, '%b', '%Y')
            start_day = end_day - 7

        lay_can_start = _build_lay_can_date(start_day, month, year)
        lay_can_end = _build_lay_can_date(end_day, month, year)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    logger.error(f'New lay can pattern spotted: {raw_lay_can}, reported date: {reported}')


def _get_lay_can_year(month, reported):
    """Get the year of lay can date with reference of reported date.

    Args:
        month:
        reported:

    Returns:
        int:

    """
    year = parse_date(reported).year
    if 'DEC' == month and 'Jan' in reported:
        year -= 1
    if 'JAN' == month and 'Dec' in reported:
        year += 1
    return year


def _build_lay_can_date(day, month, year):
    """Build lay can date.

    Args:
        day:
        month:
        year:

    Returns:
        datetime.datetime:

    """
    return parse_date(f'{day} {month} {year}', dayfirst=True)


def normalize_status(raw_status):
    """Normalize status.

    Examples:
        >>> normalize_status('subs')
        'On Subs'
        >>> normalize_status('rplc fsl piraeus')
        'Fully Fixed'

    Args:
        raw_status:

    Returns:

    """
    raw_status = raw_status.upper()
    for status in STATUS_MAPPING:
        if status in raw_status:
            return STATUS_MAPPING.get(status)
