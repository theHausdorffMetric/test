import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MISSING_ROWS = []


LAYCAN_MAPPING = {'ELY': '1', 'MID': '15', 'END': '27'}


STATUS_MAPPING = [
    ('subs', SpotCharterStatus.on_subs),
    ('fxd', SpotCharterStatus.fully_fixed),
    ('rplc', SpotCharterStatus.fully_fixed),
    ('rptd', SpotCharterStatus.fully_fixed),
    ('old', SpotCharterStatus.fully_fixed),
    ('fld', SpotCharterStatus.failed),
]


ZONE_MAPPING = {'ras tan': 'Ras Tanura', 'das isl': 'Das Island', 'yosu': 'Yeosu'}


ZONE_BLACKLIST = ['+1']


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, charters_mapping())

    # remove to be nominated vessels
    if 'TBN' in item['vessel']['name']:
        return

    # enrich laycan dates with year source might not provide lay_can_start
    # in the first col
    item['lay_can_start'] = normalize_lay_can(
        item.pop('lay_can_start', None) if item.get('lay_can_start') else item.get('lay_can_end'),
        item.get('reported_date'),
        item,
    )
    item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can_end', None), item.get('reported_date'), item
    )

    # cargo model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.tons,
        'movement': 'load',
    }

    return item


def charters_mapping():
    return {
        '0': ('vessel', lambda x: {'name': normalize_vessel(x)}),
        '1': ('cargo_volume', None),
        '2': ('cargo_product', None),
        '3': ('departure_zone', normalize_dept_zone),
        '4': ('arrival_zone', lambda x: x.strip().split('-')),
        '5': ('lay_can_start', None),
        '6': ('lay_can_end', None),
        '7': ('rate_value', None),
        '8': ('charterer', None),
        '9': ('status', normalize_status),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported, r_item):
    """Normalize raw lay can date with reference of reported year.

    Raw laycan inputs can be of the following formats:
        1) single day: '15/04'
        2) vague day: 'END/05'

    Examples:
        >>> normalize_lay_can('15/04', '1 Apr 2018', 'vessel...')
        '2018-04-15T00:00:00'
        >>> normalize_lay_can('01/01', '1 Dec 2018', 'vessel...')
        '2019-01-01T00:00:00'
        >>> normalize_lay_can('30/12', '1 Jan 2019', 'vessel...')
        '2018-12-30T00:00:00'

    Args:
        raw_lay_can (str):
        reported (str): reported date

    Returns:
        Tuple[str, str]: tuple of lay can period, (lay can start, lay can end)
    """
    if raw_lay_can:
        _day, _month = raw_lay_can.split('/')
        _year = _get_year(_month, reported)

        if _day.isdigit():
            return to_isoformat(f'{_day} {_month} {_year}', dayfirst=True)
        if _day in ['ELY', 'MID', 'END']:
            return to_isoformat(f'{LAYCAN_MAPPING.get(_day)} {_month} {_year}', dayfirst=True)

        MISSING_ROWS.append(str(r_item))
        logger.warning('Unknown lay can date pattern: %s', raw_lay_can)

        return None


def _get_year(month_string, reported):
    """Get lay can year with reference of reported date.

    Args:
        month_string (str):
        reported (str):

    Returns:
        str:

    """
    year = parse_date(reported).year
    if 'Dec' in reported and '01' in month_string.upper():
        year += 1
    if 'Jan' in reported and '12' in month_string.upper():
        year -= 1

    return year


def normalize_vessel(raw_vsl):
    """Clean vessel string

    Examples:
        >>> normalize_vessel('TORM DODO')
        'TORM DODO'
        >>> normalize_vessel('TORM DODO')
        'TORM DODO'
        >>> normalize_vessel('TORM DODO (EX-DD)')
        'TORM DODO'
        >>> normalize_vessel('(ABC) TORM DODO')
        'TORM DODO'

    Args:
        raw_vsl (str):

    Returns:
        str:

    """
    # _vsl_match = re.match(r'(^([^()]+))', raw_vsl)
    _vsl_match = re.search(
        r'((\()([A-Z]*)(\)))*(?P<Vessel_name>[A-z ]+)((\()([A-Z]*)(\)))*', raw_vsl
    )

    if _vsl_match:
        return may_strip(_vsl_match.group('Vessel_name'))

    return raw_vsl


def normalize_status(raw_status):
    """normalize status string

    Examples:
        >>> normalize_status('RATE ADDED/FXD')
        'Fully Fixed'
        >>> normalize_status('FLD')
        'Failed'
        >>> normalize_status('ADDED RATE')

    Args:
        raw_status (str):

    Returns:
        str:

    """
    if raw_status:
        for _status in STATUS_MAPPING:
            if _status[0] in raw_status.lower():
                return _status[1]

    return None


def normalize_dept_zone(raw_dept):
    """Clean departure zone

    Examples:
        >>> normalize_dept_zone('RAS TAN+1')
        'Ras Tanura'
        >>> normalize_dept_zone('CHINA')
        'CHINA'

    Args:
        raw_dept (str):

    Returns:
        str:

    """
    zone = may_strip(may_remove_substring(raw_dept, ZONE_BLACKLIST))
    for alias in ZONE_MAPPING:
        if alias in zone.lower():
            return ZONE_MAPPING[alias]

    return zone
