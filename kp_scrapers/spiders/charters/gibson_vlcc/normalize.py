from datetime import timedelta
import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import get_last_day_of_current_month
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.utils import validate_item


VESSEL_STATUS_PATTERN = r'([^()]+)(\(.*\))?'

logger = logging.getLogger(__name__)

STATUS_MAPPING = {'fld': SpotCharterStatus.failed, 'rplc': SpotCharterStatus.fully_fixed}

ZONE_MAPPING = {'ag': 'Persian Gulf', 'wafr': 'WAF', 'ras tan': 'Ras tanura'}

VAGUE_DAY_MAPPING = {'ely': (1, 7), 'early': (1, 7), 'mid': (14, 21)}

DATE_PATTERN_NORMAL = r'(\d{1,2})-?(\d{1,2})?/(\d{1,2})'

DATE_PATTERN_ROLLOVER = r'(\d{1,2})-(\d{1,2})/(\d{1,2})-(\d{1,2})'

DATE_PATTERN_VAGUE = r'^([a-z]+).(\d{1,2})$'

TO_REMOVE = r's/s|p/c|c/c|o/o'


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    item['vessel'], item['status'] = normalize_vessel(item.pop('vessel_status'))
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    if not item['vessel']:
        return

    return item


def field_mapping():
    return {
        '0': ('vessel_status', None),
        '1': ignore_key('size'),
        '2': ('lay_can', None),
        '3': ('departure_zone', lambda x: ZONE_MAPPING.get(x.lower(), x)),
        '4': ('arrival_zone', lambda x: [ZONE_MAPPING.get(z.lower(), z) for z in x.split('-')]),
        '5': ('rate_value', lambda x: re.sub(TO_REMOVE, '', x).strip()),
        '6': ('charterer', lambda x: None if may_strip(x) == 'CNR' else x),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel(raw_vessel):
    """Normalize vessel, handle status info.

    It may contains status info, in brackets.

    Examples:
        >>> normalize_vessel('Alterego 1 (rplc)')
        ({'name': 'Alterego 1'}, 'Fully Fixed')
        >>> normalize_vessel('Alterego')
        ({'name': 'Alterego'}, None)
        >>> normalize_vessel('Oceanis o/o')
        ({'name': 'Oceanis'}, None)

    Args:
        raw_vessel (str):

    Returns:
        Dict[str, str]:

    """
    if 'TBN' in raw_vessel.split():
        return None, None

    # remove unwanted string
    raw_vessel = re.sub(TO_REMOVE, '', raw_vessel).strip()
    # check if contains status
    _match = re.match(VESSEL_STATUS_PATTERN, raw_vessel)
    if not _match:
        logger.warning(f'Vessel {raw_vessel} doesn\'t match current pattern to extract status.')

    vessel, status = _match.groups()
    if status:
        status = STATUS_MAPPING.get(status.replace('(', '').replace(')', '').strip())

    return {'name': vessel.strip()}, status


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Date format:
    1. normal case: 10/10, 11-13/10
    2. rollover case: 27-10/05-11
    3. vague case: Ely/11
    4. no date case: DNR

    Examples:
        >>> normalize_lay_can('10/10', '25 Sep 2018')
        ('2018-10-10T00:00:00', '2018-10-10T00:00:00')
        >>> normalize_lay_can('11-13/10', '25 Sep 2018')
        ('2018-10-11T00:00:00', '2018-10-13T00:00:00')
        >>> normalize_lay_can('30/12', '1 Jan 2019')
        ('2018-12-30T00:00:00', '2018-12-30T00:00:00')
        >>> normalize_lay_can('1-2/1', '30 Dec 2018')
        ('2019-01-01T00:00:00', '2019-01-02T00:00:00')
        >>> normalize_lay_can('30-10/02-11', '1 Oct 2018')
        ('2018-10-30T00:00:00', '2018-11-02T00:00:00')
        >>> normalize_lay_can('28-12/02-01', '1 Jan 2019')
        ('2018-12-28T00:00:00', '2019-01-02T00:00:00')
        >>> normalize_lay_can('Ely/11', '15 Oct 2018')
        ('2018-11-01T00:00:00', '2018-11-07T00:00:00')
        >>> normalize_lay_can('End/1', '28 Dec 2018')
        ('2019-01-24T00:00:00', '2019-01-31T00:00:00')
        >>> normalize_lay_can('DNR', '23 Oct 2018')
        (None, None)

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]: (lay can start, lay can end)

    """
    # no date case
    if raw_lay_can == 'DNR':
        return None, None

    # month rollover case
    _match_rollover = re.match(DATE_PATTERN_ROLLOVER, raw_lay_can)
    if _match_rollover:
        start_day, start_month, end_day, end_month = _match_rollover.groups()
        year = _get_lay_can_start_year(reported, start_month)
        return _build_lay_can_date(start_day, start_month, end_day, end_month, year)

    # normal case
    _match_normal = re.match(DATE_PATTERN_NORMAL, raw_lay_can)
    if _match_normal:
        start_day, end_day, month = _match_normal.groups()
        year = _get_lay_can_start_year(reported, month)
        return _build_lay_can_date(start_day, month, end_day, month, year)

    # vague case
    _match_vague = re.match(DATE_PATTERN_VAGUE, raw_lay_can.lower())
    if _match_vague:
        _vague_day, month = _match_vague.groups()
        year = _get_lay_can_start_year(reported, month)
        start_day, end_day = VAGUE_DAY_MAPPING.get(_vague_day, (None, None))
        if _vague_day == 'end':
            end_day = get_last_day_of_current_month(month, year, '%m', '%Y')
            start_day = end_day - 7
        return _build_lay_can_date(start_day, month, end_day, month, year)

    # other undiscovered case
    logger.exception(f'Unknown lay can date pattern: {raw_lay_can}')
    return None, None


def _get_lay_can_start_year(reported, lay_can_start_month):
    """Get lay can start year with reference of its month and reported date.

    Examples:
        >>> _get_lay_can_start_year('1 Jan 2019', '12')
        2018
        >>> _get_lay_can_start_year('31 Dec 2018', '1')
        2019
        >>> _get_lay_can_start_year('1 Oct 2018', 10)
        2018

    Args:
        reported (str):
        lay_can_start_month (str): numerical month

    Returns:
        int:

    """
    year = parse_date(reported).year

    if lay_can_start_month == '12' and 'Jan' in reported:
        year -= 1
    if lay_can_start_month == '1' or lay_can_start_month == '01' and 'Dec' in reported:
        year += 1

    return year


def _build_lay_can_date(start_day, start_month, end_day, end_month, year):
    """Build and validate lay can date.

    Examples:
        >>> _build_lay_can_date(27, 10, 5, 10, 2018)
        ('2018-10-27T00:00:00', '2018-11-05T00:00:00')
        >>> _build_lay_can_date(27, 10, 5, 11, 2018)
        ('2018-10-27T00:00:00', '2018-11-05T00:00:00')
        >>> _build_lay_can_date(27, 12, 2, 1, 2018)
        ('2018-12-27T00:00:00', '2019-01-02T00:00:00')

    Args:
        start_day (str | int):
        start_month (str | int):
        end_day (str | int):
        end_month (str | int):
        year (str | int):

    Returns:
        Tuple[str, str]: (lay can start, lay can end)

    """
    end_day = end_day if end_day else start_day
    end_month = end_month if end_month else start_month

    start = parse_date(f'{start_day} {start_month} {year}', dayfirst=True)
    end = parse_date(f'{end_day} {end_month} {year}', dayfirst=True)

    # probably a typo, do month rollover
    # start: 27-10-2018, end: 05-10-2018
    if timedelta(days=0) < start - end <= timedelta(days=180):
        end = end + relativedelta(months=1)

    # year rollover for lay can end date
    # start: 30-12-2018, end: 02-01-2018
    elif start - end > timedelta(days=180):
        end = end + relativedelta(years=1)

    return start.isoformat(), end.isoformat()
