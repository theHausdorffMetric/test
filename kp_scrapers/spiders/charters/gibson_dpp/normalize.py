import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import get_last_day_of_current_month, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MISSING_ROWS = []
SPELLING_CHECK = [
    ('aor', 'apr'),
]
STATUS_MAPPING = {
    'SUBS': SpotCharterStatus.on_subs,
    'WORKING': SpotCharterStatus.on_subs,
    'FXD': SpotCharterStatus.fully_fixed,
    'CONF': SpotCharterStatus.fully_fixed,
    'FLD': SpotCharterStatus.failed,
}
VAGUE_DAY_MAPPING = {'ELY': (1, 7), 'EARLY': (1, 7), 'MID': (14, 21)}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # discard TBN vessels
    if any(sub in item['vessel']['name'] for sub in ('TBN', 'VESSEL')):
        return

    # discard failed charters
    if item['status'] == SpotCharterStatus.failed:
        return

    if not item['lay_can']:
        return

    # build cargo item
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        # laycan cannot be popped here to have it shown in missing rows list
        item['lay_can'],
        item['reported_date'],
        item,
    )

    item.pop('lay_can')
    return item


def field_mapping():
    return {
        'vessel': ('vessel', lambda x: {'name': may_strip(x)}),
        'size': ('cargo_volume', None),
        'cargo': ('cargo_product', None),
        'layday': ('lay_can', None),
        'load': ('departure_zone', None),
        'discharge': ('arrival_zone', lambda x: normalize_voyage(x)),
        'freight': ('rate_value', None),
        'charterer': ('charterer', None),
        'status': ('status', lambda x: STATUS_MAPPING.get(x, None)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported, raw_string_item):
    """Normalize lay can date with reported year as reference.

    In this report, the lay can date can vary differently, however, we only extract below formats:
    - 15-16JUNE (format 0)
    - 2-3 NOV (format 1)
    - 30-02 JAN (format 1 with rollover)
    - 13-14TH JUL (format 1 with additional qualifier)
    - 22-24/01 (format 2)
    - 12th OCT (format 3)
    - 7-Dec (format 4)
    - End JAN (format 5)
    - DNR
    - MID MONTH
    - PPT ON

    Examples:
        >>> normalize_lay_can('2-3 NOV', '22 Sep 2018', 'vessel...')
        ('2018-11-02T00:00:00', '2018-11-03T00:00:00')
        >>> normalize_lay_can('13-14TH JUL', '10 Jul 2019', 'vessel...')
        ('2019-07-13T00:00:00', '2019-07-14T00:00:00')
        >>> normalize_lay_can('12th OCT', '22 Sep 2018', 'vessel...')
        ('2018-10-12T00:00:00', '2018-10-12T00:00:00')
        >>> normalize_lay_can('1-2/01', '28 Dec 2018', 'vessel...')
        ('2018-01-01T00:00:00', '2018-01-02T00:00:00')
        >>> normalize_lay_can('END JAN', '01 Jan 2018', 'vessel...')
        ('2018-01-24T00:00:00', '2018-01-31T00:00:00')
        >>> normalize_lay_can('', '28 Dec 2018', 'vessel...')
        (None, None)
        >>> normalize_lay_can('DNR', '28 Dec 2018', 'vessel...')
        (None, None)
        >>> normalize_lay_can('MID MONTH', '28 Dec 2018', 'vessel...')
        (None, None)
        >>> normalize_lay_can('PPT ON', '28 Dec 2018', 'vessel...')
        (None, None)

    Args:
        raw_lay_can (str)
        reported (str)

    Returns:
        Tuple[str, str]

    """
    for wrong_spelling in SPELLING_CHECK:
        raw_lay_can = re.sub(wrong_spelling[0], wrong_spelling[1], raw_lay_can)
    _match_0 = re.match(r'^(\d+).(\d+)([A-z]+)$', raw_lay_can)
    _match_1 = re.match(r'^(\d{1,2})(?:[A-z]{2})?.(\d{1,2})(?:[A-z]{2})?.([A-z0-9]+)$', raw_lay_can)
    _match_2 = re.match(r'^(\d{1,2})(?:[A-z]+)?(?:[- ])([A-z]+)$', raw_lay_can)
    _match_3 = re.match(r'^(ELY|EARLY|MID|END)\s([A-z]+$)', raw_lay_can)

    # format 0
    if _match_0:
        start_day, end_day, month = _match_0.groups()
        year = _get_year(month, reported)
        month = parse_date(f'01 {month} {year}', dayfirst=True).month

        # in the event the rollover has february dates (30-01 FEB), first result would
        # be 30th february which does not exist, hence try and except is needed
        try:
            start = parse_date(f'{start_day} {month} {year}', dayfirst=True)
        except Exception:
            start = parse_date(f'{start_day} {month-1} {year}', dayfirst=True)

        try:
            end = parse_date(f'{end_day} {month} {year}', dayfirst=True)
        except Exception:
            MISSING_ROWS.append(str(raw_string_item))
            return
        # if it's rollover case
        if start > end:
            start = start + relativedelta(months=-1)

        return start.isoformat(), end.isoformat()

    # format 1 and 2
    if _match_1:
        start_day, end_day, month = _match_1.groups()
        year = _get_year(month, reported)
        month = parse_date(f'01 {month} {year}', dayfirst=True).month

        # in the event the rollover has february dates (30-01 FEB), first result would
        # be 30th february which does not exist, hence try and except is needed
        try:
            start = parse_date(f'{start_day} {month} {year}', dayfirst=True)
        except Exception:
            start = parse_date(f'{start_day} {month-1} {year}', dayfirst=True)

        try:
            end = parse_date(f'{end_day} {month} {year}', dayfirst=True)
        except Exception:
            MISSING_ROWS.append(str(raw_string_item))
            return

        # if it's rollover case
        if start > end:
            start = start + relativedelta(months=-1)

        return start.isoformat(), end.isoformat()

    # format 3 and 4
    if _match_2:
        start_day, month = _match_2.groups()
        year = _get_year(month, reported)
        start = to_isoformat(f'{start_day} {month} {year}', dayfirst=True)
        end = start

        return start, end

    # format 5
    if _match_3 and 'MONTH' not in raw_lay_can:
        _vague_day, month = _match_3.groups()
        year = _get_year(month, reported)
        start_day, end_day = VAGUE_DAY_MAPPING.get(_vague_day, (None, None))
        if _vague_day == 'END':
            end_day = get_last_day_of_current_month(month, year, '%b', '%Y')
            start_day = end_day - 7
        start = to_isoformat(f'{start_day} {month} {year}', dayfirst=True)
        end = to_isoformat(f'{end_day} {month} {year}', dayfirst=True) if end_day else start

        return start, end

    # other undiscovered case
    MISSING_ROWS.append(str(raw_string_item))
    logger.error('Unknown lay can date pattern: %s', raw_lay_can)

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


def normalize_voyage(raw_voyage):
    """Normalize departure zone and arrival zone from voyage field.

    Examples:
        >>> normalize_voyage('MED-MALTA')
        ['MED', 'MALTA']
        >>> normalize_voyage('UKC')
        ['UKC']

    Args:
        raw_voyage (str):

    Returns:
        List[str]: arrival zones

    """
    return raw_voyage.split('-')
