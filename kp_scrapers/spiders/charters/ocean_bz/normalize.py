import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


STATUS_MAPPING = {'F': SpotCharterStatus.failed}


@validate_item(SpotCharter, normalize=True, strict=True)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        SpotCharter | None

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    if not item['vessel']['name']:
        return

    item['charterer'], item['status'] = normalize_charter_status(item.pop('charterer_status', None))

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    # build cargo sub-model, product might be empty
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    return item


def field_mapping():
    return {
        'ship': ('vessel', lambda x: {'name': x if x and 'TBN' not in x else None}),
        'load': ('departure_zone', None),
        'dis': ('arrival_zone', lambda x: normalize_arrival_zone(x)),
        'qty(kt)': ('cargo_volume', None),
        'cargo': ('cargo_product', None),
        'laydays': ('lay_can', None),
        'frt': ('rate_value', None),
        'chartr': ('charterer_status', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: normalize_reported_date(x)),
    }


def normalize_charter_status(raw_charter_status):
    """Extract status from charterer

    Examples:
        >>> normalize_charter_status('BP(F)')
        ('BP', 'Failed')
        >>> normalize_charter_status('BP-')
        ('BP-', None)
        >>> normalize_charter_status('')
        (None, None)


    Args:
        raw_charter_status (str):

    Returns:
        Tuple[str, str]:

    """
    if raw_charter_status:
        _match = re.match(r'(.*)\((.*)\)', raw_charter_status)
        if _match:
            return _match.group(1), STATUS_MAPPING.get(_match.group(2), None)

        return raw_charter_status, None

    return None, None


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can start date with the year of reported date as reference.

    Lay can start date format:
        1. 21/SEP, 21/SEPT (normal case)
        2. END/AUG, ELY/SEP (vague case)
        3. --/SEP, DNR, PPT (invalid case)
        4. 07-MAR-20 (normal case with year)

    Examples:
        >>> normalize_lay_can('Aug 9', '27 Aug 2018')
        ('2018-08-09T00:00:00', '2018-08-09T00:00:00')
        >>> normalize_lay_can('07-MAR-20', '27 Aug 2018')
        ('2020-03-07T00:00:00', '2020-03-07T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        str:

    """
    _normal_match = re.match(r'([A-z]+)\s(\d+)', raw_lay_can)
    if _normal_match:
        day, month = _normal_match.groups()
        year = _get_lay_can_year(month, reported)
        lay_can_date = to_isoformat(f'{day} {month} {year}', dayfirst=True)
        return lay_can_date, lay_can_date

    if len(raw_lay_can.split('-')) == 3:
        day, month, year = raw_lay_can.split('-')
        lay_can_date = to_isoformat(f'{day} {month} 20{year}', dayfirst=True)
        return lay_can_date, lay_can_date

    logger.error(f'Unknown lay can date format: {raw_lay_can}')
    return None, None


def _get_lay_can_year(month, reported):
    """Get lay can month with reference of reported year.

    Args:
        month (str):
        reported (str):

    Returns:

    """
    year = parse_date(reported).year
    month = month.upper()
    if 'DEC' in month and 'Jan' in reported:
        year -= 1
    # Year shift Jan case
    if 'JAN' in month and 'Dec' in reported:
        year += 1

    return year


def normalize_reported_date(raw_reported_date):
    """Filter charterer and remove irrelevant letters.

    Examples:
        >>> normalize_reported_date('OCEANBZ MARKET REPORTS 5 DECEMBER 2019')
        '05 Dec 2019'

    Args:
        raw_reported_date (str):

    Returns:
        str:

    """
    _match = re.match(r'.*\s(\d{1,2}\s[A-z]+\s\d{2,4})', raw_reported_date)
    if _match:
        return parse_date(_match.group(1), dayfirst=True).strftime('%d %b %Y')

    return dt.datetime.now().strftime('%d %b %Y')


def normalize_arrival_zone(raw_arrival_zone):
    """Filter charterer and remove irrelevant letters.

    Examples:
        >>> normalize_arrival_zone('Sing OP China')
        ['Sing', 'China']
        >>> normalize_arrival_zone('Sing/China')
        ['Sing', 'China']
        >>> normalize_arrival_zone('')

    Args:
        raw_arrival_zone (str):

    Returns:
        List[str]:

    """
    if raw_arrival_zone:
        raw_arrival_zone = raw_arrival_zone.replace(' OP ', '/')
        return [may_strip(x) for x in raw_arrival_zone.split('/')]
    return None
