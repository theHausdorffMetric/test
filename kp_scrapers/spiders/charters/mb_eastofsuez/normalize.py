import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


ZONE_MAPPING = {'OPTS': ''}


ZONE_SUBSTR_BLACKLIST = ['STS', '+1', '+ 1', '(L)']


PERSIAN_GULF = 'Persian Gulf'
PERSIAN_GULF_ZONES = [
    'jebel',
    'dhana',
    'mubarraz',
    'zirku',
    'halul',
    'fateh',
    'sirri',
    'ras laffan',
    'al shaheen',
    'al rayyan',
    'ryrus',
    'lavan',
    'ras tanura',
    'das is',
    'juaymah',
    'r.tan',
    'basrah',
    'khafiji',
    'mina saud',
    'barhegan',
    'assaluyeh',
    'lavan',
    'fujairah',
    'muscat',
    'cyrus',
    'barhegan',
    'mina al fahal',
    'bashayer',
    'muda',
    'ruwais',
    'meg',
    'fuj',
    'maa',
    'maf',
    'bot',
]


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # build cargo item
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        'laycan': ('lay_can', None),
        'charterer': ('charterer', lambda x: x.replace('?', '')),
        'volume': ('cargo_volume', None),
        'cargo': ('cargo_product', None),
        'departure_zone': ('departure_zone', lambda x: normalize_departure_zone(x)),
        'arrival_zone': ('arrival_zone', lambda x: normalize_voyage(x)),
        'vessel': ('vessel', lambda x: {'name': normalize_vessel(x)}),
        'rate': ('rate_value', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    In this report, the lay can date can vary differently, however, we only extract below formats:
    - 31-02/02 format 1 with rollover
    - 02-04/02 format 1
    - 02/02 format 2

    Rollover dates have not been identified yet

    Examples:
        >>> normalize_lay_can('31-02/02', '22 JAN 2019')
        ('2019-01-31T00:00:00', '2019-02-02T00:00:00')
        >>> normalize_lay_can('02-04/02', '22 JAN 2019')
        ('2019-02-02T00:00:00', '2019-02-04T00:00:00')
        >>> normalize_lay_can('02/02', '22 JAN 2019')
        ('2019-02-02T00:00:00', '2019-02-02T00:00:00')

    Args:
        raw_lay_can (str)
        reported (str)

    Returns:
        Tuple[str, str]

    """
    _match_1 = re.match(r'^(\d{1,2})\-(\d{1,2})\/(\d{1,2})$', raw_lay_can)
    _match_2 = re.match(r'^(\d{1,2})\/(\d{1,2})$', raw_lay_can)
    # format 1
    if _match_1:
        start_day, end_day, month = _match_1.groups()
        year = _get_year(month, reported)

        # handle typo errors in dates
        start_day = end_day if int(start_day) > 31 else start_day

        # to accomodate end dates for february
        try:
            start = to_isoformat(f'{start_day} {month} {year}', dayfirst=True)
        except Exception:
            start = to_isoformat(f'{start_day} {int(month) - 1} {year}', dayfirst=True)

        end = to_isoformat(f'{end_day} {month} {year}', dayfirst=True)

        return start, end

    # format 2
    if _match_2:
        month, start_day = _match_2.groups()
        year = _get_year(month, reported)
        start = to_isoformat(f'{start_day} {month} {year}', dayfirst=True)

        return start, start

    # other undiscovered case
    logger.exception(f'Unknown lay can date pattern: {raw_lay_can}')

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
    return [x for x in raw_voyage.split('-') if ZONE_MAPPING.get(x, x)]


def normalize_vessel(raw_vessel):
    """Normalize vessel and remove unwanted strings.

    If vessel name contains `TBN`, it's irrelevant vessel, discard it.
    Remove irrelevant extra words.

    Examples:
        >>> normalize_vessel('NORD - ')
        'NORD'

    Args:
        raw_vessel (str):

    Returns:
        str:

    """
    if 'TBN' in raw_vessel.upper().split():
        logger.info(f'Discard irrelevant vessel: {raw_vessel}')
        return

    # remove unwanted string
    vessel = may_strip(re.sub(r'\-', '', raw_vessel))

    return vessel


def normalize_departure_zone(raw_zone):
    """Normalize departure zones.

    Args:
        raw_zone (str):

    Returns:
        str:

    Examples:
        >>> normalize_departure_zone('BASRAH-KAA')
        'Persian Gulf'
        >>> normalize_departure_zone('BASRAH')
        'BASRAH'
        >>> normalize_departure_zone('BASRAH+1')
        'Persian Gulf'
        >>> normalize_departure_zone('BUKIT TUA+1')
        'BUKIT TUA'
        >>> normalize_departure_zone('STS YOSU')
        'YOSU'
    """
    if is_persian_gulf_zone(raw_zone):
        return PERSIAN_GULF

    zone = may_strip(may_remove_substring(raw_zone, ZONE_SUBSTR_BLACKLIST))
    for alias in ZONE_MAPPING:
        if alias in zone.lower():
            return ZONE_MAPPING[alias]

    return zone


def is_persian_gulf_zone(raw_zone):
    """Check if a raw zone is within Persian Gulf and contains multiple zones.

    Examples:
        >>> is_persian_gulf_zone('BASRAH-KAA')
        True
        >>> is_persian_gulf_zone('BASRAH')
        False
        >>> is_persian_gulf_zone('BASRAH+1')
        True
        >>> is_persian_gulf_zone('BUKIT TUA+1')
        False
        >>> is_persian_gulf_zone('STS YOSU')
        False
    """
    return any(z in raw_zone.lower() for z in PERSIAN_GULF_ZONES) and (
        '+' in raw_zone or '-' in raw_zone
    )
