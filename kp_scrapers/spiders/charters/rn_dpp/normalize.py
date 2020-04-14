import logging
import re

from kp_scrapers.lib.date import get_date_range, to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


STATUS_MAPPING = {
    'FLD': SpotCharterStatus.failed,
    'SUBS': SpotCharterStatus.on_subs,
    'FXD': SpotCharterStatus.fully_fixed,
}

ZONE_SUBSTR_BLACKLIST = ['STS', '+1', '+ 1']

ZONE_MAPPING = {'CPC': 'NOVOROSSIYSK', 'EAST': 'FAR EAST'}

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
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]): value may be datetime

    Yields:
        Dict(str, Any):

    """
    item = map_keys(raw_item, spot_charter_mapping())

    # discard irrelevant vessels
    if not item['vessel']['name']:
        return

    # normalize laycan period
    item['lay_can_start'], item['lay_can_end'] = normalize_laycan(
        item.pop('laycan'), item['reported_date']
    )

    item['cargo_unit'] = Unit.kilotons if len(item['cargo_volume']) <= 3 else Unit.tons

    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': item.pop('cargo_unit', None),
    }

    # normalize rate values
    item['rate_value'] = ' '.join([item.pop('rate'), item.pop('value')])

    return item


def spot_charter_mapping():
    return {
        '0': ('vessel', lambda x: {'name': normalize_vessel_name(x)}),
        '1': ('charterer', None),
        '2': ('cargo_volume', lambda x: x.replace(',', '')),
        '3': ('cargo_product', None),
        '4': ('departure_zone', normalize_departure_zone),
        '5': ('arrival_zone', normalize_arrival_zone),
        '6': ('laycan', None),
        '7': ('rate', None),
        '8': ('value', None),
        '9': ('status', lambda x: STATUS_MAPPING.get(x, None)),
        '10': ('status', lambda x: STATUS_MAPPING.get(x, None)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel_name(raw_vessel_name):
    """Normalize vessel name.

    Examples:
        >>> normalize_vessel_name('NECTAR O/O')
        'NECTAR'
        >>> normalize_vessel_name('NECTAR')
        'NECTAR'
        >>> normalize_vessel_name('NECTAR TBN')

    Args:
        raw_vessel_name (str):

    Returns:
        str:

    """
    raw_vessel_name = may_strip(raw_vessel_name).upper()

    if 'TBN' in raw_vessel_name.split():
        return None

    return re.sub(r'O/O|P/C|S/S|C/C|N/B', '', raw_vessel_name).strip()


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


def normalize_arrival_zone(raw_zone):
    """Normalize arrival zones.

    We don't care about persian gulf macro zones when it is an arrival zone.

    Args:
        raw_zone (str):

    Returns:
        List[str]:

    Examples:
        >>> normalize_arrival_zone('BASRAH-KAA')
        ['BASRAH', 'KAA']
        >>> normalize_arrival_zone('BASRAH')
        ['BASRAH']
        >>> normalize_arrival_zone('BASRAH+1')
        ['BASRAH']
        >>> normalize_arrival_zone('BUKIT TUA+1')
        ['BUKIT TUA']
        >>> normalize_arrival_zone('STS YOSU')
        ['YOSU']
    """
    arrival_zone = []
    for single_zone in raw_zone.split('-'):
        zone = may_strip(may_remove_substring(single_zone, ZONE_SUBSTR_BLACKLIST))
        for alias in ZONE_MAPPING:
            if alias in zone.lower():
                arrival_zone.append(ZONE_MAPPING[alias])
                break

        arrival_zone.append(zone)

    return arrival_zone if arrival_zone else [raw_zone]


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


def normalize_laycan(raw_laycan, rpt_date):
    """Normalize raw laycan date.

    Raw laycan inputs can be of the following formats:
        - range: '15-17/6'
        - range with month rollover: '31-1/6'
        - single day: '14/3'
        - english month: '14/SEP'
        - vague date: 'MID/SEP'
        - vague month: 'OCT-DEC'

    Args:
        raw_laycan (str):
        year (str | int): string numeric of report's year

    Returns:
        Tuple[str]: tuple of laycan period

    Examples:
        >>> normalize_laycan('26-28/3', '2018')
        ('2018-03-26T00:00:00', '2018-03-28T00:00:00')
        >>> normalize_laycan('31-3/6', '2018')
        ('2018-05-31T00:00:00', '2018-06-03T00:00:00')
        >>> normalize_laycan('30-2/1', '2018')
        ('2017-12-30T00:00:00', '2018-01-02T00:00:00')
        >>> normalize_laycan('12/3', '2018')
        ('2018-03-12T00:00:00', '2018-03-12T00:00:00')
        >>> normalize_laycan('14/SEP', '2018')
        ('2018-09-14T00:00:00', '2018-09-14T00:00:00')
        >>> normalize_laycan('MID/SEP', '2018')
        ('2018-09-14T00:00:00', '2018-09-21T00:00:00')
        >>> normalize_laycan('26-Feb-2019', '2019')
        ('2019-02-26T00:00:00', '2019-02-26T00:00:00')
        >>> normalize_laycan('OCT-DEC', '2018')
        (None, None)
    """
    # for cpp attachment, dates are in dd-mmm-yyyy format
    if len(raw_laycan.split('-')) == 3:
        return to_isoformat(raw_laycan, dayfirst=True), to_isoformat(raw_laycan, dayfirst=True)

    return get_date_range(raw_laycan, '/', '-', rpt_date)
