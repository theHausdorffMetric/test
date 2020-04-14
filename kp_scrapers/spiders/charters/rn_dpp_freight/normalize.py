import datetime as dt
import logging

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

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
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, charters_mapping())
    # discard unknown vessels
    if not item['vessel']:
        return

    # discard failed charters
    if item.pop('is_failed', False):
        return

    # properly enrich laycan dates with year and month
    item['lay_can_start'], item['lay_can_end'] = normalize_laycan(
        item.pop('laycan'), year=parse_date(item['reported_date'], dayfirst=True).year
    )

    # build cargo sub-model
    item['cargo'] = normalize_cargo(item.pop('volume'), item.pop('product'))

    return item


def charters_mapping():
    return {
        'Charterers': ('charterer', None),
        'Comments': ('is_failed', lambda x: 'FLD' in x),
        'Disch': ('arrival_zone', normalize_arrival_zone),
        'Freight': ('rate_value', None),
        'Grd': ('product', None),
        'Layday': ('laycan', None),
        'Load': ('departure_zone', normalize_departure_zone),
        'provider_name': ('provider_name', None),
        'Qty (Kt)': ('volume', None),
        'reported_date': ('reported_date', None),
        'Vessel On subs/fxd': ('vessel', lambda x: {'name': may_strip(x)}),
    }


def normalize_cargo(volume, product):
    """Normalize cargo.

    Args:
        volume (str):
        product (str):

    Returns:
        Dict[str, str]:

    """
    return {'product': product, 'movement': 'load', 'volume': volume, 'volume_unit': Unit.kilotons}


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


def normalize_laycan(raw_laycan, year):
    """Normalize raw laycan date.

    Raw laycan inputs can be of the following formats:
        1) range: '15-17/6'
        2) range: '09/11/02'
        3) range with month rollover: '31-1/6'
        4) range with month rollover: '30/08-01/09'
        5) range with month rollover: '30-01/02-02'
        6) single day: '14/3'

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
        >>> normalize_laycan('09/11/02', '2018')
        ('2018-02-09T00:00:00', '2018-02-11T00:00:00')
        >>> normalize_laycan('30-2/1', '2018')
        ('2017-12-30T00:00:00', '2018-01-02T00:00:00')
        >>> normalize_laycan('30/8-01/09', '2018')
        ('2018-08-30T00:00:00', '2018-09-01T00:00:00')
        >>> normalize_laycan('30/12-02/01', '2018')
        ('2018-12-30T00:00:00', '2019-01-02T00:00:00')
        >>> normalize_laycan('30-01/02-02', '2019')
        ('2019-01-30T00:00:00', '2019-02-02T00:00:00')
        >>> normalize_laycan('12/3', '2018')
        ('2018-03-12T00:00:00', '2018-03-12T00:00:00')

    """
    if len(raw_laycan.split('/')) > 2:
        # check if date is of format #4, will process separately
        if '-' in raw_laycan.split('/')[1]:
            start, end = raw_laycan.split('-')
            lay_can_start = parse_date(f'{start}/{year}', dayfirst=True)
            lay_can_end = parse_date(f'{end}/{year}', dayfirst=True)
            # sanity check to get correct year
            if lay_can_start > lay_can_end:
                lay_can_end += relativedelta(years=1)

            return lay_can_start.isoformat(), lay_can_end.isoformat()
        # check if date is of format #2; can be pre-processed first
        else:
            start, _, end = raw_laycan.partition('/')
            raw_laycan = f'{start}-{end}'

    # check if date is of format #5; can be processed easily
    if len(raw_laycan.split('/')) == 2 and all('-' in each for each in raw_laycan.split('/')):
        start, end = raw_laycan.split('/')
        return (
            to_isoformat(f'{start}-{year}', dayfirst=True),
            to_isoformat(f'{end}-{year}', dayfirst=True),
        )

    # extract the month and date range first
    date_range, month = raw_laycan.split('/')

    # laycan dates may/may not be a range
    if len(date_range.split('-')) == 1:
        start = end = date_range
    elif len(date_range.split('-')) == 2:
        start, end = date_range.split('-')
    else:
        raise ValueError(f'Unknown raw laycan format: {raw_laycan}')

    # init laycan period
    # sometimes, we may be presented with dates like `31-3/6`, which contains a month rollover
    # hence, we need to use try/except
    lay_can_end = dt.datetime(year=int(year), month=int(month), day=int(end))
    try:
        lay_can_start = dt.datetime(year=int(year), month=int(month), day=int(start))
    except ValueError:
        lay_can_start = dt.datetime(year=int(year), month=int(month) - 1, day=int(start))
    if lay_can_start > lay_can_end:
        lay_can_start -= relativedelta(months=1)

    return lay_can_start.isoformat(), lay_can_end.isoformat()
