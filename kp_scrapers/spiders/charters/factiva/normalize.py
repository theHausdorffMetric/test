import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


STATUS_MAPPING = {
    'RPLC': SpotCharterStatus.fully_fixed,
    'RMRD': SpotCharterStatus.fully_fixed,
    'FLD': SpotCharterStatus.failed,
}

ZONE_MAPPING = {'AG': 'Persian Gulf', 'STS/MALTA': 'Malta Light'}

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

    # build cargo sub-model, product and volume field might not exist
    if item.get('product'):
        item['cargo'] = {
            'product': item.get('product'),
            'movement': 'load',
            'volume': item.get('volume'),
            'volume_unit': Unit.kilotons,
        }
    item.pop('product', '')
    item.pop('volume', '')

    item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage', ''))
    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can', ''), item['reported_date']
    )
    item['charterer'], item['status'] = normalize_charterer_status(item.pop('charterer_status', ''))

    return item


def field_mapping():
    return {
        '0': ('vessel', lambda x: {'name': x} if 'TBN' not in x.split() else None),
        '1': ('volume', None),  # optional
        '2': ('product', None),  # optional
        '3': ('voyage', None),
        '4': ('lay_can', None),
        '5': ('rate_value', None),
        '6': ('charterer_status', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can date pattern:
        1. Normal case: 04/11, 03-05/11
        2. Rollover case: 29-01/12 (Nov 29 to Dec 1)
        3. Weird cases: 15/16/02 (15 Feb to 16 Feb)

    Examples:
        >>> normalize_lay_can('04/11', '17 Oct 2018')
        ('2018-11-04T00:00:00', '2018-11-04T00:00:00')
        >>> normalize_lay_can('03-05/11', '17 Oct 2018')
        ('2018-11-03T00:00:00', '2018-11-05T00:00:00')
        >>> normalize_lay_can('2/1', '28 Dec 2018')
        ('2019-01-02T00:00:00', '2019-01-02T00:00:00')
        >>> normalize_lay_can('15/16/02', '17 Jan 2019')
        ('2019-02-15T00:00:00', '2019-02-16T00:00:00')
        >>> normalize_lay_can('', '28 Dec 2018')
        (None, None)
        >>> normalize_lay_can('DNR', '30 Oct 2018')
        (None, None)
        >>> normalize_lay_can('29-01/12', '9 Nov 2018')
        ('2018-11-29T00:00:00', '2018-12-01T00:00:00')
        >>> normalize_lay_can('31-01/02', '7 Jan 2019')
        ('2019-01-31T00:00:00', '2019-02-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    # pre-process weird cases first (scenario #3)
    if len(raw_lay_can.split('/')) == 3:
        start, _, end = raw_lay_can.partition('/')
        raw_lay_can = f'{start}-{end}'

    if not raw_lay_can or raw_lay_can == 'DNR':
        return None, None

    _match = re.match(r'(\d{1,2})-?(\d{1,2})?/(\d{1,2})', raw_lay_can)
    if not _match:
        logger.error(f'This might be new lay can pattern: {raw_lay_can}')
        return None, None

    start_day, end_day, month = _match.groups()
    year = parse_date(reported).year
    if month == '11' and 'Jan' in reported:
        year -= 1
    if month == '1' or month == '01' and 'Dec' in reported:
        year += 1

    # rollover formats include 31-01/02, spider will fail since 31/02 does not
    # exist, hence try and except method is used
    try:
        start = parse_date(f'{start_day} {month} {year}', dayfirst=True)
    except Exception:
        start = parse_date(f'{start_day} {int(month) - 1} {year}', dayfirst=True)

    end = parse_date(f'{end_day} {month} {year}', dayfirst=True) if end_day else start

    # month rollover case, month is for lay can end
    if start > end:
        start = start + relativedelta(months=-1)

    return start.isoformat(), end.isoformat()


def normalize_voyage(raw_voyage):
    """Normalize departure zone and arrival zone from voyage field.

    Examples:
        >>> normalize_voyage(' JOSE/SPORE-CHINA')
        ('JOSE', ['SPORE', 'CHINA'])
        >>> normalize_voyage(' BALTIC/UKC+SHORTS')
        ('BALTIC', ['UKC', 'SHORTS'])
        >>> normalize_voyage(' X UKC')
        ('X UKC', [''])
        >>> normalize_voyage('')
        ('', [''])
        >>> normalize_voyage(' AG/SPORE-CHINA')
        ('Persian Gulf', ['SPORE', 'CHINA'])
        >>> normalize_voyage('STS/MALTA/SINGAPORE')
        ('Malta Light', ['SINGAPORE'])

    Args:
        raw_voyage (str):

    Returns:
        Tuple[str, List[str]]: (departure zone, arrival zone)

    """
    for zone in ZONE_MAPPING:
        if zone in raw_voyage:
            raw_voyage = raw_voyage.replace(zone, ZONE_MAPPING[zone])

    departure, _, arrival = may_strip(raw_voyage).partition('/')
    arrivals = [x for x in arrival.replace('+', '-').split('-')]

    return departure, arrivals


def normalize_charterer_status(charterer_status):
    """Normalize charterer and status.

    Examples:
        >>> normalize_charterer_status('DAY HARVEST')
        ('DAY HARVEST', None)
        >>> normalize_charterer_status('SHELL - RMRD')
        ('SHELL', 'Fully Fixed')
        >>> normalize_charterer_status(None)
        (None, None)
        >>> normalize_charterer_status('')
        (None, None)

    Args:
        charterer_status (str):

    Returns:
        Tuple[str, str]: (charterer, status)

    """
    # since charterer and status are both optional
    if not charterer_status:
        return None, None

    charterer, _, status = charterer_status.partition('-')
    return charterer.strip(), STATUS_MAPPING.get(status.strip(), None)
