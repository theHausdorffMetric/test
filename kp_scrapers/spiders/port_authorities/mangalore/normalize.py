import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

PRODUCT_BLACKLIST = [
    'CONTAINER',
    'BALLAST',
    'BOXES',
    'DREDGER',
    'SPM OP.',
    'PORT OP.',
    'PASSENGER',
    'BUILD. MATRL',
    'BUNKERING',
    'RESEARCH',
]

MOVEMENT_MAPPING = {'D': 'discharge', 'L': 'load'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # build vessel sub model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_loa', None)}

    if 'NIL' in item['vessel']['name']:
        return

    if item['product'] in PRODUCT_BLACKLIST:
        return

    # get total volume, first table splits it into 2
    if item.get('volume_bal') or item.get('volume_total'):
        item['volume'] = str(int(item['volume_total']) + int(item['volume_bal']))

    # build cargo sub model
    item['cargoes'] = list(normalize_cargo(item.pop('product', None), item.pop('volume', None)))

    # build proper date
    if item.get('eta'):
        item['eta'] = normalize_date(item['eta'], item['reported_date'])
    if item.get('arrival'):
        item['arrival'] = normalize_date(item['arrival'], item['reported_date'])
    if item.get('berthed'):
        item['berthed'] = normalize_date(item['berthed'], item['reported_date'])
    if item.get('departure'):
        item['departure'] = normalize_date(item['departure'], item['reported_date'])

    # if there's eta time
    if item.get('eta_time'):
        eta_time = item.pop('eta_time')
        hour, minute = normalize_time(eta_time)
        item['eta'] = (
            parse_date(item['eta']).replace(hour=int(hour), minute=int(minute)).isoformat()
        )

    for x in ['volume_total', 'volume_bal', 'eta_time']:
        item.pop(x, None)

    if not (item.get('departure') or item.get('berthed') or item.get('arrival') or item.get('eta')):
        return

    return item


def portcall_mapping():
    return {
        'B.NO.': ('berth', None),
        'SL.NO': (ignore_key('irrelevant')),
        'NAME OF VESSEL': ('vessel_name', normalize_vessel_name),
        'NAME OF THE VESSEL': ('vessel_name', normalize_vessel_name),
        'IND/': (ignore_key('irrelevant')),
        'LOA': ('vessel_loa', lambda x: try_apply(x, float, int)),
        'ETA': ('eta', None),
        'E.RT.TIME': ('eta_time', None),
        'DOA': ('eta', None),
        'RT.TIME': ('eta_time', None),
        'NOD(WAITING)': (ignore_key('irrelevant')),
        'ARRIVAL': ('arrival', None),
        'BERTHING': ('berthed', None),
        'CARGO': ('product', None),
        'AGENT': ('shipping_agent', None),
        'RECEIVER': (ignore_key('cannot be used for now')),
        'QTY.': ('volume', normalize_volume),
        'QTY': ('volume', normalize_volume),
        'DAY': (ignore_key('irrelevant')),
        'TOTAL': ('volume_total', normalize_volume),
        'BALANCE': ('volume_bal', normalize_volume),
        'ETD': ('departure', None),
        'REASONS FOR WAITING': (ignore_key('irrelevant')),
        'B.PREFER': (ignore_key('irrelevant')),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_date(raw_date, raw_reported_date):
    """Normalize dates

    Examples:
        >>> normalize_date('30/04', '2019-04-23T00:00:00')
        '2019-04-30T00:00:00'
        >>> normalize_date('30/12', '2019-12-30T00:00:00')
        '2019-12-30T00:00:00'
        >>> normalize_date('30/12', '2020-01-01T00:00:00')
        '2019-12-30T00:00:00'
        >>> normalize_date('30/12', '2020-01-01T00:00:00')
        '2019-12-30T00:00:00'

    Args:
        raw_date (str):

    Returns:
        str: date in ISO 8601 format

    """
    _match_date = re.match(r'(\d{1,2})\/(\d{1,2}).*', raw_date)
    if _match_date:
        _day, _month = _match_date.groups()
    _year = parse_date(raw_reported_date).year
    formatted_date = parse_date(f'{_day} {_month} {_year}', dayfirst=True)

    # to accomodate end of year parsing, prevent dates too old or far into
    # the future. 100 days was chosen as a gauge
    if (formatted_date - parse_date(raw_reported_date)).days < -100:
        formatted_date = dt.datetime(year=int(_year) + 1, month=int(_month), day=int(_day))
        return formatted_date.isoformat()

    if (formatted_date - parse_date(raw_reported_date)).days > 100:
        formatted_date = dt.datetime(year=int(_year) - 1, month=int(_month), day=int(_day))

        return formatted_date.isoformat()

    return formatted_date.isoformat()


def normalize_vessel_name(raw_name):
    """Normalize vessel name.

    Examples:
        >>> normalize_vessel_name('DAWN MADURAI (8.0)')
        'DAWN MADURAI'
        >>> normalize_vessel_name('VISHVA EKTA(13.0) P.MHC')
        'VISHVA EKTA'
        >>> normalize_vessel_name('SEA EAGLE III /')
        'SEA EAGLE III'

    Args:
        raw_name:

    Returns:
        str:
    """
    _match = re.match(r'([\w\s]*)([\W\d]*)?', raw_name)
    if _match:
        vessel_name, _ = _match.groups()
        return vessel_name.strip()


def normalize_cargo(raw_movement_cargo, raw_volume):
    """Normalize cargo with given information.

    Args:
        raw_movement_cargo: product(movement)
        raw_volume:

    Yields:
        Dict:

    """
    _match = re.match(r'([^\(\)]+)\(?(L|D)?\)?', raw_movement_cargo)

    if _match and raw_volume:
        product_str, movement = _match.groups()

        product_list = product_str.split('/')
        volume_list = raw_volume.split('+')

        if len(volume_list) == len(product_list):
            cargo_volume_list = list(zip(product_list, volume_list))
            for item in cargo_volume_list:
                yield {
                    'product': item[0],
                    'movement': MOVEMENT_MAPPING.get(movement, movement),
                    'volume': item[1],
                    'volume_unit': Unit.tons,
                }

        else:
            fin_volume = int(int(raw_volume) / len(product_list))
            for item in product_list:
                yield {
                    'product': item,
                    'movement': MOVEMENT_MAPPING.get(movement, movement),
                    'volume': fin_volume,
                    'volume_unit': Unit.tons,
                }


def normalize_volume(raw_vol):
    """Normalize volume.

    Examples:
        >>> normalize_volume(None)
        '0'
        >>> normalize_volume('197')
        '197'
        >>> normalize_volume('197(187)')
        '197'

    Args:
        raw_vol(str):

    Returns:
        int:
    """
    if raw_vol:
        _vol_match = re.match(r'(\d+).*', raw_vol)
        if _vol_match:
            return _vol_match.group(1)

    return '0'


def normalize_time(raw_time):
    """Normalize volume.

    Examples:
        >>> normalize_time('AM')
        ('0', '0')
        >>> normalize_time('09:09')
        ('09', '09')
        >>> normalize_time('24:00')
        ('0', '0')

    Args:
        raw_time(str):

    Returns:
        str:
    """
    _match_time = re.match(r'(\d+)\:(\d+)', raw_time)

    if _match_time:
        _hour, _minute = _match_time.groups()
        if int(_hour) in range(0, 24) and int(_minute) in range(0, 60):
            return _hour, _minute

    return '0', '0'
