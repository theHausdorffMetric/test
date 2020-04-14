import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


# derive appropriate date var with respective vars
# accepted var name, report var date, report var time
VAR_MAPPING = [
    ['arrival', 'arrival_date', 'arrival_time'],
    ['eta', 'eta_date', 'eta_time'],
    ['berthed', 'berthed_date', 'berthed_time'],
]


CARGO_BLACKLIST = [
    'container',
    'fish',
    'store',
    'ballast',
    'nil',
    'box',
    'vehicle',
    'supply',
    'vessel',
]


MOVEMENT_MAPPING = {'LOAD': 'load', 'DISCHARGE': 'discharge'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # file contains date and time in 2 different columns
    # each tab might not provide each date type
    # this checks if the date type exists and map it to
    # what we have in the portcall model
    for _date_var in VAR_MAPPING:
        input_time = _date_var[2] if _date_var[2] else None

        if _date_var[1] in item:
            item[_date_var[0]] = normalize_date(item[_date_var[1]], input_time)

    item['vessel'] = {
        'name': may_strip(item.pop('vessel_name')),
        'flag_name': may_strip(item.pop('vessel_flag')),
        'gross_tonnage': may_strip(item.pop('vessel_gt')),
        'length': may_strip(item.pop('vessel_loa')),
    }

    # some tabs do not provide movement information
    # checks if it exists else None
    item['cargo_movement'] = item['cargo_movement'] if 'cargo_movement' in item else None

    # remove unnessary cargoes
    if any(word in item['cargo_product'].lower() for word in CARGO_BLACKLIST):
        return

    cargo_list = normalize_product(
        item['cargo_product'], item['cargo_volume'], item['cargo_movement']
    )

    for col in [
        'arrival_date',
        'arrival_time',
        'berthed_date',
        'berthed_time',
        'eta_date',
        'eta_time',
        'cargo_movement',
        'cargo_product',
        'cargo_volume',
    ]:

        item.pop(col, None)

    for _cargo in cargo_list:
        # build proper Cargo model
        item['cargo'] = _cargo
        yield item


def field_mapping():
    return {
        'berth': ('berth', None),
        'vessel': ('vessel_name', None),
        'nationality': ('vessel_flag', None),
        'agent': ('shipping_agent', None),
        'arrival_date': ('arrival_date', None),
        'arrival_time': ('arrival_time', None),
        'eta_date': ('eta_date', None),
        'eta_time': ('eta_time', None),
        'loa': ('vessel_loa', lambda x: try_apply(x, float, int, str)),
        'gt': ('vessel_gt', lambda x: try_apply(x, float, int, str)),
        'fwd': ignore_key('irrelevant'),
        'aft': ignore_key('irrelevant'),
        'berthed_date': ('berthed_date', None),
        'berthed_time': ('berthed_time', None),
        'cargo': ('cargo_product', may_strip),
        'total': ('cargo_volume', lambda x: try_apply(x, float, int, str)),
        'tonnage': ('cargo_volume', None),
        'remarks': ('cargo_movement', None),
        'purpose': ('cargo_movement', None),
        'terminal': ('installation', None),
        'position': ignore_key('irrelevant'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_date(raw_date, raw_time):
    """
    Args:
        raw_date (str):
        raw_time (str):

    Returns:
        str: ISO-8601 formatted matching date

    Examples:
        >>> normalize_date('01/01/2019', '0.1')
        '2019-01-01T00:00:00'
        >>> normalize_date('01/01/2019', None)
        '2019-01-01T00:00:00'
        >>> normalize_date('01/01/2019', '10:00')
        '2019-01-01T10:00:00'
    """
    if raw_date and is_date(raw_date):
        _datetime = parse_date(raw_date, dayfirst=True)
        if raw_time and ':' in raw_time:
            _hour, _minute = raw_time.split(':')
            return _datetime.replace(hour=int(_hour), minute=int(_minute)).isoformat()

        return _datetime.isoformat()

    return None


def normalize_product(raw_prod, raw_volume, raw_movement):
    """
    Args:
        raw_prod (str):
        raw_volume (str):
        raw_movement (str):

    Returns:
        str:

    Examples:  # noqa
        >>> normalize_product('GASOIL', '10000', 'DISCHARGE GASOIL')
        [{'product': 'GASOIL', 'movement': 'discharge', 'volume': '10000', 'volume_unit': 'tons'}]
        >>> normalize_product('GASOIL', '10000', None)
        [{'product': 'GASOIL', 'movement': None, 'volume': '10000', 'volume_unit': 'tons'}]
        >>> normalize_product('GASOIL/DIESEL', '10000', 'DISCHARGE GASOIL')
        [{'product': 'GASOIL', 'movement': 'discharge', 'volume': 5000.0, 'volume_unit': 'tons'}, {'product': 'DIESEL', 'movement': 'discharge', 'volume': 5000.0, 'volume_unit': 'tons'}]
        >>> normalize_product('GASOIL/DIESEL', '10000/2000', 'DISCHARGE GASOIL')
        [{'product': 'GASOIL', 'movement': 'discharge', 'volume': '10000', 'volume_unit': 'tons'}, {'product': 'DIESEL', 'movement': 'discharge', 'volume': '2000', 'volume_unit': 'tons'}]
        >>> normalize_product('GASOIL/DIESEL', None, None)
        [{'product': 'GASOIL', 'movement': None, 'volume': None, 'volume_unit': None}, {'product': 'DIESEL', 'movement': None, 'volume': None, 'volume_unit': None}]
    """
    movement = None
    if raw_movement:
        _movement_match = re.match(r'^.*(DISCHARGE|LOAD).*$', raw_movement)
        if _movement_match:
            movement = MOVEMENT_MAPPING.get(_movement_match.group(1))

    volume_list = None
    if raw_volume:
        _volume_match = re.match(r'([0-9.\/]+)', raw_volume)
        if _volume_match:
            volume_list = [try_apply(x, float, int, str) for x in _volume_match.group(1).split('/')]

    product_list = raw_prod.replace('&', '/').split('/')

    f_list = []
    if volume_list and raw_prod:
        if len(volume_list) == len(product_list):
            for idx, _item in enumerate(product_list):
                f_item = {
                    'product': _item,
                    'movement': movement,
                    'volume': volume_list[idx],
                    'volume_unit': Unit.tons,
                }

                f_list.append(f_item)

        if len(product_list) > len(volume_list) and len(volume_list) == 1:
            # split volume equally for each product
            for idx, _item in enumerate(product_list):
                f_item = {
                    'product': _item,
                    'movement': movement,
                    'volume': int(volume_list[0]) / 2,
                    'volume_unit': Unit.tons,
                }

                f_list.append(f_item)

    if raw_prod and not volume_list:
        for idx, _item in enumerate(product_list):
            f_item = {'product': _item, 'movement': None, 'volume': None, 'volume_unit': None}

            f_list.append(f_item)

    return f_list


def is_date(string, fuzzy=False):
    """
    Args:
        string (str):
        fuzzy (bool):

    Returns:
        str: ISO-8601 formatted matching date

    Examples:
        >>> is_date('01/01/2019')
        True
        >>> is_date('TBC')
        False
    """
    try:
        parse_date(string)
        return True

    except ValueError:
        return False
