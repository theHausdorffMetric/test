from itertools import zip_longest
import logging
import re

from kp_scrapers.lib.date import is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, is_number, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MISSING_ROWS = []


UNIT_MAPPING = {
    'kb': Unit.kilobarrel,
    'bbls': Unit.barrel,
    'bblss': Unit.barrel,
    'bbl': Unit.barrel,
    'mt': Unit.tons,
    'mb': Unit.kilobarrel,
}


PORT_MAPPING = {
    'keoje': 'yeosu',
    'daesan': 'daesan',
    'okyc': 'yeosu',
    'ulsan': 'ulsan',
    'yosu': 'yeosu',
}


MOVEMENT_MAPPING = {'load': 'load', 'disch': 'discharge'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, grades_mapping())

    # remove vessels not named
    if not item['vessel']['name']:
        return

    for date_col in ('departure', 'eta', 'berthed'):
        item[date_col] = normalize_dates(item[date_col], item['year'])

    list_of_cargo = normalize_product_volume(item['cargo_product'], item['cargo_volume'])

    item['port_name'] = PORT_MAPPING.get(item['installation'], item['installation'])

    for col in ('cargo_product', 'cargo_volume', 'year'):
        item.pop(col, None)

    for _cargo in list_of_cargo:
        item['cargo'] = {
            'product': _cargo[0],
            'movement': item.pop('cargo_movement', None),
            'volume': _cargo[1],
            'volume_unit': _cargo[2],
        }
        yield item

    return item


def grades_mapping():
    return {
        'VESSEL': ('vessel', lambda x: {'name': may_strip(x) if x else None}),
        'ETA': ('eta', lambda x: may_strip(x.replace('\'', ''))),
        'ETB': ('berthed', lambda x: may_strip(x.replace('\'', ''))),
        'ETD': ('departure', lambda x: may_strip(x.replace('\'', ''))),
        'GRADE': ('cargo_product', may_strip),
        'QTY(K.BLS)': ('cargo_volume', lambda x: x.lower().replace(',', '').replace(' ', '')),
        'LESSEE': ignore_key('lessee'),
        'L/D': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x.lower(), x)),
        'DISPORT': ignore_key('disport'),
        'CHARTERER': ignore_key('charterer'),
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
        'port_name': ('installation', lambda x: may_strip(x.lower().replace('line-up', ''))),
        'year': ('year', None),
    }


def normalize_product_volume(raw_product, raw_volume):
    """Normalize raw laycan date.

    Raw laycan inputs can be of the following formats:
        1) range: '02-03 SEP 2019'

    Args:
        raw_laycan (str):
        year (str | int): string numeric of report's year

    Returns:
        Tuple[str]: tuple of laycan period

    Examples:
        >>> normalize_product_volume('SOKOL CRUDE OIL', '600kb')
        [('SOKOL CRUDE OIL', '600', 'kilobarrel')]
        >>> normalize_product_volume('SOKOL CRUDE OIL', '600000')
        [('SOKOL CRUDE OIL', '600000', 'barrel')]
        >>> normalize_product_volume('Amna/sirtica', '952972/904732')
        [('Amna', '952972', 'barrel'), ('sirtica', '904732', 'barrel')]
        >>> normalize_product_volume('WTI/E45', '2036690')
        [('WTI', '1018345.0', 'barrel'), ('E45', '1018345.0', 'barrel')]
        >>> normalize_product_volume('WTI/E45', '2036kb')
        [('WTI', '1018.0', 'kilobarrel'), ('E45', '1018.0', 'kilobarrel')]
    """
    product_list = re.split(r'[\/\+]', raw_product)
    volume_list = re.split(r'[\/\+]', raw_volume)

    f_list = []
    if len(product_list) == len(volume_list):
        _list = list(zip(product_list, volume_list))
        for _val in _list:
            vol_unit = re.match(r'(\d+)(kb|bbl|bbls|bblss)', _val[1])
            if not vol_unit:
                vol_unit = (_val[1], Unit.barrel)
            else:
                vol_unit = vol_unit.groups()
            f_list.append((_val[0], vol_unit[0], UNIT_MAPPING.get(vol_unit[1], vol_unit[1])))

    if len(product_list) > 1 and len(volume_list) == 1:
        _list = list(zip_longest(product_list, volume_list))
        vol_unit_match = re.match(r'(\d+)(kb|bbl|bbls|bblss)', _list[0][1])
        if vol_unit_match:
            _vol = vol_unit_match.group(1)
            _unit = UNIT_MAPPING.get(vol_unit_match.group(2), vol_unit_match.group(2))
        else:
            _vol = _list[0][1]
            _unit = Unit.barrel

        if is_number(_vol):
            vol = int(_vol) / len(product_list)
        else:
            vol = None

        for _prod in product_list:
            f_list.append((_prod, str(vol), _unit))

    return f_list


def normalize_dates(raw_date, raw_year):
    """Normalize raw laycan date.

    Args:
        raw_date (str):
        raw_year (str):

    Returns:
        str:

    Examples:
        >>> normalize_dates('2/8', '2019')
        '2019-02-08T00:00:00'
        >>> normalize_dates('2/8 11:00', '2019')
        '2019-02-08T11:00:00'
    """
    if not is_isoformat(raw_date):
        datetime_array = raw_date.split(' ')
        if len(datetime_array) == 1:
            try:
                return to_isoformat(f'{datetime_array[0]}/{raw_year}', dayfirst=False)
            except Exception:
                return raw_date

        if len(datetime_array) == 2:
            if datetime_array[1] in ['2400', '24:00']:
                datetime_array[1] = '0000'

            if datetime_array[1].replace('.', '').lower() == 'am':
                datetime_array[1] = '0900'

            if datetime_array[1].replace('.', '').lower() == 'pm':
                datetime_array[1] = '1500'

            try:
                return to_isoformat(
                    f'{datetime_array[0]}/{raw_year} {datetime_array[1]}', dayfirst=False
                )
            except Exception:
                return raw_date

    return raw_date
