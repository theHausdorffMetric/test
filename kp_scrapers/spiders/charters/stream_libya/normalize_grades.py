from itertools import zip_longest
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


TERMINAL_MAPPING = {
    'ras lanuf': 'Ras Lanuf',
    'es sider': 'Es Sider',
    'marsa el brega': 'Marsa El Brega',
    'zueitina': 'Zueitina',
    'm elhariga': 'zawia',
    'mellitah': 'Mellitah',
    'farwah off shore': 'Farwah',
    'bouri off shore': 'Bouri',
    'zawia': 'Zawia',
}

UNIT_MAPPING = {
    'kbbls': Unit.kilobarrel,
    'kbbl': Unit.kilobarrel,
    'k': Unit.kilobarrel,
    'bbls': Unit.barrel,
    'bbl': Unit.barrel,
}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    if not item['vessel'] or not item['vessel']['name']:
        return

    item['arrival'], item['departure'] = normalize_dates(item['laycan'])

    list_of_cargo = normalize_product_volume(item['cargo_product'], item['cargo_volume'])

    for col in ('cargo_product', 'cargo_volume', 'laycan'):
        item.pop(col, None)

    for _cargo in list_of_cargo:
        item['cargo'] = {
            'product': _cargo[0],
            'movement': 'load',
            'volume': _cargo[1],
            'volume_unit': _cargo[2],
        }
        yield item


def field_mapping():
    return {
        'M/T Name': ('vessel', lambda x: {'name': x} if 'tbn' not in x.lower() else None),
        'Terminal': ('port_name', lambda x: TERMINAL_MAPPING.get(x, x)),
        'Grade': ('cargo_product', may_strip),
        'Quantity': ('cargo_volume', lambda x: x.replace(',', '').replace(' ', '').lower()),
        'Qty (+/-5%)': ('cargo_volume', lambda x: x.replace(',', '').replace(' ', '').lower()),
        'Quantity ()+/-5%': ('cargo_volume', lambda x: x.replace(',', '').replace(' ', '').lower()),
        'Load Window': ('laycan', may_strip),
        'Lifter': ignore_key('irrelevant'),
        'Destination': ignore_key('irrelevant'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', to_isoformat),
    }


def normalize_dates(raw_dates):
    """Normalize dates
    Raw laycan inputs can be of the following formats:
        1) range: '02-03 SEP 2019'

    Args:
        raw_dates (str):

    Returns:
        Tuple[str]:

    Examples:
        >>> normalize_dates('02-03 SEP 2019')
        ('2019-09-02T00:00:00', '2019-09-03T00:00:00')
    """
    days, month, year = raw_dates.split(' ')
    start_day, end_day = days.split('-')

    try:
        arrival = parse_date(f'{year} {month} {start_day}')
        departure = parse_date(f'{year} {month} {end_day}')
        return arrival.isoformat(), departure.isoformat()
    except Exception:
        return None, None


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
        >>> normalize_product_volume('MELLITAH  c.o.', '600kbbls')
        [('MELLITAH  c.o.', '600', 'kilobarrel')]
        >>> normalize_product_volume('Amna/sirtica', '590kamna+10ksirticabbls')
        [('Amna', '590', 'kilobarrel'), ('sirtica', '10', 'kilobarrel')]
        >>> normalize_product_volume('Buattifel c.o.', '400k/1500bblsflush')
        [('Buattifel c.o.', '401500', 'barrel')]
        >>> normalize_product_volume('SARIR+MESLA', '100kbbl')
        [('SARIR', '50.0', 'kilobarrel'), ('SARIR', '50.0', 'kilobarrel')]
    """
    product_list = re.split(r'[\/\+]', raw_product)
    volume_list = re.split(r'[\/\+]', raw_volume)

    f_list = []
    if len(product_list) == len(volume_list):
        _list = list(zip(product_list, volume_list))
        for _val in _list:
            vol, units = re.search(r'(\d+)(k|bbl|barrel|kbbl)', _val[1]).groups()
            f_list.append((_val[0], vol, UNIT_MAPPING.get(units, units)))

    if len(product_list) > 1 and len(volume_list) == 1:
        _list = list(zip_longest(product_list, volume_list))
        _vol, units = re.search(r'(\d+)(k|bbl|barrel|kbbl)', volume_list[0]).groups()
        vol = int(_vol) / len(product_list)
        for _prod in product_list:
            f_list.append((product_list[0], str(vol), UNIT_MAPPING.get(units, units)))

    if 'flush' in raw_volume:
        total_volume = 0
        for _vol in volume_list:
            t_vol, t_unit = re.search(r'(\d+)(\w+)', _vol).groups()
            if UNIT_MAPPING.get(t_unit) == 'kilobarrel':
                t_vol = int(t_vol) * 1000
                total_volume = total_volume + t_vol
            else:
                total_volume = total_volume + int(t_vol)

        f_list.append((product_list[0], str(total_volume), Unit.barrel))

    return f_list
