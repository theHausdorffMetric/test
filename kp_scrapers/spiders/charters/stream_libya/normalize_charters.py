import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


MISSING_ROWS = []


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


logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, charters_mapping())

    if not item['vessel'] or not item['vessel']['name']:
        return

    # print(item)
    # enrich laycan dates with year and month
    item['lay_can_start'], item['lay_can_end'] = normalize_laycan(item['laycan'], str(raw_item))
    f_prod, f_vol, f_units = normalize_product_volume(item['cargo_product'], item['cargo_volume'])

    # build cargo sub-model
    item['cargo'] = {'product': f_prod, 'movement': 'load', 'volume': f_vol, 'volume_unit': f_units}

    for col in ('cargo_product', 'cargo_volume', 'laycan'):
        item.pop(col, None)

    return item


def charters_mapping():
    return {
        'M/T Name': ('vessel', lambda x: {'name': x} if 'tbn' not in x.lower() else None),
        'Terminal': ('departure_zone', lambda x: TERMINAL_MAPPING.get(x, x)),
        'Grade': ('cargo_product', may_strip),
        'Quantity': ('cargo_volume', lambda x: x.replace(',', '').replace(' ', '').lower()),
        'Qty (+/-5%)': ('cargo_volume', lambda x: x.replace(',', '').replace(' ', '').lower()),
        'Quantity ()+/-5%': ('cargo_volume', lambda x: x.replace(',', '').replace(' ', '').lower()),
        'Load Window': ('laycan', may_strip),
        'Lifter': ('charterer', lambda x: x if 'tbn' not in x.lower() else None),
        'Destination': ('arrival_zone', lambda x: [x] if 'tbn' not in x.lower() else None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_laycan(raw_laycan, raw_dict):
    """Normalize raw laycan date.

    Raw laycan inputs can be of the following formats:
        1) range: '02-03 SEP 2019'

    Args:
        raw_laycan (str):

    Returns:
        Tuple[str]: tuple of laycan period

    Examples:
        >>> normalize_laycan('02-03 SEP 2019', 'dict')
        ('2019-09-02T00:00:00', '2019-09-03T00:00:00')
    """
    days, month, year = raw_laycan.split(' ')
    start_day, end_day = days.split('-')

    try:
        lay_can_start = parse_date(f'{year} {month} {start_day}')
        lay_can_end = parse_date(f'{year} {month} {end_day}')
        return lay_can_start.isoformat(), lay_can_end.isoformat()
    except Exception:
        MISSING_ROWS.append(raw_dict)
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
        ('MELLITAH  c.o.', '600', 'kilobarrel')
        >>> normalize_product_volume('Amna/sirtica', '590kamna+10ksirticabbls')
        ('Crude Oil', None, None)
        >>> normalize_product_volume('Buattifel c.o.', '400k/1500bblsflush')
        ('Crude Oil', None, None)
    """
    product_list = re.split(r'[\/\+]', raw_product)
    volume_list = re.split(r'[\/\+]', raw_volume)

    if len(product_list) == 1 and len(volume_list) == 1:
        _match = re.match(r'(\d+)(.*)', volume_list[0])
        volume, units = _match.groups()
        return product_list[0], volume, UNIT_MAPPING.get(units, units)

    return 'Crude Oil', None, None
