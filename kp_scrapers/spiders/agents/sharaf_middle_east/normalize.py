from itertools import zip_longest
import logging
import re

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MOVEMENT_MAPPING = {'loading': 'load', 'discharging': 'discharge', 'disch': 'discharge'}


UNIT_MAPPING = {
    'bbls': Unit.barrel,
    'bbl': Unit.barrel,
    'kbbl': Unit.kilobarrel,
    'kbbls': Unit.kilobarrel,
    'mt': Unit.tons,
    'cbm': Unit.cubic_meter,
}


MISSING_ROWS = []


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # completely disregard rows with no departure date
    if not item['vessel']['name']:
        return

    # get unit volume list
    if item['cargo_product'] and 'tba' not in item['cargo_product'].lower():
        f_cargo_list, f_units = split_product(
            item.pop('cargo_product', None),
            item.pop('cargo_volume_unit', None),
            item.pop('attachment_name', None),
        )

        if f_cargo_list:
            for cargo in f_cargo_list:
                item['cargo'] = {
                    'product': cargo[0],
                    'movement': item.pop('cargo_movement', None),
                    'volume': cargo[1],
                    'volume_unit': f_units,
                }

                yield item
        else:
            logger.error('Unable to parse item: %s', item)
            MISSING_ROWS.append(str(item))


def field_mapping():
    return {
        'Port Code': ignore_key('redundant'),
        'VESSEL NAME': ('vessel', lambda x: {'name': clean_vessel_name(x) if x else None}),
        'TYPE OF CALL': (
            'cargo_movement',
            lambda x: MOVEMENT_MAPPING.get(x.lower(), may_strip(x.lower())),
        ),
        'ETA': ('eta', None),
        'CARGO': ('cargo_product', lambda x: x.replace('F/Oil', 'Fuel Oil')),
        'QTY(MT/BBLS)': ('cargo_volume_unit', lambda x: str(x).replace(',', '')),
        'Loadport': ignore_key('redundant'),
        'Disport': ignore_key('redundant'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'attachment_name': ('attachment_name', None),
    }


def split_product(raw_product, raw_volume_unit, raw_attachment_name):
    """split and yield multiple products

    Args:
        raw_product (str):
        raw_volume_unit (str):
        raw_attachment_name (str):

    Examples:
        >>> split_product('Jet A1 + Gasoil', '30000 + 20000', 'yanbu')
        ([('Jet A1', '30000'), ('Gasoil', '20000')], 'tons')
        >>> split_product('Jet A1 + Gasoil', '30000', 'yanbu')
        ([('Jet A1', '15000.0'), ('Gasoil', '15000.0')], 'tons')
        >>> split_product('Jet A1', '30000', 'yanbu')
        ([('Jet A1', '30000')], 'tons')
        >>> split_product('Butane / Propane', 'TBA', 'yanbu')
        ([('Butane', None), ('Propane', None)], None)


    Returns:
        str:
    """
    product_list = [may_strip(prod) for prod in re.split(r'[\\\+\&\/\;]', raw_product)]

    volume_unit_list, units = get_vol_unit(raw_volume_unit, raw_attachment_name)

    if len(product_list) == len(volume_unit_list):
        return list(zip(product_list, volume_unit_list)), units

    if len(product_list) > 1 and len(volume_unit_list) == 1:
        final_list = []
        for item_product in product_list:
            # source may contain typo errors
            try:
                vol_append = str(float(volume_unit_list[0]) / len(product_list))
            except Exception:
                vol_append = None
            final_list.append((item_product, vol_append))
        return final_list, units

    if product_list and not volume_unit_list:
        return list(zip_longest(product_list, volume_unit_list)), None

    return None, None


def get_vol_unit(get_vol_unit, get_attachment_name):
    """split and yield multiple products

    Args:
        get_vol_unit (str):
        get_attachment_name (str):

    Examples:
        >>> get_vol_unit('60000 + 30000', 'yanbu')
        (['60000', '30000'], 'tons')
        >>> get_vol_unit('60000 + 30000 MTS', 'yanbu')
        (['60000', '30000'], 'tons')
        >>> get_vol_unit('1000 KBBLS', 'yanbu')
        (['1000'], 'kilobarrel')
        >>> get_vol_unit('1000000', 'basra')
        (['1000000'], 'barrel')
        >>> get_vol_unit('-', 'basra')
        ([], None)

    Returns:
        str:
    """
    final_vol_list = []
    vol_list = [may_strip(vol) for vol in re.split(r'[\\\+\&\/]', get_vol_unit)]

    for _vol_unit in vol_list:
        match = re.match(r'([0-9.]+)([A-z\s]+)?', _vol_unit)
        if match:
            vol, unit = match.groups()
            final_vol_list.append(vol)
            final_unit = unit.lower() if unit else None

    if final_vol_list:
        final_unit = (
            UNIT_MAPPING.get(may_strip(final_unit), Unit.tons)
            if 'basra' not in get_attachment_name.lower()
            else Unit.barrel
        )

        return final_vol_list, final_unit

    return [], None


def clean_vessel_name(raw_vessel_name):
    """clean vessel name

    Args:
        raw_vessel_name (str):

    Examples:
        >>> clean_vessel_name('mt dht leopard')
        'dht leopard'

    Returns:
        str:
    """
    return may_strip(raw_vessel_name.lower().replace('mt', ''))
