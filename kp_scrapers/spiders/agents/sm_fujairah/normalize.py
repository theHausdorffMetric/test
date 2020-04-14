from itertools import zip_longest
import logging
import re

from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MISSING_ROWS = []


MOVEMENT_MAPPING = {'l': 'load', 'd': 'discharge', 'disch': 'discharge'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    if not item.get('vessel', {}).get('name'):
        return

    # split products, quantities, units and movement
    cargoes = normalize_cargo(
        item['cargo_product'], item['cargo_movement'], item['cargo_volume'], item
    )

    for col in ['cargo_product', 'cargo_movement', 'cargo_volume']:
        item.pop(col, None)

    if cargoes:
        for cargo in cargoes:
            # cargo[0] -  product information
            # cargo[1] -  movement information
            # cargo[2] -  volume information
            item['cargo'] = {
                'product': cargo[0],
                'movement': MOVEMENT_MAPPING.get(may_strip(cargo[1]), cargo[1]),
                'volume': cargo[2] if cargo[2] else None,
                'volume_unit': Unit.cubic_meter if cargo[2] else None,
            }
            yield item


def field_mapping():
    return {
        'SR': (ignore_key('irrelevant')),
        'Date': ('berthed', None),
        'Jetty': ('berth', lambda x: try_apply(x, str).replace('.0', '')),
        'Operation': ('cargo_movement', lambda x: re.sub(r'[\W]', '', x.lower())),
        'Product': ('cargo_product', None),
        'Vessel Name': ('vessel', lambda x: {'name': may_strip(x)}),
        # TODO include buyer and seller
        'Customer / Terminal': (ignore_key('buyer and seller, cannot be used currently')),
        'Latest Value b/w CT inform & Berth Available': (ignore_key('irrelevant')),
        'Parcel size M3': ('cargo_volume', lambda x: try_apply(x, str).replace(',', '')),
        'port_name': ('port_name', None),
        'installation': ('installation', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_cargo(raw_product, raw_movement, raw_volume, raw_row):
    """Normalize cargo, yield multiple items

    Args:
        raw_port (str):

    Returns:
        str:

    Examples:
        >>> normalize_cargo('GO/JET', 'L', '10/20', 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'L', '10'), ('JET', 'L', '20')]
        >>> normalize_cargo('GO', 'L', '10/20', 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'L', '10'), ('GO', 'L', '20')]
        >>> normalize_cargo('GO/JET', 'L', '10', 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'L', '5.0'), ('JET', 'L', '5.0')]
        >>> normalize_cargo('GO/JET', 'L/D', '10', 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'L', '5.0'), ('JET', 'D', '5.0')]
    """
    f_product_list = []
    raw_product_list = raw_product.split('/')
    raw_movement_list = raw_movement.split('/')
    raw_volume_list = raw_volume.split('/')

    product_list = zip_longest(raw_product_list, raw_movement_list, raw_volume_list)

    for idx, _item in enumerate(product_list):
        _item = list(_item)

        if idx == 0:
            # memoise data to ffill
            _product = _item[0]
            _movement = _item[1]
            _volume = _item[2]

        if not _item[1]:
            _item[1] = _movement

        if not _item[0]:
            _item[0] = _product

        if len(raw_product_list) > 1 and len(raw_volume_list) == 1:
            _item[2] = str(float(_volume) / len(raw_product_list))

        if len(raw_movement_list) != len(raw_movement_list):
            MISSING_ROWS.append(raw_row)
            return

        f_product_list.append((_item[0], _item[1], _item[2]))

    return f_product_list
