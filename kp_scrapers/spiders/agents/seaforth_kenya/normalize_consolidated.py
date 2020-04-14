import datetime as dt
from itertools import zip_longest
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import is_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MISSING_ROWS = []
ZONE_MAPPING = {'kot': 'Kipevu', 'mbk': 'Mbaraki', 'sot': 'Shimanzi', 'agol': 'Mombasa'}
PLAYER_BLACKLIST = ['?', 'n/a']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # vessel item
    item['vessel'] = {'name': item.pop('vessel_name', None)}
    if not item.get('vessel').get('name'):
        return

    # sometimes source provides second date for second date
    second_port_date = item.pop('second_port_date', None)
    port_list_checker = item['raw_port_name'].split('+')

    # get movement list and buyers
    get_movements, get_volume = normalize_movement_list(item)
    buyer = item.pop('cargo_buyer', None)
    seller = item.pop('cargo_seller', None)

    # split products, quantities, units and movement
    cargoes = normalize_cargo(
        item['cargo_product'], get_movements, get_volume, port_list_checker, item
    )

    for col in [
        'cargo_product',
        'raw_port_name',
        'cargo_volume_disc',
        'cargo_volume_load',
        'second_port_date',
        'departure_zone',
    ]:
        item.pop(col, None)

    if cargoes:
        for idx, cargo in enumerate(cargoes):
            # cargo[0] -  product information
            # cargo[1] -  movement information
            # cargo[2] -  volume information
            # cargo[3] -  port information
            item['cargo'] = {
                'product': cargo[0],
                'movement': cargo[1],
                'volume': cargo[2],
                'volume_unit': Unit.tons,
                'buyer': {'name': may_strip(buyer)}
                if buyer and buyer.lower() not in PLAYER_BLACKLIST
                else None,
                'seller': {'name': may_strip(seller)}
                if seller and seller.lower() not in PLAYER_BLACKLIST
                else None,
            }
            item['port_name'] = ZONE_MAPPING.get(may_strip(cargo[3].lower()), cargo[3])
            if idx != 0 and second_port_date and len(port_list_checker) == 2:
                item['eta'] = second_port_date
                for overwrite_col in ('arrival', 'departure', 'berthed'):
                    item.pop(overwrite_col)

            if not item['cargo'].get('buyer') or not item['cargo'].get('buyer').get('name'):
                item['cargo'].pop('buyer')

            yield item


def field_mapping():
    return {
        'vessel': ('vessel_name', None),
        'arrival': ('arrival', lambda x: x if is_isoformat(x) else None),
        'berthing': ('berthed', lambda x: x if is_isoformat(x) else None),
        'departure': ('departure', lambda x: x if is_isoformat(x) else None),
        'second atb': ('second_port_date', lambda x: x if is_isoformat(x) else None),
        'tip(days)': ignore_key('irrelevant'),
        'load/last port': ('departure_zone', may_strip),
        'shipper': ('cargo_seller', None),
        'main receiver': ('cargo_buyer', None),
        'cargo type': ('cargo_product', None),
        'import tonnes': ('cargo_volume_disc', lambda x: x if is_number(x) else None),
        'load tonnes': ('cargo_volume_load', lambda x: x if is_number(x) else None),
        'next port': ignore_key('irrelevant'),
        'berth': ('raw_port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_movement_list(raw_item):
    """clean dates

    Args:
        raw_date (str):

    Returns:
        Tuple[List[str, str]]:

    Examples:
        >>> normalize_movement_list({'cargo_volume_load': '1000'})
        (['load'], ['1000'])
        >>> normalize_movement_list({'cargo_volume_load': '1000', 'cargo_volume_disc': '1000'})
        (['load', 'discharge'], ['1000', '1000'])
    """
    movement_list = []
    volume_list = []
    if raw_item.get('cargo_volume_load'):
        movement_list.append('load')
        volume_list.append(raw_item.get('cargo_volume_load'))

    if raw_item.get('cargo_volume_disc'):
        movement_list.append('discharge')
        volume_list.append(raw_item.get('cargo_volume_disc'))

    return movement_list, volume_list


def is_number(check_number):
    try:
        float(check_number)
        return True
    except ValueError:
        return False


def normalize_cargo(raw_product, raw_movement_list, raw_volume_list, raw_port_list, raw_row):
    """Normalize cargo, yield multiple items

    Args:
        raw_product (str):
        raw_movement_list [List]:
        raw_volume_list [List]:
        raw_port_list [List]:
        raw_row (str):

    Returns:
        List[str]:

    Examples:
        >>> normalize_cargo('GO+JET', ['load'], ['10', '20'], ['SOT'], 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'load', '10', 'SOT'), ('JET', 'load', '20', 'SOT')]
        >>> normalize_cargo('GO & JET', ['load'], ['10', '20'], ['SOT'], 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'load', '10', 'SOT'), ('JET', 'load', '20', 'SOT')]
        >>> normalize_cargo('GO', ['load'], ['10', '20'], ['SOT'], 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'load', '10', 'SOT'), ('GO', 'load', '20', 'SOT')]
        >>> normalize_cargo('GO+JET', ['load'], ['10'], ['SOT'], 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'load', '5.0', 'SOT'), ('JET', 'load', '5.0', 'SOT')]
        >>> normalize_cargo('GO+JET', ['load', 'discharge'], ['10'], ['SOT', 'MOT'], 'ABC GO 2019-01-01T00:00:00') # noqa
        [('GO', 'load', '5.0', 'SOT'), ('JET', 'discharge', '5.0', 'MOT')]
        >>> normalize_cargo('GO', ['discharge'], ['10'], ['SOT', 'MOT'], 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'discharge', '5.0', 'SOT'), ('GO', 'discharge', '5.0', 'MOT')]
        >>> normalize_cargo('GO+JET', ['discharge'], ['10'], ['SOT', 'MOT'], 'ABC GO 2019-01-01T00:00:00')
        [('GO', 'discharge', '5.0', 'SOT'), ('JET', 'discharge', '5.0', 'MOT')]
    """
    f_product_list = []
    raw_product_list = [may_strip(rpl) for rpl in re.split(r'[\+\&]', raw_product)]

    product_list = zip_longest(raw_product_list, raw_movement_list, raw_volume_list, raw_port_list)

    for idx, _item in enumerate(product_list):
        _item = list(_item)

        if idx == 0:
            # memoise data to ffill
            _product = _item[0]
            _movement = _item[1]
            _volume = _item[2]
            _port = _item[3]

        if not _item[1]:
            _item[1] = _movement

        if not _item[0]:
            _item[0] = _product

        if len(raw_product_list) > 1 and len(raw_volume_list) == 1:
            _item[2] = str(float(_volume) / len(raw_product_list))

        if len(raw_product_list) == 1 and len(raw_port_list) > 1:
            _item[2] = str(float(_volume) / len(raw_port_list))

        if not _item[3]:
            _item[3] = _port

        f_product_list.append((_item[0], _item[1], _item[2], _item[3]))

    return f_product_list


def post_process_import_item(item, trade):
    """Transform an import spot charter into a properly mapped export spot charter.

    Args:
        item (Dict[str, str]):
        trade (Dict[str, str] | None):

    """
    if trade:
        # laycan period should be +/- 1 day from trade date (c.f. analysts)
        lay_can = parse_date(trade['Date (origin)'], dayfirst=False)
        item['arrival'] = (lay_can - dt.timedelta(days=1)).isoformat()
        item['berthed'] = lay_can.isoformat()
        item['departure'] = (lay_can + dt.timedelta(days=1)).isoformat()
        # # use origin port as departure zone, destination port as arrival zone
        item['port_name'] = trade['Origin']
    else:
        item['departure'] = None
        item['arrival'] = None
        item['berthed'] = None
        item['port_name'] = item['departure_zone']
