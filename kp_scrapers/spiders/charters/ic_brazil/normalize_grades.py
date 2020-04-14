from itertools import repeat

from kp_scrapers.lib.parser import may_strip, split_by_delimiters, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


BLACKLIST = ['', '-', 'N/A', 'NIL', 'TBC', 'TBI']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # remove vessels not named yet
    if not item['vessel']['name']:
        return

    # remove vessels without a confirmed portcall date yet
    if not (item.get('eta') or item.get('berthed') or item.get('departure')):
        return

    # remove those with no cargo
    if not item['cargo_product']:
        return

    if item['raw_port_name']:
        item['port_name'] = item['raw_port_name']

    item['cargo_unit'] = Unit.tons if item['cargo_volume'] else None

    product_volume_list = split_multiple_products(item['cargo_product'], item['cargo_volume'])
    movement = item.pop('cargo_movement', None)
    volume_unit = item.pop('cargo_unit', None)

    for col in ('cargo_volume', 'cargo_product', 'raw_port_name'):
        item.pop(col, None)

    for zipped_item in product_volume_list:
        # build cargo sub model
        item['cargo'] = {
            'product': zipped_item[0],
            'movement': movement,
            'volume': zipped_item[1],
            'volume_unit': volume_unit,
        }
        yield item


def field_mapping():
    return {
        'VESSEL': ('vessel', lambda x: {'name': normalize_string(x)}),
        'ETA': ('eta', normalize_string),
        'ETB': ('berthed', normalize_string),
        'ETS': ('departure', normalize_string),
        'WAITING TIME (DAYS)': ignore_key('redundant'),
        'WT': ignore_key('redundant'),
        'BERTH': ('berth', normalize_string),
        'STATUS': ignore_key('redundant'),
        'OPERATION': (
            'cargo_movement',
            lambda x: normalize_string(x).lower() if normalize_string(x) else None,
        ),
        'DESTINATION': ignore_key('redundant'),
        'CHARTERER': ignore_key('redundant'),
        'ORIGIN': ignore_key('redundant'),
        'COMMODITY': ('cargo_product', normalize_string),
        'PRODUCT': ('cargo_product', normalize_string),
        'QUANTITY': ('cargo_volume', normalize_string),
        'SHIPPER': ignore_key('redundant'),
        'RECEIVER': ignore_key('redundant'),
        'AGENT': ignore_key('redundant'),
        'raw_port_name': ('raw_port_name', normalize_portname),
        'PORT': ('port_name', normalize_portname),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_string(raw_value):
    """Remove unnecessary strings

    Args:
        raw_value (str):

    Examples:
        >>> normalize_string('DHT LEOPARD')
        'DHT LEOPARD'
        >>> normalize_string('TBC')
        >>> normalize_string(' TBI')
        >>> normalize_string('')

    """
    raw_value = may_strip(raw_value)
    if (raw_value is not None and raw_value.upper() in BLACKLIST) or not raw_value:
        return None

    return raw_value


def normalize_portname(raw_portname):
    """Remove unnecessary strings in port names to increase matching rate

    Args:
        raw_value (str):

    Examples:
        >>> normalize_portname('Sao Luis Port')
        'Sao Luis'
    """
    return may_strip(raw_portname.replace('Port', '')) if raw_portname else None


def split_multiple_products(raw_product, raw_volume):
    """Remove unnecessary strings in port names to increase matching rate

    Args:
        raw_product (str):
        raw_volume (str):

    Examples:
        >>> split_multiple_products('GASOIL / DIESEL', '10000')
        [('GASOIL', '5000'), ('DIESEL', '5000')]
        >>> split_multiple_products('GASOIL / DIESEL', '10000 / 10000')
        [('GASOIL', '10000'), ('DIESEL', '10000')]
        >>> split_multiple_products('CHEMICALS + ETHANOL', '6,500 + 3,000')
        [('CHEMICALS', '6500'), ('ETHANOL', '3000')]
        >>> split_multiple_products('CHEMICALS + ETHANOL', None)
        [('CHEMICALS', None), ('ETHANOL', None)]
        >>> split_multiple_products('GASOIL', '10000')
        [('GASOIL', '10000')]

    Returns:
        List[Tuple[str, str]]
    """
    # split products/volumes into a list
    products = split_by_delimiters(raw_product, '/', '+')
    volumes = [
        # decimal separator can either be commas or periods
        v.replace(',', '').replace('.', '')
        for v in (split_by_delimiters(raw_volume, '/', '+') or [])
    ]

    # sometimes the source will give only a combined volume
    if len(volumes) == 1 and len(volumes) < len(products):
        volumes = [
            try_apply(v, int, str) for v in repeat(float(volumes[0]) / len(products), len(products))
        ]
    elif (len(volumes) > 1 or len(volumes) < 1) and len(volumes) < len(products):
        volumes = [None for _ in range(len(products))]

    return list(zip(products, volumes))
