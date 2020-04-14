from datetime import datetime

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


TODAY_AS_ETA = ['IN BERTH', 'AT ANCHOR']

CARGO_BLACKLIST = [
    'CEMENT',
    'CEMENT CLINKER',
    'CONTAINERS (COLLECTIVE)',
    'DANGEROUS GOODS',
    'FLY ASH',
    'GENERAL CARGO',
    'LOGS',
    'NON CARGO VESSEL - BUNKERS',
    'PASSENGERS',
    'WOOD PELLETS',
]

ZONE_MAPPING = {'APLNG JETTY': 'APLNG', 'GLNG JETTY': 'GLNG'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())
    if not item['vessel_name']:
        return

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('imo')}

    item['cargoes'] = list(normalize_cargo(item.pop('products'), item.pop('volumes')))
    if not item['cargoes']:
        return

    item['port_name'] = normalize_port_name(item.pop('berth')) or item['port_name']

    return item


def field_mapping():
    return {
        '0': ('vessel_name', lambda x: x if 'TBA' not in x else None),
        '1': (ignore_key('vessel agent')),
        '2': ('berth', None),
        '3': ('eta', lambda x: get_uct_now() if x in TODAY_AS_ETA else to_isoformat(x)),
        '4': ('products', None),
        '5': ('volumes', None),
        '6': ('reported_date', to_isoformat),
        'imo': ('imo', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
    }


def get_uct_now():
    return datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat()


def normalize_port_name(berth):
    """Retrieve port name from berth for some specific zones.

    APLNG JETTY --> APLNG
    GLNG JETTY --> GLNG

    Args:
        berth:

    Returns:

    """
    return ZONE_MAPPING.get(berth)


def normalize_cargo(products, volumes):
    """Normalize cargo.

    Args:
        products:
        volumes:

    Returns:

    """
    volume_list = volumes.split(',')
    for idx, product in enumerate(products.split(',')):
        if product.upper() not in CARGO_BLACKLIST:
            yield {
                'product': product,
                'volume': volume_list[idx] if volume_list[idx] != '1' else None,
                'volume_unit': Unit.tons,
            }
