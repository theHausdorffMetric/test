import logging

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    # build proper Vessel model
    item['vessel'] = {'name': item.pop('vessel_name', None)}

    # discard vessel movements that do not contain cargo
    if not item['cargo_product']:
        logger.info(f'Discarding {item["vessel"]["name"]} cargo movement as there is no cargo')
        return

    # build proper Cargo model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'discharge',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.tons,
        'buyer': {'name': item.pop('cargo_receiver', None)},
    }

    item['port_name'] = 'TRIESTE'

    return item


def field_mapping():
    # declarative mapping for ease of developement/maintenance
    return {
        'SHIP NAME': ('vessel_name', None),
        'ETA': ('eta', None),
        'hh.mm': ignore_key('hh.mm'),
        'PROSP.': ignore_key('PROSP.'),
        'POB': ('berthed', None),
        'BERTH': ignore_key('BERTH'),
        'ETS': ('departure', None),
        'LOAD. PORT': ignore_key('LOAD. PORT'),
        'GRADE': ('cargo_product', None),
        'M. TONS': ('cargo_volume', None),
        'RECEIVER': ('cargo_receiver', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }
