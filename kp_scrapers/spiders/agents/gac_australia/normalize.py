import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=False, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    if not item['vessel']['name']:
        return None

    # build proper Cargo model
    volume = item.pop('cargo_volume', None)
    units = Unit.tons if volume else None
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',  # The report denotes only load as per info from PO.
        'volume': volume,
        'volume_unit': units,
    }

    return item


def field_mapping():
    return {
        'vessel': ('vessel', lambda x: {'name': x}),
        'eta': ('eta', None),
        'volume': ('volume', None),
        'berth': ('berth', None),
        'dest': ('next_zone', lambda x: None if x == 'TBN' else x),
        'cargo_product': ('cargo_product', None),
        'etb': ('berthed', lambda x: to_isoformat(x, dayfirst=True)),
        'etd': ('departure', lambda x: to_isoformat(x, dayfirst=True)),
        'cargo': ('cargo_product', None),
        'qty(mt)': ('cargo_volume', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }
