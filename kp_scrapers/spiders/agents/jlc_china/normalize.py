from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


MOVEMENT_MAPPING = {'Import': 'discharge', 'Export': 'load'}

VESSEL_NAME_BLACKLIST = ['N/A']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, field_mapping())

    # completely disregard rows with blacklisted vessel names
    if not item['vessel']:
        return

    # completely disregard rows without products
    if not item['cargo_product']:
        return

    item['cargo'] = {
        'product': item.pop('cargo_product'),
        'movement': item.pop('cargo_movement', None),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.tons,
    }
    return item


def field_mapping():
    return {
        'Crude Type': ('cargo_product', None),
        'Date': ('eta', lambda x: to_isoformat(x, dayfirst=False)),
        'Estimated Date of Arrival': ('eta', lambda x: to_isoformat(x, dayfirst=False)),
        'Estimated Date of loading/unloading': ('eta', lambda x: to_isoformat(x, dayfirst=False)),
        'Importer': ignore_key('not useful right now'),
        'Origin': ignore_key('not useful right now'),
        'Port': ('port_name', lambda x: may_strip(x.replace('Port', ''))),
        'Product': ('cargo_product', None),
        'provider_name': ('provider_name', None),
        'Purchaser': ignore_key('not useful right now'),
        'Remarks': ignore_key('Not important right now'),
        'reported_date': ('reported_date', None),
        'Seller': ignore_key('Not important right now'),
        'Trade mode': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),
        'Trade Mode': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),
        'Vessel': ('vessel', lambda x: {'name': x} if x not in VESSEL_NAME_BLACKLIST else None),
        'Vessel Type': ignore_key('redundant'),
        'Volume': ('cargo_volume', None),
        'Volume (mt)': ('cargo_volume', None),
        'Volume (mt) ': ('cargo_volume', None),
    }
