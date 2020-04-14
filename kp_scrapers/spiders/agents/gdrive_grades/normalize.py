import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

UNIT_MAPPING = {
    'm3': Unit.cubic_meter,
    'cubic_meter': Unit.cubic_meter,
    'kt': Unit.kilotons,
    'mt': Unit.tons,
    't': Unit.tons,
    'tons': Unit.tons,
}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict(str, str):

    """
    item = map_keys(raw_item, grades_mapping())

    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'imo': item.pop('vessel_imo', None),
        'dead_weight': item.pop('vessel_dwt', None),
        'length': item.pop('vessel_length', None),
    }
    # discard unknown vessels
    if 'TBA' in item['vessel']['name'] or not item['vessel']['name']:
        return

    seller = item.pop('cargo_seller', None)
    buyer = item.pop('cargo_buyer', None)
    if item['cargo_product']:
        # build Cargo sub-model
        item['cargo'] = {
            'product': may_strip(item.pop('cargo_product', None)),
            'movement': item.pop('cargo_movement', None),
            'volume': item.pop('cargo_volume', None),
            'volume_unit': item.pop('cargo_unit', None),
            'buyer': {'name': buyer} if buyer else None,
            'seller': {'name': seller} if seller else None,
        }

    return item


def grades_mapping():
    return {
        'port_name': ('port_name', may_strip),
        'berthed': ('berthed', lambda x: to_isoformat(x, dayfirst=False, yearfirst=True)),
        'eta': ('eta', lambda x: to_isoformat(x, dayfirst=False, yearfirst=True)),
        'departure': ('departure', lambda x: to_isoformat(x, dayfirst=False, yearfirst=True)),
        'arrival': ('arrival', lambda x: to_isoformat(x, dayfirst=False, yearfirst=True)),
        'vessel_name': ('vessel_name', may_strip),
        'vessel_imo': ('vessel_imo', lambda x: may_apply(x, float, int, str)),
        'vessel_length': ('vessel_length', lambda x: may_apply(x, float, int)),
        'vessel_dwt': ('vessel_dwt', lambda x: may_apply(x, float, int)),
        'cargo_product': ('cargo_product', may_strip),
        'cargo_movement': ('cargo_movement', may_strip),
        'cargo_volume': ('cargo_volume', may_strip),
        'cargo_unit': ('cargo_unit', lambda x: UNIT_MAPPING.get(x.lower(), x) if x else None),
        'provider_name': ('provider_name', None),
        'reported_date': (
            'reported_date',
            lambda x: to_isoformat(x, dayfirst=False, yearfirst=True),
        ),
        'cargo_seller': ('cargo_seller', may_strip),
        'cargo_buyer': ('cargo_buyer', may_strip),
    }
