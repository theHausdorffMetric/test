import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_PRODUCTS = ['container', 'miscellaneous', 'passenger', 'vacant', 'project cargo']


VESSEL_TYPE_MAPPING = {
    'LIQUID VESSEL': 'Chemical/Oil Products Tanker',
    'BULK VESSELS': 'Bulk Carrier',
}


MOVEMENT_MAPPING = {'I': 'discharge', 'E': 'load'}


@validate_item(PortCall, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, portcall_mapping())

    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'type': item.pop('vessel_type', None) if item.get('vessel_type') else None,
    }

    if not item['vessel']['name']:
        return

    # build Cargo sub-model
    _cargo = item.get('cargo_product') or ''
    if not any(_cargo.lower() in each for each in IRRELEVANT_PRODUCTS):
        pass

    item['cargoes'] = [
        {
            'product': item.pop('cargo_product', None),
            'movement': item.pop('cargo_movement', None),
            'volume': item.pop('cargo_volume', None),
            'volume_unit': Unit.tons,
        }
    ]
    return item


def portcall_mapping():
    return {
        'Berth': ('berth', None),
        'VCN No.': ignore_key('irrelevant'),
        'Vessels name': ('vessel_name', normalize_vessel_name),
        'Cargo': ('cargo_product', None),
        'Cargo Type': ('vessel_type', lambda x: VESSEL_TYPE_MAPPING.get(x, x)),
        'Qty in MT': ('cargo_volume', None),
        'Qty MT': ('cargo_volume', None),
        'Type': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x, x)),
        'Import Export': ('cargo_movement', None),
        'IEB': ('cargo_movement', None),
        'Agent': ('shipping_agent', None),
        'Vessel Agent': ('shipping_agent', None),
        'Vessels Agent': ('shipping_agent', None),
        'ETD': ('departure', lambda x: to_isoformat(x, dayfirst=False)),
        'ETA': ('eta', lambda x: to_isoformat(x, dayfirst=False)),
        'Anch': ('eta', lambda x: to_isoformat(x, dayfirst=False)),
        'PCS Anchored': ignore_key('irrelevant'),
        'PCS': ignore_key('irrelevant'),
        'Pilot Request Time': ignore_key('irrelevant'),
        'Remark': ignore_key('irrelevant'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel_name(raw_name):
    """Normalize vessel name.

    Args:
        raw_name:

    Returns:

    """
    if raw_name.startswith('MT ') or raw_name.startswith('MV '):
        return raw_name[3:]

    return raw_name
