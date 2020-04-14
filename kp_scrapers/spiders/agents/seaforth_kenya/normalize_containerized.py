import logging

from kp_scrapers.lib.date import is_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


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

    # vessel item
    item['vessel'] = {'name': item.pop('vessel_name', None)}
    if not item.get('vessel').get('name'):
        return

    buyer = item.pop('cargo_buyer', None)

    # get movement
    if item.get('cargo_volume_dis'):
        movement = 'discharge'
        volume = item.pop('cargo_volume_dis', None)
    if item.get('cargo_volume_load'):
        movement = 'load'
        volume = item.pop('cargo_volume_load', None)

    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': movement,
        'volume': volume,
        'volume_unit': Unit.tons,
        'buyer': {'name': buyer} if buyer and buyer not in ['?'] else None,
    }

    if not item['cargo'].get('buyer') or not item['cargo'].get('buyer').get('name'):
        item['cargo'].pop('buyer')

    return item


def field_mapping():
    return {
        'vessel': ('vessel_name', None),
        'arr': ('arrival', lambda x: x if is_isoformat(x) else None),
        'eta': ('eta', lambda x: x if is_isoformat(x) else None),
        'agency': ignore_key('irrelevant'),
        'shipping line': ignore_key('irrelevant'),
        'load port': ignore_key('irrelevant'),
        'origin': ignore_key('irrelevant'),
        'charterer/trader': ignore_key('irrelevant'),
        'exporter/shipper': ignore_key('irrelevant'),
        'exporter': ignore_key('irrelevant'),
        'importer': ('cargo_buyer', may_strip),
        'cargo type': ('cargo_product', None),
        'import tonnes': ('cargo_volume_dis', lambda x: x if is_number(x) else None),
        'export tonnes': ('cargo_volume_load', lambda x: x if is_number(x) else None),
        'remarks': ignore_key('irrelevant'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def is_number(check_number):
    try:
        float(check_number)
        return True
    except ValueError:
        return False
