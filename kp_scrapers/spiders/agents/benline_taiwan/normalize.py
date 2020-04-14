import logging

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    # build proper Cargo model
    seller = item.pop('cargo_seller', None)
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': item.pop('cargo_movement', None),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.tons,
        'seller': {'name': seller} if seller else None,
    }
    return item


def field_mapping():
    return {
        'country': ignore_key('country'),
        'location': ('port_name', may_strip),
        'vessel': ('vessel', lambda x: {'name': x}),
        'facility/terminal': ignore_key('redundant'),
        'cargo': ('cargo_product', None),
        'volume/mt': ('cargo_volume', may_strip),
        'eta': ('eta', None),
        'load/discharge': ('cargo_movement', lambda x: x.lower()),
        'receiver': ('cargo_seller', None),
        'origin': ignore_key('origin'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }
