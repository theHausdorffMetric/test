import logging

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

PORT_MAPPING = {'S LOUISIANA': 'NEW ORLEANS'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # completely disregard rows with no departure date
    if not item['departure']:
        return

    item['cargo'] = {
        'product': may_strip(item.pop('cargo_product')),
        # source provides only export data, hence `load`
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        # source uses only metric tons (MT)
        'volume_unit': Unit.tons,
    }
    return item


def field_mapping():
    return {
        'Carrier': ignore_key('redundant'),
        'Commodity': ('cargo_product', None),
        'Departure Date': ('departure', None),
        'Destination Country': ignore_key('redundant'),
        'Destination Region': ignore_key('redundant'),
        'Destination Port': ignore_key('redundant'),
        'Port of Departure': ('port_name', lambda x: PORT_MAPPING.get(x, x)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Shipper': ignore_key('redundant'),
        'Vessel Name': ('vessel', lambda x: {'name': x}),
        'Weight (MT)': ('cargo_volume', None),
    }
