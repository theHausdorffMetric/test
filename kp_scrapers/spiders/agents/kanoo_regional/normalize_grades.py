from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


UNIT_MAPPING = {'Metric Ton (MT)': Unit.tons}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # build cargo sub model
    item['cargo'] = {
        'product': may_strip(item.pop('cargo_product')),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.barrel if item['cargo_unit'] is None else item['cargo_unit'],
        'movement': None,
    }
    item.pop('cargo_unit', None)
    return item


def field_mapping():
    return {
        'CALLNO': ignore_key('redundant'),
        'Charterer Name': ignore_key('report does not provide movement or origin port'),
        'Responsible Party': ignore_key('redundant'),
        'Owner - Operator (Principal Name)': ignore_key('redundant'),
        'Vessel': ('vessel', lambda x: {'name': x}),
        'Arrival': ('arrival', None),
        'Departure': ('departure', None),
        'Cargo Type': ignore_key('redundant'),
        'Commodity': ('cargo_product', None),
        'Cargo Description': ('provider_name', None),
        'Quantity(Cargo)': ('cargo_volume', lambda x: None if x == '' else x),
        'Measurement Unit': ('cargo_unit', lambda x: None if x == '' else UNIT_MAPPING.get(x, x)),
        'Country Name': ignore_key('redundant'),
        'Port Name': ('port_name', None),
        'Next Port Name': ignore_key('redundant'),
        'Prev Port Name': ignore_key('redundant'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }
