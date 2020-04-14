from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, grades_mapping())
    # discard vessels if it's yet to be named (TBN)
    if not item['vessel']:
        return

    # build Cargo sub-model
    item['cargo'] = {
        'product': item.pop('cargo_product'),
        # reports are formatted such that if no cargo movement is specified, it is an export
        'movement': item.pop('cargo_movement', 'load'),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.barrel,
    }

    return item


def grades_mapping():
    return {
        'Cargo': ('cargo_movement', lambda x: 'discharge' if 'import' in x.lower() else 'load'),
        'Cargo No': ('cargo_movement', lambda x: 'discharge' if 'import' in x.lower() else 'load'),
        'Cargo No.': ('cargo_movement', lambda x: 'discharge' if 'import' in x.lower() else 'load'),
        'Charterer': ignore_key('charterer'),
        'Charterers': ignore_key('charterer'),
        'Date': ('arrival', lambda x: to_isoformat(x, dayfirst=True)),
        'Dates': ('arrival', lambda x: to_isoformat(x, dayfirst=True)),
        'Grade': ('cargo_product', None),
        'Next Port': ignore_key('next port'),
        'Notes': ignore_key('remarks'),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
        # cargo unit is in kilobarrels
        'QTY': ('cargo_volume', lambda x: x + '000' if x else None),
        'Quantity': ('cargo_volume', lambda x: x + '000' if x else None),
        'reported_date': ('reported_date', None),
        'Shipper': ignore_key('irrelevant'),
        'Shipper/Receiver': ignore_key('irrelevant'),
        'Shipper/Receivers': ignore_key('irrelevant'),
        'Supp/Rcvr': ignore_key('irrelevant'),
        'Supplier': ignore_key('irrelevant'),
        'Vessel': ('vessel', lambda x: {'name': x} if 'TBN' not in x else None),
    }
