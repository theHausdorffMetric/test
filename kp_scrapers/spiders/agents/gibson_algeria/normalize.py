from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if not item['vessel']:
        return

    # build cargo sub-model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume'),
        'volume_unit': Unit.kilotons,
    }

    return item


def field_mapping():
    return {
        'N◦': (ignore_key('order number')),
        'VESSEL': ('vessel', normalize_vessel),
        'ETA': ('eta', normalize_date),
        'ARRIVED': ('arrival', normalize_date),
        'ETC/ETS': ('berthed', normalize_date),
        'PRODUCT': ('cargo_product', None),
        'SBCO': ('cargo_product', None),
        'QUANTITY': ('cargo_volume', None),
        'JETTY': (ignore_key('jetty')),
        'DATE': ('departure', normalize_date),
        'DEST': ('next_zone', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'port_name': ('port_name', None),
    }


def normalize_vessel(vessel_name):
    """Normalize vessel.

    Args:
        vessel_name (str):

    Returns:
        Dict[str, str]:

    """
    vessel_name = vessel_name.replace('*', '')
    if 'TBN' not in vessel_name:
        return {'name': vessel_name}


def normalize_date(raw_date):
    """Normalize date.

    Date comes with multiple formats:
        - 15.12.18 1700
        - 21.12.18 AM
        - 17.12.18

    Examples:
        >>> normalize_date('15.12.18 1700')
        '2018-12-15T17:00:00'
        >>> normalize_date('21.12.18 AM')
        '2018-12-21T00:00:00'
        >>> normalize_date('17.12.18')
        '2018-12-17T00:00:00'

    Args:
        raw_date (str):

    Returns:

    """
    date_str = raw_date.replace('AM', '').replace('PM', '')
    return to_isoformat(date_str, dayfirst=True)
