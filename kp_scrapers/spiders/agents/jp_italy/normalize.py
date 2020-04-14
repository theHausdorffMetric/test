from itertools import zip_longest
import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip, split_by_delimiters, try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

STRING_BLACKLIST = ['N/A', 'TBA', 'TBC']
MOVEMENT_MAPPING = {
    'DISCHARGING': 'discharge',
    'TO DISCHARGE': 'discharge',
    'DISCHARGED': 'discharge',
    'LOADING': 'load',
    'TO LOAD': 'load',
    'LOADED': 'load',
}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.
    Args:
        raw_item (Dict[str, str]):
    Yields:
        Dict[str, str]:
    """
    item = map_keys(raw_item, grades_mapping())

    # discard items without vessel names
    vessel_name = item.pop('vessel_name')
    if not vessel_name:
        logger.warning(f'Item has no vessel name, discarding:\n{item}')
        return

    # discard items without valid dates
    if not (item.get('arrival') or item.get('departure')):
        logger.warning(f'Item has no valid portcall dates, discarding:\n{item}')
        return

    # build Vessel sub-model
    item['vessel'] = {'name': vessel_name, 'gross_tonnage': item.pop('gross_tonnage', None)}

    # build Cargo sub-model
    products, volumes = get_products_and_volumes(item)
    movement = item.pop('cargo_movement', None)
    for product, volume in zip_longest(products, volumes):
        item['cargo'] = {
            'product': product,
            'movement': movement,
            'volume': try_apply(volume, float, int),
            'volume_unit': Unit.tons,
        }

        yield item


def grades_mapping():
    return {
        'port_name': ('port_name', None),
        'Vessel Name': ('vessel_name', None),
        'GT': ('gross_tonnage', lambda x: try_apply(x, float, int)),
        'Gross tonn.': ('gross_tonnage', lambda x: try_apply(x, float, int)),
        'Date of arrival': ('arrival', normalize_pc_date),
        'Date of departure': ('departure', normalize_pc_date),
        'Arrival Date\n(can be the expected date) ': ('arrival', normalize_pc_date),
        'Departure Date': ('departure', normalize_pc_date),
        'PORT of \norigin': ('previous_zone', normalize_feature),
        'ORIGIN': ('previous_zone', normalize_feature),
        'PORT of destination': ('next_zone', normalize_feature),
        'DESTINATION': ('next_zone', normalize_feature),
        'Terminal': ('installation', normalize_feature),
        'Operation': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),
        'Cargo': ('cargo_product', split_feature),
        'Grade': ('cargo_grade', split_feature),
        'Quantity': ('cargo_volume', split_feature),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_reported_date),
    }


def normalize_feature(raw_feature):
    """Normalize a raw feature
    Args:
        raw_feature (str)
    Returns:
        str:
    Examples:
        >>> normalize_feature('N/A')
        >>> normalize_feature('VGO')
        'VGO'
    """
    if not raw_feature or raw_feature in STRING_BLACKLIST:
        return None
    return raw_feature


def split_feature(raw_feature):
    """Split a raw feature into multiple features.
    Args:
        raw_feature (str):
    Returns:
        List[str]:
    Examples:
        >>> split_feature('N/A')
        >>> split_feature('MDO+ULSD')
        ['MDO', 'ULSD']
        >>> split_feature('VGO')
        ['VGO']
    """

    raw_feature = normalize_feature(raw_feature)
    return split_by_delimiters(raw_feature, '+', '/')


def get_products_and_volumes(item):
    """Associate volumes to their related products
    :param item: dict
    :returns: List[str], List[str]
    Examples:
        >>> get_products_and_volumes({'cargo_grade': ['A', 'B'], 'cargo_product': ['C', 'D'],\
         'cargo_volume': ['1.000', '2.000']})
        (['A', 'B'], [1000.0, 2000.0])
        >>> get_products_and_volumes({'cargo_grade': None, 'cargo_product': ['C', 'D'],\
         'cargo_volume': ['1.000', '2.000']})
        (['C', 'D'], [1000.0, 2000.0])
        >>> get_products_and_volumes({'cargo_grade': None, 'cargo_product': ['C', 'D'],\
         'cargo_volume': ['3000']})
        (['C', 'D'], [])
        >>> get_products_and_volumes({'cargo_grade': None, 'cargo_product': None,\
         'cargo_volume': ['3000']})
        ([], [])
        >>> get_products_and_volumes({'cargo_grade': None, 'cargo_product': None,\
         'cargo_volume': None})
        ([], [])
    """
    grade = item.pop('cargo_grade', None)
    cargo = item.pop('cargo_product', None)
    volumes = item.pop('cargo_volume', None)

    products = grade if grade else cargo
    products = products if products else []
    volumes = volumes if volumes and products else []

    if volumes and products:
        volumes = [may_remove_substring(volume, '.') for volume in volumes]
        if len(products) == len(volumes):
            volumes = [float(volume) for volume in volumes]
        else:
            volumes = []

    return products, volumes


def normalize_pc_date(date_str):
    """Normalize raw port-call date into an ISO8601-formatted string.
    This function WILL discard timezone data.
    Args:
        date_str (str):
    Returns:
        str: ISO8601-formatted date string
    Examples:
        >>> normalize_pc_date('3/16/2020')
        '2020-03-16T00:00:00'
    """
    if not may_strip(date_str) or any(sub in date_str for sub in STRING_BLACKLIST):
        return None

    return to_isoformat(date_str, dayfirst=False)


def normalize_reported_date(raw_date):
    """Normalize raw reported date into an ISO8601-formatted string.
    This function WILL discard timezone data.
    Args:
        raw_date (str):
    Returns:
        str: ISO8601-formatted date string
    Examples:
        >>> normalize_reported_date('Fri, 5 Jul 2018 17:15:23 +0200')
        '2018-07-05T17:15:23'
    """
    if not may_strip(raw_date) or any(sub in raw_date for sub in STRING_BLACKLIST):
        return None

    return to_isoformat(raw_date) if raw_date else None
