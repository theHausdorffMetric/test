import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


UNIT_MAPPING = {'MT': Unit.tons, 'KB': Unit.barrel}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, grades_mapping(), skip_missing=True)
    # completely disregard cancelled vessel movements, or those without valid eta and departure
    if item['cancelled'] or not (item['eta'] or item['departure']):
        return

    # remove vessels that have not been named
    if 'TBN' in item['vessel_name']:
        return

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}

    # discard items with no specified cargoes onboard
    if not item['cargo_product']:
        return

    # because the volume/product column may be swapped, we need to check before we normalize
    # the product/volume individually
    if item['cargo_product'][0].isdigit():
        item['cargo_volume'], item['cargo_product'] = item['cargo_product'], item['cargo_volume']

    volume, unit = normalize_cargo_volume(item['cargo_volume'])
    product = item.pop('cargo_product', None)
    # discard irrelevant fields
    for field in ('cargo_volume', 'cancelled'):
        item.pop(field, None)
    if product:
        list_of_products = split_product(product, volume)
        for _i_prod in list_of_products:
            item['cargo'] = {
                'product': _i_prod[0],
                'movement': item.pop('cargo_movement', None),
                'volume': str(_i_prod[1]),
                'volume_unit': unit,
            }
            yield item


def grades_mapping():
    return {
        'B/L DATE': (ignore_key('irrelevant')),
        'CHARTERER': (ignore_key('irrelevant')),
        'ETA': ('eta', normalize_portcall_date),
        'ETS': ('departure', normalize_portcall_date),
        'GRADE': ('cargo_product', None),
        'GRADE ': ('cargo_product', None),
        'IMO': ('vessel_imo', lambda x: try_apply(x, float, int, str)),
        'IMO NUMBER': ('vessel_imo', lambda x: try_apply(x, float, int, str)),
        'LOAD/DISCHARGE': ('cargo_movement', lambda x: x.lower()),
        'NEXT PORT': (ignore_key('irrelevant')),
        'PORT': ('port_name', None),
        'PREVIOUS PORT': (ignore_key('irrelevant')),
        'provider_name': ('provider_name', None),
        'QUANTITY': ('cargo_volume', None),
        'REMARK': (ignore_key('irrelevant')),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
        'STATUS': ('cancelled', lambda x: 'cancel' in x),
        'SUPPLIER': (ignore_key('irrelevant')),
        'VESSEL': ('vessel_name', None),
    }


def normalize_cargo_volume(raw_volume):
    """Normalize raw cargo volume to a valid volume and volume_unit.

    Args:
        raw_volume (str):

    Returns:
        Tuple[str, str]: tuple of (volume, volume_unit)

    Examples:
        >>> normalize_cargo_volume('9000 MT')
        ('9000', 'tons')
        >>> normalize_cargo_volume('600 KB +/-5%')
        ('600000', 'barrel')
        >>> normalize_cargo_volume('')
        (None, None)
        >>> normalize_cargo_volume('300 KN')
        ('300', None)
    """
    if not raw_volume:
        return None, None

    # obtain unit from string
    for alias in UNIT_MAPPING:
        if alias in raw_volume:
            unit = UNIT_MAPPING[alias]
            break
        else:
            unit = None

    # special volume transformation for `KB -> barrel` unit
    return raw_volume.split()[0] + ('000' if unit == 'barrel' else ''), unit


def normalize_portcall_date(raw_date):
    """Normalize raw portcall date to an ISO-8601 formatted string.

    Args:
        raw_date (str):

    Returns:
        str | None:

    Examples:
        >>> normalize_portcall_date('AM/08.01.18')
        '2018-01-08T06:00:00'
        >>> normalize_portcall_date('PM/03.01.18')
        '2018-01-03T18:00:00'
        >>> normalize_portcall_date('1930/12.07.18')
        '2018-07-12T19:30:00'
        >>> normalize_portcall_date('NOON/18.07.18')
        '2018-07-18T12:00:00'
        >>> normalize_portcall_date('24.08.18')
        '2018-08-24T00:00:00'
        >>> normalize_portcall_date('')
        >>> normalize_portcall_date('UNDER BERTHING')

    """
    if not raw_date or not has_numbers(raw_date):
        return None

    # map arbitrary time designations into something consistent
    time_map = {'AM': '0600', 'PM': '1800', 'NOON': '1200'}

    time, _, date = raw_date.partition('/')
    return to_isoformat(' '.join((date, time_map.get(time, time))), dayfirst=True)


def has_numbers(input):
    """Check if input string contains any numeric characters.

    TODO could be made generic

    Args:
        input (str):

    Returns:
        bool: True if at least one numeric char

    """
    return any(char.isdigit() for char in input)


def split_product(raw_product, raw_volume):
    """split multiple products

    Args:
        raw_product (str):
        raw_volume (str):

    Returns:
        List[str, str]:

    """
    vol_list = []
    if '/' in raw_product:
        product_list = raw_product.split('/')
        vol = int(raw_volume) / len(product_list) if raw_volume else None
        while len(vol_list) < len(product_list):
            vol_list.append(vol)

        return list(zip(product_list, vol_list))
    return zip([raw_product], [raw_volume])
