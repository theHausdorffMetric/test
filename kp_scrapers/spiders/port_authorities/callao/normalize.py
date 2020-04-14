import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, split_by_delimiters, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

PRODUCT_BLACKLIST = ['CARGA', 'PASAJEROS']
MOVEMENT_MAPPING = {'D': 'discharge', 'E': 'load'}
VESSEL_NAME_BLACKLIST = ['MANTENIMIENTO']
VESSEL_TYPE_BLACKLIST = ['CONTAINERO', 'MULTIPROPOSITO', 'PESQUERO', 'RORO']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard blacklisted vessels and vessels with irrelevant cargoes
    if not item['vessel_name'] or not item.pop('vessel_type') or not item.get('cargo_product'):
        return

    # build proper vessel submodel
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length')}

    # build proper cargoes submodel
    item['cargoes'] = [
        {'product': product, 'movement': item.get('cargo_movement')}
        for product in item.pop('cargo_product')
    ]
    item.pop('cargo_movement', None)

    # normalize ETA
    item['eta'] = normalize_eta(item.get('eta'), item.pop('month'), item.pop('year'))

    return item


def portcall_mapping():
    return {
        'ETA': ignore_key('use more accurate ETB as ETA estimate instead'),
        'SHIPS': ('vessel_name', clean_vessel_name),
        'ETB': ('eta', None),
        'BERTH': ('berth', None),
        'LOA / BEAM': ('vessel_length', lambda x: try_apply(x.split('/')[0], float, int)),
        'TYPE': ('vessel_type', lambda x: None if x in VESSEL_TYPE_BLACKLIST else x),
        'OPERATOR': ('shipping_agent', None),
        'LINE': ignore_key('irrelevant'),
        'CARGO': ('cargo_product', normalize_cargo_product),
        'QUANTITY': ('cargo_movement', normalize_cargo_movement),
        'PILOT': ignore_key('irrelevant'),
        'TUG': ignore_key('irrelevant'),
        'LAST PORT': ignore_key('previous port'),
        'NEXT PORT': ignore_key('next port'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'port_name': ('port_name', None),
        'month': ('month', None),
        'year': ('year', None),
    }


def clean_vessel_name(raw_name):
    """Cleanup and normalize raw vessel name.

    Occasionally, vessel name may have parantheses in them which we don't want.
    This function removes those parantheses.

    This function will also remove known blacklisted vessel names.

    Args:
        raw_name (str):

    Returns:
        str | None:

    Examples:
        >>> clean_vessel_name('HIGH PROGRESS')
        'HIGH PROGRESS'
        >>> clean_vessel_name('BUNGA LOTUS (V)')
        'BUNGA LOTUS'
        >>> clean_vessel_name('MANTENIMIENTO')
        >>> clean_vessel_name('MANTENIMIENTO (1)')
    """
    name = may_strip(raw_name.split('(')[0])
    return None if name in VESSEL_NAME_BLACKLIST else name


def normalize_eta(raw_eta, month, year):
    """Normalize raw eta date to an ISO-8601 compatible string.

    Args:
        raw_eta (str):
        month (int):
        year (int):

    Returns:
        str: ISO-8601 compatible string

    Examples:
        >>> normalize_eta('010830', 8, 2018)
        '2018-08-01T08:30:00'
        >>> normalize_eta(')231500', 9, 2018)
        '2018-09-23T15:00:00'

    """
    eta_pattern = r'\d{6}'
    res = re.search(eta_pattern, raw_eta)
    if res:
        eta = res.group()
        day, time = eta[:2], eta[2:]
        try:
            return to_isoformat(f'{day}/{month}/{year} {time}', dayfirst=True)
        except ValueError:
            return to_isoformat(f'{int(day) - 1}/{month}/{year} {time}', dayfirst=True)

    else:
        logger.warning(f'Eta date is invalid: {raw_eta}')


def normalize_cargo_product(raw_product):
    """Normalize cargo product from raw product string.

    TODO confirm with analysts how we want to normalize the volumes,
    since they are not structured at all, and it will be complex to normalize.

    Args:
        raw_product (str):

    Returns:
        List[str]: empty list if name contains blacklisted product strings

    Examples:
        >>> normalize_cargo_product('METHANOL')
        ['METHANOL']
        >>> normalize_cargo_product('GASOLINA 95 & 90 & TURBO A1')
        ['GASOLINA 95', '90', 'TURBO A1']
        >>> normalize_cargo_product('TRIGO GRANEL + ALIMENTO PROCE.')
        ['TRIGO GRANEL', 'ALIMENTO PROCE.']
    """
    if any(alias in raw_product for alias in PRODUCT_BLACKLIST):
        return []

    if any(d in raw_product for d in ('+', '&', '-', ',')):
        return [may_strip(each) for each in split_by_delimiters(raw_product, '+', '&', '-', ',')]

    return [raw_product]


def normalize_cargo_movement(raw_movement):
    """Normalize cargo movement from raw volume string.

    TODO confirm with analysts how we want to normalize the volumes,
    since they are not structured at all, and it will be complex to normalize.

    Args:
        raw_movement (str):

    Returns:
        str | None:

    Examples:
        >>> normalize_cargo_movement('D/ 33.550.00 TM')
        'discharge'
        >>> normalize_cargo_movement('E/ 13367.2 MT & 1286 PQTS')
        'load'
        >>> normalize_cargo_movement('TBC')
    """
    for movement in MOVEMENT_MAPPING:
        if raw_movement.startswith(movement):
            return MOVEMENT_MAPPING[movement]

    return None
