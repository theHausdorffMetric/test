import logging

from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.units import Unit


logger = logging.getLogger(__name__)


# voyage ref code
VOYAGE_TO_PORT_MAPPING = {
    'BA': 'Baltimore',
    'BO': 'Boston',
    'CC': 'Corpus Christi',
    'FL': 'Everglades',
    'FP': 'Freeport Texas',
    'GA': 'Texas City',
    'HO': 'Houston',
    'LB': 'Long Beach',
    'LC': 'Lake Charles',
    'NH': 'New Haven',
    'NO': 'New Orleans',
    'NY': 'New York',
    'PA': 'Philadelphia',
    'PO': 'Portland (Maine)',
    'PT': 'Port Arthur',
    'RI': 'Providence',
    'SC': 'Savannah',
    'SF': 'San Francisco',
}


# berth full names
BERTH_TO_PORT_MAPPING = {
    'BUCKEYE': 'Charleston',
    'BUCKEYE JACKSONVILLE': 'Jacksonville',
    'CENTERPOINT': 'Jacksonville',
    'CHEVRON PASCAGOULA': 'Pascagoula',
    'GALVESTON LIGHTERING AREA': 'Galveston Light.',
    'KINDER MORGAN SPARROWS POINT': 'Baltimore',
    'KMI 4': 'Charleston',
    'MAGELLAN WILMINGTON': 'Wilmington',
    'REK 223': 'Tampa',
    'SEAPORT CANAVERAL': 'Port Canaveral',
    'SHELL BLAKELEY MOBILE': 'Mobile',
    'SPRAGUE SEARSPORT': 'Searsport',
    'STAPLETON ANCHORAGE': 'Stapleton Light.',
    'WEBBER TANKS': 'Bucksport',
}


MOVEMENT_MAPPING = {'D': 'discharge', 'L': 'load'}


def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # completely disregard rows with no date
    if not item['berthed']:
        return

    # build Vessel model
    item['vessel'] = {'name': item.pop('vessel_name', None), 'imo': item.pop('vessel_imo', None)}

    # normalize port name
    if not item.get('port_name'):
        item['port_name'] = normalize_port(item)

    # prioritise bol quantity but sometimes it can be empty, if so we use gross quantity
    item['cargo_volume'] = item.get('quantity_bol') or item.get('quantity_gross')
    movement = item.pop('cargo_movement', None)
    cargoes = normalize_product(item['cargo_product'], item['cargo_volume'])

    # discard irrelevant keys
    for key in ('voyage', 'quantity_gross', 'quantity_bol', 'cargo_product', 'cargo_volume'):
        item.pop(key, None)

    for product, volume in cargoes:
        item['cargo'] = {
            'product': product,
            'volume': volume,
            'movement': movement,
            'volume_unit': Unit.barrel,
        }
        yield item


def field_mapping():
    return {
        'Berth Short Name': ('berth', None),
        'BL Date': ('berthed', None),  # OIL
        'BL Quantity': ('quantity_bol', None),
        'Cargo Short Name': ('cargo_product', None),
        'Commence Date Local': ('berthed', None),  # CPP
        'IMO No': ('vessel_imo', None),
        'Ops': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),  # OIL
        'provider_name': ('provider_name', None),
        'Port Func': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),  # CPP
        'reported_date': ('reported_date', None),
        'Ship Gross': ('quantity_gross', None),
        'Vessel Name': ('vessel_name', None),
        'Voyage Reference': ('voyage', None),
        'Port Name': ('port_name', None),
        'Complete Date Local': ('berthed', None),
    }


def normalize_port(item):
    """Normalize port name depending on the voyage reference code and berth name provided.

    Args:
        item (Dict[str, str]):

    Returns:
        str: mapped port name

    """
    # first, take voyage reference code to map to ports
    for voyage, port in VOYAGE_TO_PORT_MAPPING.items():
        if voyage in item['voyage']:
            return port
    logger.warning(f'Unknown voyage code: {item["voyage"]}')

    # if voyage-port mapping not found, use berth name instead
    for berth, port in BERTH_TO_PORT_MAPPING.items():
        if berth in item['berth']:
            return port
    logger.warning(f'Unknown berth name: {item["berth"]}')

    # sane fallback of using berth name since it's more descriptive
    return item['berth']


def normalize_product(product_str, qty_str):
    """Normalize product string into 1 or more products and corresponding quantities

    Assign normally if a qty is given for each product.
    There are cases where there are more products than quantities,
    we divide that qty by the number of products.
    (Does not work for rare case: k products, n quantities, where k > n and n > 1)

    Examples:
        >>> normalize_product('RBOB/CBOB', '45639.02')
        [('RBOB', 22819.51), ('CBOB', 22819.51)]

    """
    products = [may_strip(product) for product in _extract_product_name(product_str).split('/')]
    quantities = [try_apply(quantity, may_strip, float) for quantity in str(qty_str).split('/')]
    if len(products) != len(quantities):
        quantities = [quantities[0] / len(products)] * len(products)
    return list(zip(products, quantities))


def _extract_product_name(product_str):
    """Removes extra information from product string such as '+15' and 'ADD LUB' from
    'ULSD +15 ADD LUB'.

    Judging from pattern seen thus far, we assume that:
    - if there are more than 2 cargo types, no extra info would be provided.
        E.g. 'RBOB 15.0 / CBOB 15.0' never occurs, only 'RBOB/CBOB'

    Examples:
        >>> _extract_product_name('LSHO+15 (NO ADDITIVES)')
        'LSHO'
        >>> _extract_product_name('ULSD -16 NO LUB')
        'ULSD '
        >>> _extract_product_name('LSHO (NO DYE/NO LUB)')
        'LSHO '
        >>> _extract_product_name('RBOB 15.0')
        'RBOB '
        >>> _extract_product_name('DSL #1 LS')
        'DSL #1 LS'

    """
    # remove everything from first bracket as they are extra info
    raw_product = product_str.split('(')[0]

    # do not split DSL #1 LS
    if 'DSL #' in raw_product:
        return raw_product

    for each in raw_product:
        # only take product str before the extra info of the form: '+15', '-22', '15.0'
        if each in '+-' or each.isdigit():
            return raw_product.split(each)[0]

    return raw_product
