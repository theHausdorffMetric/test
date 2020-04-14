from itertools import zip_longest

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


GHANA_ABIDJAN_PORT = 'ABIDJAN'

PORT_INSTALLATION_MAPPING = {
    # special mapped installations
    'ATLAS COVE': ('Lagos', 'Atlas Cove'),
    'FOLAWIYO JETTY': ('Lagos', 'Folawiyo'),
    'NEW OIL JETTY (NOJ)': ('Lagos', 'NOJ Apapa'),
    'WOSBAB JETTY': ('Lagos', 'AO Apapa'),
    # generic ports
    'AKWA IBOM': ('Nigeria', None),
    'CALABAR': ('Calabar', None),
    'ESCRAVOS': ('Escravos', None),
    'HARCOURT': ('Harcourt', None),
    'LAGOS': ('Lagos', None),
    'WARRI': ('Warri', None),
}

GHANA_ABIDJAN_MAPPING = {
    'ABB TEMA': ('Tema', 'Tema'),
    'OIL BERTH': ('Ghana', None),
    'SBM': ('Tema', 'Tema'),
    'SIR 11': ('Abidjan', 'Takoradi'),
    'TAKORADI ANCH': ('Takoradi Anch', None),
    'TAKORADI BERTH': ('Aboadze', 'Takoradi'),
    'TEMA ANCH': ('Tema Light.', None),
}

# do not process items from these ports
IRRELEVANT_PORTS = ['STATUS OF PPMC SHUTTLES', 'VSLS PROGRAMMED FOR STS', 'VSLS AWAITING PROGRAMME']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # map special ghana/abidjan ports
    if GHANA_ABIDJAN_PORT in (item['port_installation'][0] or ''):
        item['port_installation'] = map_ghana_abidjan_port_and_installation(
            item['port_installation'][0], item['situation']
        )

    # discard irrelevant items as denoted by their port names (c.f. analysts)
    if item['port_installation'] == (None, None):
        return

    # properly map the port and installation in the item
    item['port_name'], item['installation'] = item['port_installation']

    # construct `eta` based on this priority: ETB >> ETA >> ETD (c.f. analysts)
    item['eta'] = item['etb'] or item['eta'] or item['etd']

    # separate multiple products for each item
    cargoes = list(build_cargo(item))

    # remove irrelevant fields
    for key in [
        'etb',
        'etd',
        'port_installation',
        'situation',
        'cargo_product',
        'cargo_movement',
        'cargo_volume',
    ]:
        item.pop(key, None)

    for cargo in cargoes:
        item['cargo'] = cargo
        yield item


def build_cargo(item):
    """Build a Cargo model.

    This function will split the cargo into multiples if there is the presence of a "/" delimiter.

    Args:
        item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    cargoes = zip_longest(
        (item['cargo_product'] or '').split('/'), (item['cargo_volume'] or '').split('/')
    )
    for single in cargoes:
        yield {
            'product': single[0],
            'movement': item.get('cargo_movement', None),
            'volume': single[1] if single[1] else None,
            # all cargo quantities are expressed as metric tons in this report
            'volume_unit': Unit.tons if single[1] else None,
        }

    # discard temporary fields
    for field in ('cargo_product', 'cargo_movement', 'cargo_volume'):
        item.pop(field, None)


def field_mapping():
    return {
        'ARRVD\n[ETA]': ('eta', normalize_date),
        'CARGO': ('cargo_product', normalize_product),
        'C  A  R  G  O': ('cargo_product', normalize_product),
        'CHARTERERS/\nRECEIVERS': (ignore_key('not required for grades spider')),
        'ETB': ('etb', normalize_date),
        'port_name': ('port_installation', map_raw_port_and_installation),
        'POSITION': ('situation', None),
        'provider_name': ('provider_name', None),
        'QTY\n[MT]': ('cargo_volume', normalize_quantity),
        'REMARKS': ('cargo_movement', normalize_movement),
        'reported_date': ('reported_date', normalize_date),
        'SAILED\n[ETD]': ('etd', normalize_date),
        'SHIP’S NAME': ('vessel', lambda x: {'name': may_strip(x)}),
    }


def map_raw_port_and_installation(raw_port):
    """Map a raw combined port/installation string to a tuple of port and installation.

    Args:
        raw_port (str):

    Returns:
        Tuple[str | None, str | None]:

    """
    if any(irrelevant in raw_port for irrelevant in IRRELEVANT_PORTS):
        return (None, None)

    for alias in PORT_INSTALLATION_MAPPING:
        if alias in raw_port:
            return PORT_INSTALLATION_MAPPING[alias]

    return (raw_port, None)


def map_ghana_abidjan_port_and_installation(raw_port, raw_situation):
    """Map a raw situation string to a tuple of port and installation.

    Because the data for Ghana and Abidjan ports are combined together, we need to
    disambiguate them using the "situation" raw string.

    Args:
        raw_port (str):
        raw_situation (str):

    Returns:
        Tuple[str | None, str | None]:

    """
    for alias in GHANA_ABIDJAN_MAPPING:
        if alias in raw_situation:
            return GHANA_ABIDJAN_MAPPING[alias]

    return (None, None)


def normalize_movement(raw_movement):
    """Normalize remarks into a valid cargo movement.

    If input string contains 'load ', movement should be 'load'.
    We ignore 'loading ex'.

    Examples:
        >>> normalize_movement('Awtg Terminal readiness to load ex WRPC')
        'load'
        >>> normalize_movement('Ex Ashabi, Yet to arrive for discharge at Pinnacle Jetty')
        'discharge'
        >>> normalize_movement('Yet to arive for loading ex Okrika refinery')

    """
    if 'loading ex' in raw_movement:
        return None

    if 'load' in raw_movement:
        return 'load'

    if 'discharg' in raw_movement:
        return 'discharge'

    return None


def normalize_date(raw_date):
    """Normalize a raw date.

    Args:
        raw_date (str):

    Returns:
        str:

    """
    return to_isoformat(raw_date, dayfirst=True, yearfirst=False)


def normalize_product(raw_product):
    """Normalize a raw cargo quantity value.

    Args:
        raw_product (str):

    Returns:
        str:

    """
    result = raw_product
    for char_to_remove in ('\n', ' '):
        result = raw_product.replace(char_to_remove, '')

    return result


def normalize_quantity(raw_quantity):
    """Normalize a raw cargo quantity value.

    Args:
        raw_quantity (str):

    Returns:
        str:

    Examples:
        >>> normalize_quantity('5000')
        '5000'
        >>> normalize_quantity('5,000')
        '5000'
        >>> normalize_quantity('5,000 ROB')
        '5000'
        >>> normalize_quantity('20,860/ 5000')
        '20860/5000'
        >>> normalize_quantity('5,000 ROB/\\n20,860 ROB')
        '5000/20860'
        >>> normalize_quantity('5,000/ TBC')
        '5000/'
        >>> normalize_quantity('INBALLAST')

    """
    result = ''
    for char in raw_quantity:
        if char.isdigit() or char == '/':
            result += char

    return result if result else None
