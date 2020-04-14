import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import split_by_delimiters
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_CARGO = ['ballast', 'cont', 'f/fish', 'g/cargo', 'inballast']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transfer raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping())
    # discard None vessels
    if not item['vessel_name']:
        return

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length')}

    # discard vessels movements without ETA
    if not item.get('eta') and not item.get('berthed') and not item.get('arrival'):
        return

    # build Cargoes sub-model
    cargo_volume, cargo_product = item.pop('cargo_volume', []), item.pop('cargo_product')
    if cargo_product is None:
        return item
    elif cargo_volume is None or (len(cargo_volume) != len(cargo_product)):
        item['cargoes'] = [{'product': product} for product in cargo_product]
    else:
        item['cargoes'] = [
            {'product': product, 'volume': volume, 'volume_unit': Unit.tons}
            for product, volume in zip(cargo_product, cargo_volume)
        ]
    return item


def portcall_mapping():
    return {
        'Ship': ('vessel_name', normalize_vessel_name),
        'Vessel Name': ('vessel_name', normalize_vessel_name),
        'Terminal': ('berth', None),
        'Berth': ('berth', None),
        'Expected Time (ETA)': ('eta', normalize_date),
        'Length(M)': (
            'vessel_length',
            lambda x: None if x is None or not float(x) > 0 else int(float(x)),
        ),
        'Agent': ignore_key('shipping agent'),
        'Cargo': ('cargo_product', normalize_cargo_product),
        'Tonnage': ('cargo_volume', normalize_cargo_volume),
        'Berth Date': ('berthed', normalize_date),
        'ETD': ('eta', normalize_date),
        'Rotation': ignore_key('irrelevant'),
        'Comm': ('cargo_product', normalize_cargo_product),
        'Ship to Follow': ignore_key('next vessel'),
        'reported_date': ('reported_date', None),
        'port_name': ('port_name', normalize_port),
        'provider_name': ('provider_name', None),
        'Location': ignore_key('location'),
        'Date of Arrival': ('arrival', normalize_date),
    }


def normalize_date(raw_date):
    """Normalize raw date to an ISO8601 compatible timestamp.

    Raw dates can be in two different format:
    example 1 : 23/02/20 AM
    example 2 : Sun, March 15, 2020 13:45 PM

    Args:
        raw_date (str): raw date string

    Returns:
        str: ISO8601 formatted timestamp

    Examples:
        >>> normalize_date('23/02/20 AM')
        '2020-02-23T00:00:00'
        >>> normalize_date('Sun, March 15, 2020 13:45 PM')
        '2020-03-15T00:00:00'
    """
    if raw_date is None:
        return None
    elif '/' in raw_date:
        raw_date = re.split(r'([^0-9\/]+)', raw_date)[0]
    else:
        raw_date = raw_date.split(':')[0][:-2]
    return to_isoformat(raw_date)


def normalize_port(raw_port):
    """Normalize port

    Args:
        raw_port (str): raw date string

    Returns:
        str:

    Examples:
        >>> normalize_port('ApapaA')
        'Lagos'
        >>> normalize_port('TincanA')
        'Lagos'
        >>> normalize_port('CalabarA')
        'Calabar'
        >>> normalize_port('WarriA')
        'Warri'
        >>> normalize_port('OnneA')
        'Onne'
        >>> normalize_port('RiversA')
        'Rivers'
    """
    if 'Apapa' in raw_port or 'Tincan' in raw_port:
        return 'Lagos'
    elif 'Calabar' in raw_port:
        return 'Calabar'
    elif 'Onne' in raw_port:
        return 'Onne'
    elif 'Rivers' in raw_port:
        return 'Rivers'
    elif 'Warri' in raw_port:
        return 'Warri'
    else:
        return None


def normalize_vessel_name(raw_name):
    """Normalize and cleanup raw vessel name.

    Raw string may contain useless characters like "◎", "*" or "~" that are not required.

    Args:
        raw_name (str):

    Returns:
        str:

    Examples:
        >>> normalize_vessel_name('EVANGELIA')
        'EVANGELIA'
        >>> normalize_vessel_name('-')
        >>> normalize_vessel_name('KAI-EI')
        'KAI-EI'
        >>> normalize_vessel_name('VACANT')

    """

    return None if (raw_name is None or raw_name == '-' or 'VACANT' in raw_name) else raw_name


def normalize_cargo_product(raw_product):
    """Normalize and cleanup cargo product name.

    Product names can often be listed with a category prefix,
    e.g. `B/WHEAT` representing Bulk Wheat

    Args:
        raw_product (str):

    Returns:
        List[str]:

    Examples:
        >>> normalize_cargo_product('STEEL PRODUCT')
        ['STEEL PRODUCT']
        >>> normalize_cargo_product('B/WHEAT')
        ['WHEAT']
        >>> normalize_cargo_product('AGO+JET A1')
        ['AGO', 'JET A1']
        >>> normalize_cargo_product('G/CARGO')

    """
    if raw_product is None:
        return None
    for cargo in IRRELEVANT_CARGO:
        if cargo in raw_product.lower():
            return None

    cleaned_product = raw_product.partition('/')[2] if '/' in raw_product else raw_product
    return split_by_delimiters(cleaned_product, '+')


def normalize_cargo_volume(raw_volume):
    """Normalize and cleanup cargo volume.

    Args:
        raw_volume (str):

    Returns:
        List[str]:

    Examples:
        >>> normalize_cargo_volume('34969.245MTS')
        ['34969.245']
        >>> normalize_cargo_volume('11600MT+15000')
        ['11600', '15000']
        >>> normalize_cargo_volume('000')

    """
    if raw_volume is None:
        return None
    cleaned_volume = raw_volume
    for unclean_str in ['MTS', 'MT', 'FCL', 'UNITS']:
        cleaned_volume = cleaned_volume.replace(unclean_str, '')
    cleaned_volume = cleaned_volume.replace(',', '.')
    if cleaned_volume.isdigit() and not int(cleaned_volume) > 0:
        return None
    return split_by_delimiters(cleaned_volume, '+')
