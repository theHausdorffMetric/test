import re
from typing import Dict, List

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.contracts.bill_of_lading.normalize import should_keep_item


VESSEL_PREFIX_PATTERN = re.compile(r'^(((l[pn]g(/c)?)|(m(\.)?[tv](\.)?))\s)', re.IGNORECASE)
PRODUCT_NOISY_TOKENS = {
    'KGS',
    'TONS',
    'METRIC',
    'BARRELS',
    'LITERS',
    'CUBIC',
    'KILO',
    'PRODUCT',
    'US',
    'METERS',
    'LITRES',
    'GRADE',
    'BULK',
    'BARREL',
    'VOLUME',
    'MMBTU',
    'MT',
    'KG',
    'ENERGY',
    'DELIVERED',
    'LOADED',
    'LONG',
    'LAT',
    'M3',
}


@validate_item(CargoMovement, normalize=True, strict=False, log_level='error')
def process_item(raw_item: Dict[str, str], not_terms: List[str]):
    if not should_keep_item(raw_item['cargo_product'], not_terms):
        return None

    item = map_keys(raw_item, field_mapping())
    if item['arrival'] is None:
        item['arrival'] = item['reported_date']

    # build proper Cargo model
    volume = item.pop('cargo_volume', None)
    units = Unit.tons if volume else None
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'buyer': {'name': item.pop('cargo_buyer', None)},
        'movement': 'discharge',
        'volume': volume,
        'volume_unit': units,
    }
    return item


def field_mapping():
    return {
        'port_name': ('port_name', lambda x: translate_port_name(may_strip(x))),
        'vessel': ('vessel', lambda x: {'name': normalize_vessel(may_strip(x))}),
        'arrival': ('arrival', lambda x: to_isoformat(may_strip(x), dayfirst=True)),
        'reported_date': ('reported_date', lambda x: to_isoformat(may_strip(x), dayfirst=True)),
        'berth': ('berth', may_strip),
        'cargo_product': ('cargo_product', lambda x: normalize_product(may_strip(x))),
        'cargo_volume': ('cargo_volume', lambda x: parse_volume(may_strip(x))),
        'cargo_buyer': ('cargo_buyer', may_strip),
        'provider_name': ('provider_name', may_strip),
    }


def translate_port_name(raw_port_name):
    """Translate port name.

   Args:
       port_name (str):

   Examples:
       >>> translate_port_name('KWWB')
       'Karachi'
       >>> translate_port_name('KOSK')
       'Karachi'
       >>> translate_port_name('KPQI')
       'Port Qasim'

   Returns:
       str:

   """
    if raw_port_name in ['KWWB', 'KEWB', 'KPT', 'KOSK']:
        return 'Karachi'
    elif raw_port_name in ['KPQI', 'KQIB', 'PQIB']:
        return 'Port Qasim'
    return None


def normalize_product(raw_product):
    """Normalize the product.
    This is an naive attempt to have proper product names.
    A more powerful approach would be to properly tokenize the text and use a terminology

   Args:
       raw_product (str):

   Examples:
       >>> normalize_product('MOGAS 92- RON')
       'MOGAS 92-RON'
       >>> normalize_product('GRADE GASOLINE 92 RON CUBIC METERS @ 15')
       'GASOLINE 92-RON'
       >>> normalize_product('A QUANTITY SAID TO BE : 210000 KGS 2-ETHYL HEXANOL')
       '2-ETHYL HEXANOL'
       >>> normalize_product('GREEN PETROLEUM COKE IN BULK.')
       'GREEN PETROLEUM COKE'
       >>> normalize_product('MONO ETHYLENE GLYCOL(MEG). (IBC CODE: ETHYLENE GLYCOL).')
       'MONO ETHYLENE GLYCOL'
       >>> normalize_product('A QUANTITY SAID TO BE : 157500 KGS MIXED XYLENE')
       'MIXED XYLENE'
       >>> normalize_product('2 ETHYLE HEXANOL OCTANOL (IBC CODE: OCTANOL (ALL ISOMERS)).')
       'ETHYLE HEXANOL OCTANOL'
       >>> normalize_product('STEAM COAL IN BULK OF SOUTH AFRICA ORIGIN')
       'STEAM COAL'
       >>> normalize_product('LIQUEFIED NATURAL GAS (LNG) @ ---- (142,358 CUBIC METERS)')
       'LIQUEFIED NATURAL GAS'

   Returns:
       str:

   """
    # first of all work on the full string
    verbatim_noise = ['A QUANTITY SAID TO BE', 'IN BULK', 'EQUIVALENT TO']
    regexp_noise = [r'OF .+ ORIGIN', r'\(.+\)', r'(@.*)']
    for noisy_regexp in regexp_noise + verbatim_noise:
        raw_product = re.sub(noisy_regexp, '', raw_product, flags=re.IGNORECASE)

    # then work on the tokens
    tokens = re.findall(r'[\w\-]*[a-zA-Z][\w\-]*|(?<=\W)[0-9]+\W+(?:RON|PPM)', raw_product)
    tokens = [
        normalize_token(token)
        for token in tokens
        if token.upper() not in PRODUCT_NOISY_TOKENS and len(token) > 1
    ]
    return ' '.join(tokens)


def normalize_token(token):
    """Normalize a single token

   Args:
       normalize_token (str):

   Examples:
       >>> normalize_token('92- RON')
       '92-RON'
       >>> normalize_token('92-RON')
       '92-RON'
       >>> normalize_token(' XYZ  ')
       'XYZ'

   Returns:
       str:

   """
    return re.sub(r'[\s\-_]+', '-', token.strip())


def normalize_vessel(raw_vessel_name):
    """Normalize vessel.

    Args:
        vessel_name (str):

    Examples:
        >>> normalize_vessel('MT. FAIRCHEM COPPER')
        'FAIRCHEM COPPER'
        >>> normalize_vessel('M.V. CHEM BULLDOG')
        'CHEM BULLDOG'
        >>> normalize_vessel('LNG/C SOLARIS')
        'SOLARIS'
        >>> normalize_vessel('LPG GAS AMAZON')
        'GAS AMAZON'
        >>> normalize_vessel('GAS AMAZON')
        'GAS AMAZON'

    Returns:
        str:

    """
    return re.sub(VESSEL_PREFIX_PATTERN, '', raw_vessel_name)


def parse_volume(raw_volume: str):
    """Parse the volume value and return an int

    Examples:
        >>> parse_volume('2.000')
        2
        >>> parse_volume('2,000')
        2000
        >>> parse_volume('2,000.999')
        2001
        >>> parse_volume('2000')
        2000
        >>> parse_volume('abc') is None
        True

    """
    volume_match = raw_volume.replace(',', '')
    try:
        return round((float(volume_match)))
    except ValueError:
        return None
