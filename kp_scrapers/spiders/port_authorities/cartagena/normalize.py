import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MOVEMENT_MAPPING = {'Desembarque': 'discharge', 'Embarque': 'load'}
PRODUCT_BLACKLIST = ['contenedore', 'pescado fresco']
PRODUCT_PREFIX = [r'^\.', r'.\d{3}']
NOISY_EXP = [
    'c/',
    'terminal',
    'repsol',
    'tarragona',
    'ilbocaceite',
    'enagas',
    'alkion',
    'lavera',
    'francia',
    'aratu',
    'brasil',
    'algeciras',
    'pozzallo',
    'italia',
    'pedrosuministro',
    'maritima',
    'cartagana',
]
UNSAFE_TOKENS = [
    'de',
    'como',
    'com',
    'san',
]


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into normalized item.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # discard vessel movement if no relevant cargoes present
    if not item['cargoes'] or item['cargoes'] == [None]:
        return

    # build vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo', None),
        'gross_tonnage': item.pop('vessel_gross_tonnage', None),
        'length': item.pop('vessel_length', None),
    }

    return item


def field_mapping():
    return {
        'bandera': ignore_key('vessel flag'),
        'calent': ignore_key('vessel draught'),
        'codbuq': ('vessel_imo', normalize_imo),
        'desmue': ignore_key('berth'),
        'destipbuq': ignore_key('vessel type'),
        'eslora': ('vessel_length', lambda x: try_apply(x, float, int, str)),
        'fecatr': ('eta', lambda x: to_isoformat(x, dayfirst=True) if x else None),
        'fecsal': ('departure', lambda x: to_isoformat(x, dayfirst=True) if x else None),
        'gt': ('vessel_gross_tonnage', lambda x: try_apply(x, str)),
        'nombuq': ('vessel_name', None),
        'nomcsg': ignore_key('shipping agent'),
        'operaciones': ('cargoes', lambda x: list(normalize_cargoes(x))),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }


def normalize_date(raw_date):
    """Normalize date info into ISO 8601 format.

    Examples:
        >>> normalize_date('10/08/2018 10:45')
        '2018-08-10T10:45:00'

    Args:
        raw_date (str): in the format like 10/08/2018 10:45

    Returns:
        str:

    """
    if raw_date:
        return to_isoformat(raw_date, dayfirst=True)


def normalize_cargoes(raw_operation):
    """Extract cargo info from raw operation list.

    Args:
        raw_operation (List[Dict[str, str]]):

    Yields:
        Dict[str, str]:

    """
    for raw_cargo in raw_operation:
        movement = raw_cargo['nomoperacion']
        product = normalize_cargo_name(raw_cargo['mercancia'])
        volume = raw_cargo['toneladas']

        product_list = re.split(r'\sy\s|\+', product)
        if len(product_list) > 1:
            volume = float(volume) / len(product_list)

        for p in product_list:
            # only keep relevant movement and relevant product
            if (
                movement in MOVEMENT_MAPPING
                and product
                and not any(alias in product.upper() for alias in PRODUCT_BLACKLIST)
            ):
                yield {
                    'product': p,
                    'movement': MOVEMENT_MAPPING[movement],
                    'volume': try_apply(volume, str),
                    'volume_unit': Unit.tons,
                }


def normalize_cargo_name(raw_cargo):
    """cargo name occasionally will have the terminal names in the the cargo.
    they may be joint together as seen in example 3. Hence, to safely remove tokens,
    the list is split into 2 so as to not be too broad.
    Also remove E012 prefix and . prefix.

    Examples:
        >>> normalize_cargo_name('E018GASOIL DE VACIO')
        'gasoil vacio'
        >>> normalize_cargo_name('.CRUDE OIL')
        'crude oil'
        >>> normalize_cargo_name('GASOIL DE VACIO')
        'gasoil vacio'
        >>> normalize_cargo_name('SAN PEDRO C011CORDEROS')
        'corderos'
        >>> normalize_cargo_name('FAME')
        'fame'
        >>> normalize_cargo_name('ULSD 10 PPM GASOIL')
        'ulsd 10 ppm gasoil'
        >>> normalize_cargo_name('LAVERA (FRANCIA)ULSD 10 PPM')
        'ulsd 10 ppm'
        >>> normalize_cargo_name('TERMINAL DE ALKIONPHENOL')
        'phenol'
        >>> normalize_cargo_name('SAN PEDROSUMINISTRO DE GASOIL COMO COM')
        'gasoil'

    Args:
        raw_cargo (str):

    Returns:
        str:

    """

    for pattern in PRODUCT_PREFIX:
        match = re.search(pattern, raw_cargo)
        if match:
            idx = match.span()[1]
            raw_cargo = raw_cargo[idx:]

    tokens = raw_cargo.lower().split(' ')
    clean_tokens = []
    for idx, token in enumerate(tokens):
        for exp in NOISY_EXP:
            if tokens[idx] in token:
                token = token.replace(exp, '')
        for u_token in UNSAFE_TOKENS:
            if tokens[idx] == u_token:
                token = token.replace(u_token, '')

        clean_tokens.append(token)

    return may_strip(re.sub(r'([\s+\(\)])', ' ', ' '.join(clean_tokens)))


def normalize_imo(raw_imo):
    """We sometimes have strange IMO like = 'EABQ'

    Examples:
        >>> normalize_imo('4CT-4-2-01')
        >>> normalize_imo('9134139')
        '9134139'

    Args:
        raw_imo:

    Returns:
        str | None:

    """
    return raw_imo if re.match(r'^\d*$', raw_imo) else None
