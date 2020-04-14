import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


MOVEMENT_MAPPING = {'Embarque': 'load', 'Desembarque': 'discharge'}

CARGO_BLACKLIST = [
    'Cruceristas',
    'Papel y pasta',
    'Contenedores llenos',
    'Cereales y su harina',
    'Contenedores vacíos',
    'Pasajeros',
    'Vehículos',
    'Resto de mercancías',
]


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if not item['cargoes']:
        return

    port_call = {
        'vessel': {
            'name': item['vessel_name'],
            'imo': item['imo'],
            'gross_tonnage': item['gross_tonnage'],
            'length': item['length'],
        },
        'cargoes': item['cargoes'],
        'arrival': item['arrival'],
        'port_name': item['port_name'],
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
    }
    if item['departure']:
        port_call.update(departure=item['departure'])
    return port_call


def field_mapping():
    return {
        'nombuq': ('vessel_name', None),
        'fecatr': ('arrival', normalize_date),
        'fecsal': ('departure', normalize_date),
        'nomcsg': ignore_key('Shipping agent'),
        'eslora': ('length', lambda x: try_apply(x, float, int, str)),
        'codbuq': ('imo', normalize_imo),
        'bandera': ignore_key('Flag'),
        'destipbuq': ignore_key('Vessel type'),
        'calent': ignore_key('Draught'),
        'desmue': ignore_key('berth'),
        'gt': ('gross_tonnage', None),
        'operaciones': ('cargoes', lambda x: list(normalize_cargoes(x))),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_date),
    }


def normalize_date(raw_date):
    """Normalize date to ISO 8601 format.

    Args:
        raw_date (str): in the format of dd/mm/yyyy hh:mm

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
        product = raw_cargo['mercancia']
        volume = raw_cargo['toneladas']

        # we only care about specific movement types of cargo
        if movement in MOVEMENT_MAPPING and product not in CARGO_BLACKLIST:
            yield {
                'product': product,
                'movement': MOVEMENT_MAPPING[movement],
                'volume': try_apply(volume, int, str),
                'volume_unit': Unit.tons,
            }


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
