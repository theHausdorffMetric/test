import logging

from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


PRODUCT_ALIASES = [
    # gas
    'ENERGÉTICOS',
    'GAS NATURAL',
    # liquids
    'ABONOS',
    'ACEITE',
    'AROMÁTICOS',
    'ASFALTO',
    'CRUDO',
    'ETANOL',
    'FAME',
    'FUEL',
    'GAS OIL',
    'GASO',
    'METANOL',
    'PETROLÍFEROS',
    'PETRÓLEO',
    'QUÍMICOS',
    # dry bulk / coal
    'CARBONES',
    'CEMENTO',
    'CHATARRA',
    'CLINQUER',
    'COQUE',
    'HABAS',
    'HARINA',
    'MAIZ',
    'POTASA',
    'SAL COMÚN',
    'SIDERÚRGICOS',
    'SOSA CÁUSTICA',
    'VIGA',
]

MOVEMENT_MAPPING = {'C': 'load', 'D': 'discharge', 'E': 'load'}

IRRELEVANT_VALUES = ['-', '.']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Yields:
        PortCall:

    """
    # map keys
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # discard vessels with irrelevant cargoes
    cargo = normalize_cargo(item)
    if not cargo:
        return

    return {
        'vessel': {'name': item.get('vessel_name')},
        'berthed': item.get('matching_date'),
        'cargoes': [cargo],
        'berth': item.get('berth'),
        'port_name': item.get('port_name'),
        'provider_name': item.get('provider_name'),
        'reported_date': item.get('reported_date'),
    }


def field_mapping():
    return {
        'BUQUE': ('vessel_name', None),
        'CARGA': ('volume_load', lambda x: None if x in IRRELEVANT_VALUES else x),
        'CONSIGNATARIO': ('consignee', None),
        'CONSIGNATARIO/': ('consignee', None),
        'DESCARGA': ('volume_unload', lambda x: None if x in IRRELEVANT_VALUES else x),
        'DESTINO': ('destination', None),
        'ESTIBADOR': ('stevedore', None),
        'matching_date': ('matching_date', None),
        'MERCANCÍA': ('product_barcelona', lambda x: None if x in IRRELEVANT_VALUES else x),
        'MUELLE': ('berth', None),
        'port_name': ('port_name', None),
        'PROCEDENCIA': ('origin', None),
        'provider_name': ('provider_name', None),
        'RECEPTOR': ('receiver', None),
        'reported_date': ('reported_date', None),
        'TONS': ('product_tarragona', normalize_tarragona_product),
    }


def normalize_tarragona_product(raw_product_str):
    """Normalize product/volume/unit data from a raw string.

    Raw string is formatted as such:
        - <PRODUCT>/<VOLUME>-<MOVEMENT>

    Product will always be first element of split list.
    Volume will have '.' thousands separator.
    Movement will be mapped with MOVEMENT_MAPPING.

    Sometimes, <volume> and <movement> won't be present.

    """
    # no product provided
    if raw_product_str in IRRELEVANT_VALUES:
        return {}

    raw_list = raw_product_str.split('/')
    product = raw_list.pop(0)
    volume, movement = None, None
    # only product info is present; we skip processing volume and movement
    if len(raw_list) != 0:
        volume, movement = raw_list.pop(0).split('-')

    return {
        'product': product,
        'volume': volume.replace('.', '') if volume else None,
        'movement': MOVEMENT_MAPPING.get(movement),
    }


def normalize_cargo(item):
    """Build cargo information from raw item.

    Args:
        item (Dict[str, str]):

    Returns:
        List(Cargo(str, str)):

    """
    if item['port_name'] == 'Barcelona':
        product = item.get('product_barcelona') or item.get('product_tarragona', {}).get('product')
        movement = 'load' if item.get('volume_load') else 'discharge'
        volume = item.get('volume_load') or item.get('volume_unload')
    else:
        product = item.get('product_tarragona', {}).get('product')
        movement = item.get('product_tarragona', {}).get('movement')
        volume = item.get('product_tarragona', {}).get('volume')

    # discard vessels with no product info
    if not product:
        return

    # check if product field is relevant commodity to filter irrelevant row
    if all(alias not in product for alias in PRODUCT_ALIASES):
        logger.info(f'Unknown/irrelevant product: {product}')
        return None

    # volume often comes in the format '63.000 Tons', which represents 63000, for example.
    volume = int(volume.split()[0].replace('.', '')) if volume else None

    return {
        'product': product,
        'movement': movement,
        'volume': volume,
        'volume_unit': Unit.tons if volume else None,
    }
