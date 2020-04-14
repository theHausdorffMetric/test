import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

# mapping of port names
# mainly for disambiguation of 'Cartagena (Spain)' with 'Cartagena (Colombia)'
# and other ports without clear mapping
PORT_NAMES = {'CARTAGENA': 'CARTAGENA (COLOMBIA)', 'COVEÑAS': 'COVENAS'}

# do not yield items with these products in them
PRODUCT_BLACKLIST = ['CONTENEDOR', 'VEHICULOS']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into something usable.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, field_mapping())
    # discard vessel movements of irrelevant vessels
    if not item['product_volume']:
        logger.info(
            f'Discarding vessel {item["vessel_name"]} (IMO {item["vessel_imo"]}) '
            f'with ETA {item["eta"]}'
        )
        return

    item['port_name'] = item['load_port'] if item['is_load'] else item['discharge_port']
    # build cargo sub-model
    item['cargoes'] = list(normalize_cargoes(item))
    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}

    for field in ('is_load', 'load_port', 'discharge_port', 'product_volume'):
        item.pop(field, None)

    return item


def field_mapping():
    return {
        'Agencia Marítima': ignore_key('shipping agent'),
        'Autorización': ignore_key('irrelevant'),
        'Bandera': ignore_key('vessel flag'),
        'ETA': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'Nave': ('vessel_name', None),
        'OMI - Matrícula': ('vessel_imo', None),
        'Pais Cargue': ('is_load', lambda x: x == 'COLOMBIA'),
        'Pais Descargue': ignore_key('discharge country'),
        'Producto - Cantidad': (
            'product_volume',
            lambda x: None if any(product in x for product in PRODUCT_BLACKLIST) else x,
        ),
        'provider_name': ('provider_name', None),
        'Puerto Cargue': ('load_port', lambda x: PORT_NAMES.get(x, x)),
        'Puerto Descargue': ('discharge_port', lambda x: PORT_NAMES.get(x, x)),
        'reported_date': ('reported_date', None),
    }


def normalize_cargoes(item):
    """Normalize cargoes.

    Args:
        item (Dict[str, str]):

    Yields:
        Dict[str, str]:
    """
    cargoes = item['product_volume'].split(';')
    for cargo in cargoes:
        # TODO confirm with analysts on volume unit, until then we cannot use volume figure
        # product, _, volume
        product, _, _ = cargo.partition(',')
        yield {
            'product': may_strip(product),
            'movement': 'load' if item['is_load'] else 'discharge',
        }
