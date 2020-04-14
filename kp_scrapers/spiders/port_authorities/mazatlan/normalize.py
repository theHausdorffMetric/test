import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import split_by_delimiters
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

RELEVANT_VESSEL_TYPES = {'CHATARRA', 'PETROLERO'}  # bulk carrier  # tanker
NOISY_EXP = ['un lote de', 'descarga', 'de', ',']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard irrelevant vessel types
    vessel_type = item.pop('vessel_type')
    if vessel_type not in RELEVANT_VESSEL_TYPES:
        logger.info(f"Vessel {item['vessel_name']} is of an irrelevant type: {vessel_type}")
        return

    # build vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo'),
        'mmsi': item.pop('vessel_mmsi'),
        'gross_tonnage': item.pop('vessel_gross_tonnage'),
        'length': item.pop('vessel_length'),
    }

    # build cargoes sub-model
    item['cargoes'] = []
    for product in split_by_delimiters(item.pop('cargo_product'), '/', ' Y '):
        vol, prod, units = normalize_cargo(product)
        item['cargoes'].append({'product': prod, 'volume': vol, 'volume_unit': units})

    return item


def portcall_mapping(**kwargs):
    return {
        'BANDERA': ignore_key('vessel flag'),
        'BUQUE': ('vessel_name', None),
        'CALADO': ignore_key('unknown'),
        'CALADO (m)': ignore_key('unknown'),
        'D/C': ignore_key('unknown'),
        'DESC. CARGA': ('cargo_product', None),
        'DESTINO': ignore_key('next portcall country'),
        'ESLORA': ('vessel_length', lambda x: x.split('.')[0]),
        'ESLORA (m)': ignore_key('duplicate vessel length'),
        'ETA': ignore_key('ETA date; not needed since it is duplicated'),
        'FECHA_ETA': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'FONDEO': ignore_key('anchoring'),
        'HORA': ignore_key('ETA hour; not needed since it is duplicated'),
        'ID BUQUE': ignore_key('internal vessel ID used by source'),
        'IMDG': ignore_key('unknown'),
        'IMO': ('vessel_imo', lambda x: x if x else None),
        'MANGA': ignore_key('vessel breath'),
        'MANGA (m)': ignore_key('duplicate vessel breadth'),
        'MMSI': ('vessel_mmsi', None),
        'MUELLE': ignore_key('portcall berth'),
        'NAVIERA': ignore_key('shipping agent'),
        'OBSERVACIONES': ignore_key('remarks'),
        'ORIGEN': ignore_key('previous portcall country'),
        'PAIS': ignore_key('vessel flag'),
        'PERMISIONARIO': ignore_key('unknown'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'PUERTO_DESTINO': ignore_key('next portcall; TODO discuss with analysts'),
        'PUERTO_ORIGEN': ignore_key('previous portcall; TODO discuss with analysts'),
        'REGISTRO': ignore_key('unknown'),
        'reported_date': ('reported_date', None),
        'TIPO_BUQUE': ('vessel_type', None),
        'TIPO_CARGA_EXPO': ignore_key('type of cargo exported'),
        'TIPO_CARGA_IMPO': ignore_key('type of cargo imported'),
        'TONELADAS': ignore_key('cargo_volume'),
        'TRB': ('vessel_gross_tonnage', lambda x: x.split('.')[0]),
        'TRN': ignore_key('vessel nett tonnage'),
        'TURNO': ignore_key('unknown'),
        'VID': ignore_key('internal voyage ID used by source'),
    }


def normalize_cargo(raw_product):
    """Normalize dates

    Examples:
        >>> normalize_cargo('UN LOTE DE 70,000 BRLS DE GASOLINA')
        ('70000', 'gasolina', 'tons')
        >>> normalize_cargo('GASOLINA')
        (None, 'gasolina', None)
        >>> normalize_cargo('EN TRANSITO UN LOTE DE 30,000 BRLS DE GA')
        (None, None, None)
        >>> normalize_cargo('Descarga un lote de 140,000 Brls de gaso')
        ('140000', 'gaso', 'tons')

    Args:
        raw_date (str):

    Returns:
        str: date in ISO 8601 format
    """
    raw_product = raw_product.lower()
    if 'en transito' in raw_product:
        return None, None, None

    for nexp in NOISY_EXP:
        raw_product = raw_product.replace(nexp, '')

    _match = re.match(r'.*?(\d+)(?:\s+brls\s+)(.*)', raw_product)
    if _match:
        logger.info(_match.groups())
        volume, product = _match.groups()
        return volume, product, Unit.tons
    return None, raw_product, None
