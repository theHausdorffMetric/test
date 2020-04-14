import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

PRODUCTS_TO_EXCLUDE = ['CONTÊINERES', 'VEICULOS', 'AUTOMOVEIS C/MOTOR EXPLOSAO,15', 'PASSAGEIROS']

MOVEMENT_MAPPING = {'Imp': 'discharge', 'Exp': 'load', 'Imp/Exp': None}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """
    Transform raw data into port_call schema

    Args:
        Dict[str, str]:

    Return:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    # discard items if irrelevant or no product found; analyst rule
    if 'product' not in item or not item['product']:
        return

    # build Vessel sub-model
    item['vessel'] = {
        'imo': item.pop('imo'),
        'name': item.pop('name'),
        'gross_tonnage': None,
        'length': item.pop('length'),
    }

    # volume extraction
    if 'volume' in item:
        abs_volume = re.findall(r'[0-9.,]*', item.pop('volume', None))

    volume_carried, volume_unit = (
        (None, None)
        if not abs_volume
        else (
            # volumes have commas as decimal separators, and periods as thousands separators
            # e.g. 3.894,000 tons
            abs_volume[0].replace('.', '').replace(',', '.'),
            'tons',
        )
    )

    # volumes should be strictly positive, else we ignore it
    if volume_carried and not float(volume_carried) > 0:
        volume_carried, volume_unit = None, None

    # build Cargo sub-model
    item['cargoes'] = [
        {
            'product': item.pop('product', None),
            'movement': item.pop('movement', None),
            'volume': volume_carried,
            'volume_unit': volume_unit,
        }
    ]

    return item


def field_mapping():
    return {
        'ETS': ('departure', lambda x: to_isoformat(x, dayfirst=True) if x else None),
        'ETA': ('eta', lambda x: to_isoformat(x, dayfirst=True) if x else None),
        'Chegada': ('arrival', lambda x: to_isoformat(x, dayfirst=True) if x else None),
        'Mercadoria': ('product', lambda x: None if x in PRODUCTS_TO_EXCLUDE else x),
        'Sentido': ('movement', lambda x: None if not x else MOVEMENT_MAPPING[x]),
        'Previsto': ('volume', None),
        'IMO': ('imo', None),
        'Embarcação': ('name', None),
        # FIXME vessel length should be a float, but we yield an int since downstream processes
        # only recognise int values
        'LOA': ('length', lambda x: None if not x else x.split(',')[0]),
        'Atracação': ('berthed', lambda x: to_isoformat(x, dayfirst=True) if x else None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True, fuzzy=True)),
        'Bordo': ignore_key('board'),
        'DUV': ignore_key('DUV'),
        'Agência': ignore_key('agency'),
        'Operador': ignore_key('operator'),
        'Tons/Dia': ignore_key('Tons/Day'),
        'Realizado': ignore_key('accomplished'),
        'Saldo': ignore_key('balance'),
        'ETB': ignore_key('ETB'),
        'Desatracacao': ignore_key('Desatracacao'),
    }
