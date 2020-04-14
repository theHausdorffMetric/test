import logging
from typing import Any, Callable, Dict, Optional, Tuple

from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


# map vessel type as given by website to a known Kpler product group
VESSEL_TYPE_TO_CARGO_MAPPING = {
    'BUQUE TANQUE DE LIQUIDOS ESPECIALES': 'Liquids',  # liquids tanker
    'CARGA GENERAL': None,  # general cargo
    'CARGA RODADA': None,  # ro-ro cargo
    'CIENTIFICO': None,  # research vessel
    'GASERO': 'LNG',  # lng tanker
    'GRANELERO': None,  # dry bulk; check with analysts if this is valuable
    'PASAJE': None,  # cruise ship
    'PASAJE CABOTAJE': None,  # cruise ship
    'PASAJE DE TRANSBORDO RODADO': None,  # ro-ro ferry
    'PESQUERO DE CERCO': None,  # fishing vessel
    'PETROLERO': 'LPG / Liquids',  # lpg or oil tanker
    'PORTACONTENEDOR': None,  # containers
    'TANQUE QUIMIQUERO': 'Liquids',  # oil/chemical tanker
    'TIPO DESCONOCIDO': None,  # warship
    'TRANSBORDADOR': None,  # ro-ro ferry
    'YATE': None,  # tugs
}


@validate_item(PortCall, normalize=True, strict=True)
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Map and normalize raw_item into a usable event."""
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard vessels not relevant to filtered port
    _filter_name, _public_port_name = item.pop('_filter_name'), item.pop('_public_port_name')
    if _filter_name.lower() not in _public_port_name.lower():
        return

    # build Vessel sub model
    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'imo': item.pop('vessel_imo', None),
        'call_sign': item.pop('vessel_callsign', None),
        'length': item.pop('vessel_length', None),
        'gross_tonnage': item.pop('vessel_gt', None),
    }

    # discard vessels without an IMO number
    if not item['vessel']['imo']:
        return

    # discard vessels without a known cargo
    if not item.get('cargoes'):
        return

    return item


def portcall_mapping() -> Dict[str, Tuple[str, Optional[Callable]]]:
    return {
        'bandera': ignore_key('vessel flag ISO3601; redundant'),
        'caracteristica': ('vessel_callsign', None),
        'dmEslora': ('vessel_length', lambda x: try_apply(x, int)),
        'dmtrg': ('vessel_gt', lambda x: try_apply(x, int)),
        'Estado': ignore_key('type of vessel movement; here the url resource always gives ETA'),
        'fecha': ignore_key('preliminary ETA figure; not so accurate'),
        'filter_name': ('_filter_name', may_strip),
        'fechafin': ('eta', may_strip),  # dates provided are already UTC timezone aware
        'id': ignore_key('internal vessel ID'),
        'idBahia': ignore_key('internal port name ID'),
        'idevento': ignore_key('internal vessel movement ID'),
        'lgViajeInternacional': ignore_key('unknown significance'),
        'NMBahia': ('_public_port_name', may_strip),
        'Nombre_2': ('vessel_name', may_strip),
        'nombreBandera': ignore_key('vessel flag country; redundant'),
        'nombrePuerto': ('port_name', may_strip),
        'nro': ignore_key('internal vessel ID'),
        'nromi': ('vessel_imo', lambda x: x if x else None),
        'provider_name': ('provider_name', may_strip),
        'puerto': ignore_key('internal port ID'),
        'reported_date': ('reported_date', may_strip),
        'TipoNave': ('cargoes', normalize_cargo),
    }


def normalize_cargo(vessel_type: str) -> Optional[str]:
    """Transform and normalize vessel type to a known cargo.

    Examples:
        >>> normalize_cargo('PETROLERO')
        [{'product': 'LPG / Liquids'}]
        >>> normalize_cargo('CIENTIFICO')
        >>> normalize_cargo('foobar')

    """
    product = VESSEL_TYPE_TO_CARGO_MAPPING.get(vessel_type)
    return [{'product': product}] if product else None
