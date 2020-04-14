import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, split_by_delimiters, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_PRODUCT = ['CARGA', 'CONTÊINERES', 'GERAL', 'INSTRUÇÕES', 'VEÍCULOS']
REPLACE_STRINGS = ['(TB)', '(DG)']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        ArrivedEvent | BerthedEvent | EtaEvent:

    """
    item = map_keys(raw_item, portcall_mapping())

    event_type = item.pop('event').lower()
    if not item.get('event_date'):
        logger.info(f"Date empty for {item['vessel_name']}, discarding")

    if 'expected' in event_type:
        item['eta'] = item.pop('event_date', None)
    elif 'anchor' in event_type:
        item['eta'] = item.pop('event_date', None)
    elif 'undock' in event_type:
        item['departure'] = item.pop('event_date', None)
    elif 'dock' in event_type:
        item['berthed'] = item.pop('event_date', None)
    else:
        return None

    # discard portcalls with irrelevant/empty cargoes
    cargo_load, cargo_discharge = item.pop('cargo_load'), item.pop('cargo_discharge')
    if is_irrelevant_cargo(cargo_load) or is_irrelevant_cargo(cargo_discharge):
        logger.info(f"Irrelevant cargo for vessel {item['vessel_name']}, discarding")
        return
    if not (cargo_load or cargo_discharge):
        logger.info(f"Empty cargo for vessel {item['vessel_name']}, discarding")
        return

    # build cargoes sub-model
    item['cargoes'] = []
    for idx, raw_cargo in enumerate((cargo_load, cargo_discharge)):
        # skip empty cargo
        if not raw_cargo:
            continue

        # normalize raw cargo
        for product in normalize_cargo(raw_cargo, movement='discharge' if idx else 'load'):
            if product:
                item['cargoes'].append(product)

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length')}

    return item


def portcall_mapping():
    return {
        '0': ('vessel_name', None),
        '1': ignore_key('internal voyage id'),
        '2': ('vessel_length', lambda x: try_apply(x, float, int, str)),
        '3': ignore_key('shipping_agent'),
        '4': ignore_key('vessel flag'),
        '5': ignore_key('previous port of call'),
        '6': ignore_key('next port of call; may be useful (TODO discuss with analysts)'),
        '7': ignore_key('situation'),
        '8': ('event_date', lambda x: to_isoformat(x, dayfirst=True)),
        '9': ('installation', lambda x: x if x and 'A/C' not in x else None),
        '10': ignore_key('estimated datetime of berthing'),
        '11': ('cargo_discharge', lambda x: x if x and 'A/C' not in x else None),
        '12': ('cargo_load', lambda x: x if x and 'A/C' not in x else None),
        'port_name': ('port_name', None),
        'table_type': ('event', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }


def is_irrelevant_cargo(raw_cargo):
    return any(alias in (raw_cargo or '') for alias in IRRELEVANT_PRODUCT)


def normalize_cargo(raw_cargo, movement):
    """Normalize raw product and build a Cargo model.

    Args:
        raw_product (str):

    Returns:
        Dict[str, str]:

    """
    # handle edge cases where bracket strings are involved or when units
    # are seperated
    for rpl in REPLACE_STRINGS:
        raw_cargo = raw_cargo.replace(rpl, '')
    raw_cargo = re.sub(r'([\d\.]+)(\s)?(m³|t|)(.*)', r'\1\3\4', raw_cargo)
    cargoes = [may_strip(c) for c in split_by_delimiters(raw_cargo, '+')]
    for cargo in cargoes:
        volume, _, product = cargo.partition(' ')

        # obtain volume_unit
        if volume.endswith('t'):
            volume_unit = Unit.tons
        elif volume.endswith('m³') or volume.endswith('m3'):
            volume_unit = Unit.cubic_meter
        else:
            logger.warning(f"Unknown volume unit, please handle: {volume}")
            volume_unit = None

        yield {
            'product': product,
            'movement': movement,
            # sanitize volume string
            'volume': re.sub(r'\D', '', volume),
            'volume_unit': volume_unit,
        }
