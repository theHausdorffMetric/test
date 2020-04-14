import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import split_by_delimiters
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_CARGOES = [
    'avitaillement',
    'cont operations',
    'ctns',
    'dicn',
    'equipage',
    'passagers',
    'poissons',
    'produits de mer',
    'oignons',
    'relache',
    'releve',
    'reléve',
    'thons',
    'vehicules',
]


EVENT_MAPPING = {
    'ACCOSTAGE': 'berthed',
    'APPAREILLAGE DU QUAI': None,
    # TODO not sure yet, to confirm with analysts
    'CHANGEMENT DE POSTE': None,
    'ENTREE MOUILLAGE': 'eta',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        ArrivedEvent | BerthedEvent:

    """
    item = map_keys(raw_item, portcall_mapping())
    # discard vessel movements with irrelevant cargo
    if not item['cargoes']:
        return

    # assign correct portcall-related event date
    event = item.pop('event')
    if not event:
        return

    item[event] = item.pop('pc_date')
    return item


def portcall_mapping():
    return {
        'Cargo': ('cargoes', normalize_cargoes),
        'Consignee': ignore_key('shipping agent'),
        'Date': ('pc_date', lambda x: to_isoformat(x, dayfirst=False)),
        'Dockside': ignore_key('berth'),
        'port_name': ('port_name', None),
        'Port of call number': ignore_key('call number'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Type': ('event', lambda x: EVENT_MAPPING.get(x)),
        'Vessel': ('vessel', lambda x: {'name': x}),
    }


def normalize_cargoes(raw_cargo):
    if not raw_cargo:
        return None

    if any(alias in raw_cargo.lower() for alias in IRRELEVANT_CARGOES):
        logger.info(f'Irrelevant cargo: {raw_cargo}')
        return None
    # product may container the receiver if a / delimiter is included
    if '/' in raw_cargo:
        raw_cargo, player = split_by_delimiters(raw_cargo, '/')
        return [
            {
                'product': product,
                'movement': 'discharge',
                'buyer': {'name': player} if player else None,
            }
            for product in split_by_delimiters(raw_cargo, '+')
        ]

    return [{'product': product} for product in split_by_delimiters(raw_cargo, '+')]
