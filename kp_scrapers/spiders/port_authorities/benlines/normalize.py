import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_PRODUCT = ['BARGES', 'DIVERS', 'NEANT', 'PASSAGERS', 'TCS', 'TRANSIT']

RELEVANT_VESSEL_TYPES = [
    'CARGO',
    'CEREALIER',
    'GAZIER',
    'HUILIER',
    'NAVIRE SUCRE',
    'PETROLIER',
    'SUCRE',
]


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        ArrivedEvent | BerthedEvent | EtaEvent:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard vessels with irrelevant types
    if not item.pop('vessel_type'):
        logger.info(
            f"Vessel {item['vessel_name']} has an irrelevant type {raw_item['TYPE']}, " "discarding"
        )
        return

    # discard vessels with irrelevant cargoes
    if is_placeholder(item) or not item['cargoes']:
        logger.info(f'Vessel {item["vessel_name"]} has irrelevant cargo, discarding')
        return

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}

    return item


def portcall_mapping():
    return {
        'ACCOSTAGE': ('berthed', to_isoformat),
        'AGENT': ignore_key('shipping agent'),
        'D.H.R': ('arrival', to_isoformat),
        'E.T.A': ('eta', to_isoformat),
        'MARCHANDISE': ('cargoes', lambda x: list(normalize_product(x))),
        'NAVIRE': ('vessel_name', may_strip),
        'port_name': ('port_name', None),
        'POSTE': ignore_key('post'),
        'PROVENANCE': ignore_key('previous zone'),
        'provider_name': ('provider_name', None),
        'RECEPTIONNAIRE': ignore_key('receiver'),
        'reported_date': ('reported_date', None),
        'T.E.D': ignore_key('draught on arrival'),
        'TONNAGE': ignore_key('we cannot use volume since there is no corresponding movement'),
        'TYPE': ('vessel_type', lambda x: x if x in RELEVANT_VESSEL_TYPES else None),
        'vessel_imo': ('vessel_imo', None),
    }


def normalize_product(raw_product):
    """Normalize raw product and build a Cargo model.

    Note that we don't use the volume provided by the source as there is no
    corresponding movement.

    Args:
        raw_product (str):

    Returns:
        Dict[str, str]:

    """
    # discard irrelevant products
    if not raw_product or any(alias in raw_product for alias in IRRELEVANT_PRODUCT):
        return

    for product in [may_strip(prod) for prod in raw_product.split('+')]:
        yield {'product': product}


def is_placeholder(item):
    """Check if vessel name is actually a placeholder and not an actual vessel.

    Sometimes, the website will display a row in the table with the vessel "FIXIN"
    with an IMO of "2222222" (clearly a placeholder).

    For example:
    https://www.portdeBenlinesVadinar.dz/components/com_jumi/files/navire_e.php?ship=2222222

    TODO monitor for other possible placeholders

    Args:
        item (Dict[str, str]):

    Returns:
        bool: True if vessel is a placeholder

    """
    return True if item['vessel_name'] == 'FIXIN' or item['vessel_imo'] == '2222222' else False
