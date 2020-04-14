import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

# in days
MIN_ETA_DIFF = -7
MAX_ETA_DIFF = 30

RELEVANT_VESSEL_TYPES = ['Bulk Carrier', 'Tanker (Fluids)', 'Tanker (Gas)']

MOVEMENT_MAPPING = {'IM.': 'discharge', 'EX.': 'load', 'NC.': 'discharge'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        ArrivedEvent | BerthedEvent | EtaEvent:

    """
    item = map_keys(raw_item, portcall_mapping())

    # sanity check; in case no vessel name provided
    if not item.get('vessel'):
        return

    # discard vessels with irrelevant types
    if not item.pop('vessel_type'):
        logger.info(
            f"Vessel {item['vessel']['name']} has an irrelevant type {raw_item['Ship Type']}, "
            "discarding"
        )
        return

    # discard vessels with invalid ETA dates,
    # since source does not remove ETAs that are years old from the website,
    # we discard if diff is more than 30 days
    eta_diff = (parse_date(item['eta'], dayfirst=False) - dt.datetime.utcnow()).days
    if not (MIN_ETA_DIFF <= eta_diff <= MAX_ETA_DIFF):
        logger.info(f"Portcall for vessel {item['vessel']['name']} has an invalid ETA, discarding")
        return

    return item


def portcall_mapping():
    return {
        'Anchorage Date': ('arrival', lambda x: to_isoformat(x, dayfirst=True)),
        'Arrival Date': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'Berthing Date': ('berthed', lambda x: to_isoformat(x, dayfirst=True)),
        'Code': ignore_key('internal vessel portcall id'),
        'Destination Port': ignore_key('TODO next zone; discuss value with analysts'),
        'Dock': ('port_name', None),
        'End Operations': ignore_key('previous zone'),
        'Operations': ('cargoes', lambda x: list(normalize_cargoes(x))),
        'Origin Port': ignore_key('previous port of call of vessel'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Ship': ('vessel', lambda x: {'name': x.replace('*', '')} if x else None),
        'Ship Company': ignore_key('shipping agent ?'),
        'Ship Line': ignore_key('shipping agent ?'),
        'Ship Type': ('vessel_type', lambda x: x if x in RELEVANT_VESSEL_TYPES else None),
        'Situation': ignore_key('situation'),
        'Start Operations': ignore_key('post'),
        'Stevedores': ignore_key('stevedore'),
        'Traffic': ignore_key('type of vessel movement'),
        'Unberthing Date': ignore_key('receiver'),
        'Voyage': ignore_key('internal vessel voyage id'),
    }


def normalize_cargoes(raw_cargoes):
    """Normalize raw cargo operation string.

    This function assumes input string has been validated and is not empty.
    The operation string will always follow this format:
        - "<PRODUCT> <VOLUME> <MOVEMENT>" (one product)
        - "<PRODUCT> <VOLUME> <MOVEMENT> / <PRODUCT> <VOLUME> <MOVEMENT>" (two products)

    Args:
        raw_cargoes (str): raw cargoes

    Returns:
        List[Dict[str, str]]:

    """
    raw_cargoes = raw_cargoes or ''
    for cargo in raw_cargoes.split('/'):
        if not may_strip(cargo):
            continue

        # product will always be the first element
        product = ' '.join(cargo.split()[:-2])
        # volume will always be the second element
        volume = int(cargo.split()[-2])
        # movement will always be the last element
        movement = MOVEMENT_MAPPING[cargo.split()[-1]]

        yield {'product': product, 'movement': movement, 'volume': volume, 'volume_unit': Unit.tons}
