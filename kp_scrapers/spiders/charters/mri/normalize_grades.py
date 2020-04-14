import datetime as dt
import logging
import re

from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_PRODUCTS = ['Oil Clean', 'Oil Dirty']


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, grades_mapping())
    # discard irrelevant vessels
    if not item['vessel_name']:
        return

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo', None)}

    # discard irrelevant cargoes
    if not item['cargo_product']:
        return

    # build cargo sub-model
    item['cargo'] = {
        'product': item.pop('cargo_product'),
        # source defaults to load movements since it's a charter
        'movement': 'load',
        'volume': item.pop('cargo_volume'),
        # source provides volume in tons
        'volume_unit': Unit.tons,
    }

    return item


def grades_mapping():
    return {
        'ENTRY DATE': ignore_key('redundant reported date'),
        'WEEK ENDING': ignore_key('redundant'),
        'Sequence Number': ignore_key('redundant'),
        'VESSEL ENTRY NAME': ('vessel_name', lambda x: x if x != 'STEAMER' else None),
        'COMMODITY LABEL': ignore_key('redundant'),
        'PORT OF ORIGIN': ('port_name', may_strip),
        'CODE ORG': ignore_key('redundant'),
        'ORG CODE': ignore_key('redundant'),
        'UNCTAD CODE LOAD': ignore_key('redundant'),
        'DESCRIPTION LOAD': (ignore_key('redundant arrival zone')),
        'DESTINATION': ignore_key('redundant'),
        'CODE (DES)': ignore_key('redundant'),
        'DES CODE': ignore_key('redundant'),
        'UNCTAD CODE D': ignore_key('redundant'),
        'DESCRIPTION DEST': ignore_key('redundant departure zone'),
        'VESSEL NAME': ignore_key('redundant'),
        'VESSEL YR': ignore_key('redundant'),
        'IMO': ('vessel_imo', lambda x: try_apply(x, int, str)),
        'TONNAGE': ('cargo_volume', normalize_cargo_volume),
        'SIZE': ignore_key('not a good estimate for dwt'),
        'COMMODITY': ('cargo_product', lambda x: x if x not in IRRELEVANT_PRODUCTS else None),
        'MRI COMM CODE': ignore_key('redundant'),
        'COMM CODE': ignore_key('redundant'),
        'Dates': ignore_key('redundant'),
        'S DATE': ('eta', None),
        'CHARTER': ignore_key('redundant'),
        'TERMS': ignore_key('redundant'),
        'RATE': ignore_key('redundant'),
        'RATE A': ignore_key('redundant'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: dt.datetime.strptime(x, '%m%d%Y').isoformat()),
    }


def normalize_cargo_volume(raw_tonnage):
    """Normalize and cleanup raw cargo volume string.

    Args:
        raw_tonnage (str):

    Returns:
        str:

    Examples:
        >>> normalize_cargo_volume('274000')
        '274000'
        >>> normalize_cargo_volume('160000-10%')
        '160000'
        >>> normalize_cargo_volume('35958 TDW')
        '35958'
        >>> normalize_cargo_volume('foobar')
    """
    _match = re.match(r'^(\d+).*', raw_tonnage)
    if not _match:
        logger.warning(f'Unknown raw cargo volume: {raw_tonnage}')
        return None

    return _match.group(1)
