import logging
from typing import Any, Callable, Dict, Optional, Tuple

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, is_number, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


INWARD_MOVEMENT = ['inward movement', 'arrival']


# see https://bit.ly/2qxQgEJ
BERTH_TO_INSTALLATION_MAPPING = {
    'Dragon': 'Dragon',
    'Puma': 'Milford Haven II',
    'South Hook': 'South Hook LNG',
    'Valero': 'Valero Pembroke',
    'VPOT': 'Milford Haven I',
}


@validate_item(PortCall, normalize=True, strict=True)
def process_item(raw_item: Dict[str, str]) -> Dict[str, Any]:
    item = map_keys(raw_item, portcall_mapping())

    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'gross_tonnage': item.pop('vessel_gt', None),
        'dead_weight': item.pop('vessel_dwt', None),
    }

    # get relevant installation based on movement
    event = item.pop('event', None)
    if event not in INWARD_MOVEMENT:
        return

    # discard etas that are more than 2 months ahead
    if (parse_date(item['eta']) - parse_date(item['reported_date'])).days > 60:
        return

    return item


def portcall_mapping() -> Dict[str, Tuple[str, Optional[Callable]]]:
    return {
        'Date': ('eta', lambda x: to_isoformat(x, dayfirst=False, yearfirst=True)),
        'Ship': ('vessel_name', None),
        'GT': ('vessel_gt', lambda x: validate_weight(x)),
        'DWT': ('vessel_dwt', lambda x: validate_weight(x)),
        'Move Type': ('event', lambda x: may_strip(x.lower())),
        'Remarks': ignore_key('handwritten notes from port authority'),
        'From': ignore_key('irrelevant'),
        'To': ('installation', map_berth_to_installation),
        'ACTION_STATUS_ID': ignore_key('internal ID used by port'),
        'Tug': ignore_key('irrelevant'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def validate_weight(raw_weight: str) -> Optional[str]:
    """validate negative or 0 weights

    Args:
        raw_weight (str):

    Examples:
        >>> validate_weight('')
        >>> validate_weight('0')
        >>> validate_weight('10000')
        '10000'

    """
    return str(raw_weight) if is_number(raw_weight) and float(raw_weight) > 0 else None


def map_berth_to_installation(raw_berth: str) -> Optional[str]:
    """Clean, normalize, and map a raw berth name to a known installation.

    Examples:
        >>> map_berth_to_installation('Valero 8')
        'Valero Pembroke'
        >>> map_berth_to_installation('Dragon No1')
        'Dragon'
        >>> map_berth_to_installation('Milford Dock')

    """
    for known_berth in BERTH_TO_INSTALLATION_MAPPING:
        if known_berth in may_strip(raw_berth):
            return BERTH_TO_INSTALLATION_MAPPING[known_berth]

    logger.debug('Unknown berth: %s', raw_berth)
    return None
