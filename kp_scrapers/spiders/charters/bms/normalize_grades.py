import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, split_by_delimiters, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

STRING_BLACKLIST = ['N/A', 'TBA']

MOVEMENT_MAPPING = {'DISC': 'discharge', 'DISCH': 'discharge', 'LOAD': 'load', 'STORAGE': 'load'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, grades_mapping())

    # discard items without valid dates
    if not (item.get('eta') or item.get('arrival') or item.get('berthed')):
        logger.warning(f'Item has no valid portcall dates, discarding:\n{item}')
        return

    # discard items without vessel names
    vessel_name, _ = item.pop('vessel_name_and_charter_status')
    if not vessel_name:
        logger.warning(f'Item has no vessel name, discarding:\n{item}')
        return

    # build Vessel sub-model
    item['vessel'] = {'name': vessel_name, 'imo': item.pop('vessel_imo', None)}

    # build Cargo sub-model
    products = item.pop('cargo_product', [])
    movement = item.pop('cargo_movement', None)
    # NOTE assume equal volume split by number of products
    volume = item.pop('cargo_volume', None)

    for product in products:
        item['cargo'] = {
            'product': product,
            'movement': movement,
            'volume': (
                str(try_apply(volume, int, float) / len(products))
                if try_apply(volume, int, float)
                else None
            ),
            'volume_unit': Unit.tons,
        }
        if movement and item.get('buyer_seller', None):
            player = 'seller' if movement == 'load' else 'buyer'
            item['cargo'].update({player: {'name': item.get('buyer_seller')}})
        item.pop('buyer_seller', None)
        yield item


def grades_mapping():
    return {
        'Arrived': ('arrival', normalize_pc_date),
        'AGENT': ignore_key('shipping agent is not required'),
        'Berthed': ('berthed', normalize_pc_date),
        'BL DD': ('berthed', normalize_pc_date),
        'CHARTERER': ignore_key('charterer is not required'),
        'COUNTRY OF DEST': ignore_key('not specific enough; we already have "NEXT PORT"'),
        'ETA': ('eta', normalize_pc_date),
        'ETB': ('berthed', normalize_pc_date),
        'ETS': ignore_key('not required'),
        'GRADE DETAIL': ('cargo_product', split_cargoes),
        'GRADE GROUP': ignore_key('we already have grade detail, so we can ignore this'),
        'IMO NR': ('vessel_imo', lambda x: try_apply(x, float, int, str) if x else None),
        'LOAD POSITION': ignore_key('irrelevant'),
        'LOAD/DISCH': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),
        'NEXT PORT': ignore_key('not required for PortCall for now'),
        'PORT': ('port_name', None),
        'PRE. PORT': ignore_key('not required'),
        'provider_name': ('provider_name', None),
        'QTT IN MT': ('cargo_volume', lambda x: try_apply(x, float, int) if x else None),
        'region_name': ignore_key('not required'),
        'reported_date': ('reported_date', normalize_reported_date),
        'Sailed': ignore_key('not required'),
        'SHIPPERS/RECEIVERS': ('buyer_seller', lambda x: x.split('/')[-1] if x else None),
        'STATUS': ignore_key('irrelevant'),
        'TERMINAL': ('installation', lambda x: x if x else None),
        'VESSEL': (
            'vessel_name_and_charter_status',
            # don't use the separator value
            lambda x: [may_strip(each) for idx, each in enumerate(x.partition('/')) if idx != 1],
        ),
    }


def split_cargoes(raw_cargo):
    """Split a raw cargo into multiple cargoes.

    Args:
        raw_cargo (str):

    Returns:
        List[str]:

    Examples:
        >>> split_cargoes('N/A')
        []
        >>> split_cargoes('MDO/ ULSD')
        ['MDO', 'ULSD']
        >>> split_cargoes('VGO')
        ['VGO']

    """
    if not raw_cargo or raw_cargo in STRING_BLACKLIST:
        return []

    return split_by_delimiters(raw_cargo, '/', '+', '\n')


def normalize_pc_date(date_str):
    """Cleanup ISO8601 portcall-related date.

    Args:
        raw_date (str):

    Returns:
        str | None: ISO8601-formatted date string

    Examples:
        >>> normalize_pc_date('2018-07-28T23:45:00')
        '2018-07-28T23:45:00'
        >>> normalize_pc_date('N/A')
        >>> normalize_pc_date(' ')

    """
    if not may_strip(date_str) or any(sub in date_str for sub in STRING_BLACKLIST):
        return None

    return date_str


def normalize_reported_date(raw_date):
    """Normalize raw reported date into an ISO8601-formatted string.

    This function WILL discard timezone data.
    Also, take note that we need to send reported date as a DD MMM YYYY string.

    Args:
        raw_date (str):

    Returns:
        str: ISO8601-formatted date string

    Examples:
        >>> normalize_reported_date('Fri, 5 Jul 2018 17:15:23 +0200')
        '2018-07-05T17:15:23'
        >>> normalize_reported_date('BMS Crude & Fuel')

    """
    date_match = re.match(r'.*:\d+(?!\+)', raw_date)
    return to_isoformat(date_match.group(0)) if date_match else None
