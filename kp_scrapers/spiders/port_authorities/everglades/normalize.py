import datetime as dt
import logging
import re

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_CARGOES = [
    'AUTOMOBILES',
    'CONTAINER',
    'CRUISE - DAILY',
    'CRUISE - MULTI-DAY',
    'LAY-IN',
    'YACHTS FLOAT ON-OFF',
    'YACHTS LOAD ON-OFF',
]

VESSEL_MOVEMENT_EVENT_MAPPING = {
    # vessel entering
    ('ARR', 'Scheduled'): 'eta',
    ('ARR', 'Confirmed'): 'arrival',
    ('ARR', 'Active'): 'arrival',
    ('ARR', 'Completed'): 'berthed',
    # vessels exiting
    # ('DEP', 'Scheduled'): 'etd',  # TODO support `etd` field for PortCall
    ('DEP', 'Confirmed'): 'departure',
    ('DEP', 'Active'): 'departure',
    ('DEP', 'Completed'): 'departure',
}


@validate_item(PortCall, normalize=True, strict=True)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        ArrivedEvent | BerthedEvent | EtaEvent:

    """
    item = map_keys(raw_item, field_mapping())
    # discard vessel movements with irrelevant cargoes
    if not item.get('cargoes'):
        logger.info(f'Discarding {item["vessel_name"]} as it has cargo {raw_item["14"]}')
        return

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}

    _movement = item.pop('event')
    _status = item.pop('vessel_status')
    logger.debug(f'Vessel {item["vessel"]["name"]} is {_movement}, status is {_status}')

    # vessel currently either in anchorage or at berth, however source does not distinguish
    if (_movement, _status) in VESSEL_MOVEMENT_EVENT_MAPPING:
        item[VESSEL_MOVEMENT_EVENT_MAPPING[(_movement, _status)]] = item.pop('matching_date')
        return item


def field_mapping():
    return {
        '0': ('vessel_status', None),
        '1': ('matching_date', normalize_matching_date),
        '2': ('event', lambda x: x if x in ['DEP', 'ARR'] else None),
        '3': ignore_key('irrelevant'),
        '4': ignore_key('irrelevant'),
        '5': ('vessel_name', None),
        '6': ignore_key('irrelevant'),
        '7': ignore_key('irrelevant'),
        '8': ignore_key('irrelevant'),
        '9': ignore_key('shipping agent'),
        '10': ignore_key('previous port call of vessel'),
        '11': ignore_key('next port call of vessel'),
        '12': ignore_key('irrelevant'),
        '13': ignore_key('irrelevant'),
        '14': ('cargoes', lambda x: [{'product': x}] if x not in IRRELEVANT_CARGOES else None),
        '15': ignore_key('irrelevant'),
        '16': ignore_key('irrelevant'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'imo': ('vessel_imo', None),
    }


def normalize_matching_date(raw_date):
    """Transform raw date string into an ISO-8601 formatted string.

    Args:
        raw_date (str):

    Returns:
        str:

    Examples:
        >>> normalize_matching_date('/Date(1523397600000-0400)/')
        '2018-04-10T22:00:00'

    """
    return dt.datetime.utcfromtimestamp(
        int(re.match(r'.*\((\d{10})', raw_date).group(1))
    ).isoformat()
