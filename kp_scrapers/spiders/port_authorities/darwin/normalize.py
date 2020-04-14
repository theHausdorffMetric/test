import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

VALID_PREVIOUS_LOCATIONS = ['Outside Port Limit', 'SEA']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard irrelevant vessel movement events
    event = item.pop('event')
    if event != 'ARR':
        logger.info(f"Vessel {item['vessel_name']} has irrelevant event {event}, discarding")
        return

    # discard irrelevant previous locations
    if item.pop('previous_location') not in VALID_PREVIOUS_LOCATIONS:
        return

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}

    # yield ordinary item
    yield item


def portcall_mapping():
    return {
        'AGENCY_ID': ignore_key('internal shipping agent ID'),
        'AGENT': ignore_key('shipping agent'),
        'DATE': ('eta', to_isoformat),
        'DOMAIN_ID': ignore_key('irrelevant'),
        'FROM_LOCATION': ('previous_location', None),
        'JOB_ID': ignore_key('internal portcall ID'),
        'JOB_TYPE': ('event', None),
        'LAST_PORT': ignore_key('previous port of call'),
        'NEXT_PORT': ignore_key('next port of call'),
        'PORT': ignore_key('duplicate of "port_name'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'SHIP': ('vessel_name', None),
        'TO_LOCATION': ('installation', None),
        'VESSEL_ID': ignore_key('internal vessel ID'),
        'vessel_imo': ('vessel_imo', None),
    }
