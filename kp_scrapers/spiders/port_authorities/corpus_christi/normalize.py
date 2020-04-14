import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


EVENT_MAPPING = {'Docked': 'berthed', 'E.T.A.': 'eta', 'Inbound': 'arrival'}

INSTALLATION_MAPPING = {'CH1': 'Corpus Christi LNG Plant', 'TD1': None}


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
    event = item.pop('event', None)
    if event not in EVENT_MAPPING:
        logger.info(f"Vessel {item['vessel_name']} has irrelevant event {event}, discarding")
        return

    # build proper portcall date
    item[EVENT_MAPPING[event]] = item.pop('pc_date')

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}

    return item


def portcall_mapping():
    return {
        'ACTIVITY': ('event', None),
        'AGENCY': ('shipping_agent', None),
        'AGENCY_ID': ignore_key('internal shipping agent ID'),
        'ANCHORAGE_TIME': ignore_key('anchorage time'),
        'DESTINATION': ignore_key('next destination'),
        'DOCK': ('installation', lambda x: INSTALLATION_MAPPING.get(x)),
        'FLAG': ignore_key('vessel flag'),
        'HARBOR_TUG': ignore_key('irrelevant'),
        'LINES': ignore_key('irrelevant'),
        'OPERATION': ignore_key('current dock operation'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'REMARKS': ignore_key('handwritten remarks by source'),
        'reported_date': ('reported_date', None),
        'SECURED': ignore_key('completion timestamp of latest portcall activity'),
        'SHIP': ('vessel_name', None),
        'STAR': ignore_key('irrelevant'),
        'STATUS': ignore_key('status of current port operation'),
        'UNDERWAY': ('pc_date', lambda x: to_isoformat(x, dayfirst=False)),  # month/day/year source
        'VESSEL_ID': ignore_key('internal vessel ID'),
        'vessel_imo': ('vessel_imo', None),
    }
